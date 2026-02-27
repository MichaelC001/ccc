package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

// MessageRecord tracks the delivery state of a single message
type MessageRecord struct {
	ID          string `json:"id"`
	Session     string `json:"session"`
	Type        string `json:"type"`   // user_prompt / assistant_text / tool_call / notification
	Text        string `json:"text"`
	Origin      string `json:"origin"` // terminal / telegram / claude
	TgDelivered bool   `json:"tg_delivered"`
	TgMsgID     int64  `json:"tg_msg_id,omitempty"`
	RetryCount  int    `json:"retry_count"`
	Timestamp   int64  `json:"timestamp"`
}

var (
	dbOnce     sync.Once
	dbInstance *sql.DB
	dbPath     = func() string { return filepath.Join(cacheDir(), "ccc.db") }
)

// openDB opens (or creates) the SQLite database and ensures tables exist.
// Safe to call multiple times — uses sync.Once internally.
func openDB() *sql.DB {
	dbOnce.Do(func() {
		path := dbPath()
		os.MkdirAll(filepath.Dir(path), 0755)

		db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(wal)&_pragma=busy_timeout(5000)")
		if err != nil {
			hookLog("db: open failed: %v", err)
			return
		}

		// Create/migrate tables
		for _, stmt := range []string{
			// Events: append-only timeline for debugging
			`CREATE TABLE IF NOT EXISTS events (
				id         INTEGER PRIMARY KEY AUTOINCREMENT,
				session    TEXT NOT NULL,
				type       TEXT NOT NULL,
				source     TEXT NOT NULL,
				ref_id     TEXT,
				detail     TEXT,
				created_at INTEGER NOT NULL
			)`,
			`CREATE INDEX IF NOT EXISTS idx_events_session ON events(session)`,
			`CREATE INDEX IF NOT EXISTS idx_events_ref ON events(ref_id)`,

			// Messages: current delivery state
			`CREATE TABLE IF NOT EXISTS messages (
				id           TEXT PRIMARY KEY,
				session      TEXT NOT NULL,
				type         TEXT NOT NULL,
				text         TEXT,
				origin       TEXT,
				tg_delivered INTEGER DEFAULT 0,
				tg_msg_id    INTEGER DEFAULT 0,
				retry_count  INTEGER DEFAULT 0,
				created_at   INTEGER NOT NULL
			)`,
			`CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session)`,
			`CREATE INDEX IF NOT EXISTS idx_messages_pending ON messages(session, tg_delivered) WHERE tg_delivered = 0`,

			// Tool state: live tool call display
			`CREATE TABLE IF NOT EXISTS tool_state (
				session   TEXT PRIMARY KEY,
				tg_msg_id INTEGER DEFAULT 0,
				tools_json TEXT DEFAULT '[]'
			)`,

			// Migration: drop old columns if they exist (SQLite ignores unknown columns in SELECT)
			// We handle this by creating new table if old one has terminal_delivered
		} {
			if _, err := db.Exec(stmt); err != nil {
				hookLog("db: create table failed: %v", err)
			}
		}

		// Migrate old messages table if needed
		migrateMessages(db)

		// Add retry_count column if missing (from earlier schema)
		db.Exec(`ALTER TABLE messages ADD COLUMN retry_count INTEGER DEFAULT 0`)

		dbInstance = db
	})
	return dbInstance
}

// migrateMessages handles migration from old schema (with terminal_delivered) to new
func migrateMessages(db *sql.DB) {
	// Check if old schema exists
	var colCount int
	row := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('messages') WHERE name = 'terminal_delivered'`)
	if err := row.Scan(&colCount); err != nil || colCount == 0 {
		return // new schema or no table, nothing to migrate
	}

	hookLog("db: migrating messages table to new schema")
	for _, stmt := range []string{
		`ALTER TABLE messages RENAME TO messages_old`,
		`CREATE TABLE messages (
			id           TEXT PRIMARY KEY,
			session      TEXT NOT NULL,
			type         TEXT NOT NULL,
			text         TEXT,
			origin       TEXT,
			tg_delivered INTEGER DEFAULT 0,
			tg_msg_id    INTEGER DEFAULT 0,
			created_at   INTEGER NOT NULL
		)`,
		`INSERT INTO messages (id, session, type, text, origin, tg_delivered, tg_msg_id, created_at)
		 SELECT id, session, type, text, origin, telegram_delivered, telegram_msg_id, created_at FROM messages_old`,
		`DROP TABLE messages_old`,
		`CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_pending ON messages(session, tg_delivered) WHERE tg_delivered = 0`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			hookLog("db: migration step failed: %v — %s", err, stmt[:50])
		}
	}

	// Migrate tool_state column rename if needed
	var tsColCount int
	row = db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('tool_state') WHERE name = 'telegram_msg_id'`)
	if err := row.Scan(&tsColCount); err == nil && tsColCount > 0 {
		for _, stmt := range []string{
			`ALTER TABLE tool_state RENAME TO tool_state_old`,
			`CREATE TABLE tool_state (
				session   TEXT PRIMARY KEY,
				tg_msg_id INTEGER DEFAULT 0,
				tools_json TEXT DEFAULT '[]'
			)`,
			`INSERT INTO tool_state (session, tg_msg_id, tools_json)
			 SELECT session, telegram_msg_id, tools_json FROM tool_state_old`,
			`DROP TABLE tool_state_old`,
		} {
			if _, err := db.Exec(stmt); err != nil {
				hookLog("db: tool_state migration failed: %v", err)
			}
		}
	}
}

// closeDB closes the database connection
func closeDB() {
	if dbInstance != nil {
		dbInstance.Close()
	}
}

// --- Events (append-only timeline) ---

// logEvent records an event in the timeline. Safe to call from any process.
func logEvent(session, eventType, source, refID, detail string) {
	db := openDB()
	if db == nil {
		return
	}
	db.Exec(
		`INSERT INTO events (session, type, source, ref_id, detail, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		session, eventType, source, refID, detail, time.Now().UnixMilli(),
	)
}

// --- Messages (current state) ---

// appendMessage inserts a message record. If the ID already exists,
// tg_delivered is only upgraded (0→1), never downgraded (1→0).
func appendMessage(rec *MessageRecord) error {
	db := openDB()
	if db == nil {
		return fmt.Errorf("db not open")
	}
	if rec.Timestamp == 0 {
		rec.Timestamp = time.Now().UnixMilli()
	}
	_, err := db.Exec(
		`INSERT INTO messages (id, session, type, text, origin, tg_delivered, tg_msg_id, retry_count, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   tg_delivered = MAX(tg_delivered, excluded.tg_delivered),
		   tg_msg_id = CASE WHEN excluded.tg_msg_id > 0 THEN excluded.tg_msg_id ELSE tg_msg_id END`,
		rec.ID, rec.Session, rec.Type, rec.Text, rec.Origin,
		boolToInt(rec.TgDelivered), rec.TgMsgID, rec.Timestamp,
	)
	return err
}

// markDelivered marks a message as delivered to Telegram with the given msg ID
func markDelivered(msgID string, tgMsgID int64) error {
	db := openDB()
	if db == nil {
		return fmt.Errorf("db not open")
	}
	_, err := db.Exec(
		`UPDATE messages SET tg_delivered = 1, tg_msg_id = ? WHERE id = ?`,
		tgMsgID, msgID,
	)
	return err
}

// isDelivered checks if a message has been delivered to Telegram
func isDelivered(msgID string) bool {
	db := openDB()
	if db == nil {
		return false
	}
	var delivered int
	err := db.QueryRow(
		`SELECT tg_delivered FROM messages WHERE id = ?`, msgID,
	).Scan(&delivered)
	if err != nil {
		return false
	}
	return delivered != 0
}

const maxRetries = 5

// findPending returns messages not yet delivered to Telegram for a session, ordered by created_at.
// Messages that exceeded maxRetries are excluded.
func findPending(session string) []*MessageRecord {
	db := openDB()
	if db == nil {
		return nil
	}
	rows, err := db.Query(
		`SELECT id, session, type, text, origin, tg_delivered, tg_msg_id, retry_count, created_at
		 FROM messages WHERE session = ? AND tg_delivered = 0 AND retry_count < ?
		 ORDER BY created_at`,
		session, maxRetries,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []*MessageRecord
	for rows.Next() {
		var r MessageRecord
		var tgDel int
		if err := rows.Scan(&r.ID, &r.Session, &r.Type, &r.Text, &r.Origin,
			&tgDel, &r.TgMsgID, &r.RetryCount, &r.Timestamp); err != nil {
			continue
		}
		r.TgDelivered = tgDel != 0
		result = append(result, &r)
	}
	return result
}

// incRetry increments the retry count for a message
func incRetry(msgID string) {
	db := openDB()
	if db == nil {
		return
	}
	db.Exec(`UPDATE messages SET retry_count = retry_count + 1 WHERE id = ?`, msgID)
}

// isPermanentError checks if an error should not be retried
func isPermanentError(errMsg string) bool {
	permanent := []string{
		"chat not found",
		"bot was blocked",
		"bot was kicked",
		"chat_id is empty",
		"not enough rights",
		"PEER_ID_INVALID",
	}
	lower := strings.ToLower(errMsg)
	for _, p := range permanent {
		if strings.Contains(lower, strings.ToLower(p)) {
			return true
		}
	}
	return false
}

// isFromTelegram checks if a prompt with matching text exists as an undelivered Telegram message.
// Used by UserPromptSubmit hook to detect if this prompt originated from Telegram.
func isFromTelegram(session, promptText string) bool {
	db := openDB()
	if db == nil {
		return false
	}
	var count int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM messages
		 WHERE session = ? AND origin = 'telegram' AND type = 'user_prompt' AND text = ?`,
		session, promptText,
	).Scan(&count)
	if err != nil {
		return false
	}
	return count > 0
}

// allSessions returns all distinct session names that have pending messages
func allSessions() []string {
	db := openDB()
	if db == nil {
		return nil
	}
	rows, err := db.Query(
		`SELECT DISTINCT session FROM messages WHERE tg_delivered = 0`,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var sessions []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err == nil {
			sessions = append(sessions, s)
		}
	}
	return sessions
}

// --- Tool State ---

// ToolState tracks tool calls and the Telegram message ID for live updates
type ToolState struct {
	MsgID int64      `json:"msg_id"`
	Tools []ToolCall `json:"tools"`
}

// lockToolState acquires a file lock for tool_state operations.
// Returns unlock function. Caller must defer unlock().
func lockToolState(session string) func() {
	lockPath := filepath.Join(cacheDir(), "tool-state-"+session+".lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return func() {}
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}
}

type ToolCall struct {
	Name   string `json:"name"`
	Input  string `json:"input"`
	IsText bool   `json:"is_text,omitempty"`
	Time   int64  `json:"time,omitempty"`
}

func loadToolState(session string) *ToolState {
	db := openDB()
	if db == nil {
		return &ToolState{}
	}
	var msgID int64
	var toolsJSON string
	err := db.QueryRow(
		`SELECT tg_msg_id, tools_json FROM tool_state WHERE session = ?`, session,
	).Scan(&msgID, &toolsJSON)
	if err != nil {
		return &ToolState{}
	}
	var tools []ToolCall
	json.Unmarshal([]byte(toolsJSON), &tools)
	return &ToolState{MsgID: msgID, Tools: tools}
}

func saveToolState(session string, state *ToolState) {
	db := openDB()
	if db == nil {
		return
	}
	toolsJSON, _ := json.Marshal(state.Tools)
	db.Exec(
		`INSERT OR REPLACE INTO tool_state (session, tg_msg_id, tools_json) VALUES (?, ?, ?)`,
		session, state.MsgID, string(toolsJSON),
	)
}

func clearToolState(session string) {
	db := openDB()
	if db == nil {
		return
	}
	db.Exec(`DELETE FROM tool_state WHERE session = ?`, session)
}

// --- Helpers ---

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// contentHash returns a short hash of content for dedup IDs
func contentHash(s string) string {
	h := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", h[:4])
}
