# FuseCapy

A personal AI assistant that connects your LLM to chat platforms, manages conversations, and runs on your own hardware.

## What it does

- **Multi-channel messaging** — Chat with your AI through Telegram, iOS app, or web interface
- **Multiple AI backends** — Claude, Gemini, and more, switchable per conversation
- **Voice input** — On-device speech recognition with WhisperKit
- **Notes & Todos** — On-device intent classification to auto-create notes and reminders
- **Email feed** — Monitor and summarize your inbox
- **Self-hosted** — Single Go binary, runs on macOS, all data stays local

## Architecture

- **Server**: Go — handles messaging, bot orchestration, SSE push, and file management
- **iOS client**: Native Swift/UIKit — real-time chat, voice, notes, todos
- **Web dashboard**: Built-in HTML UI for history browsing, search, and settings

## Status

Under active development. Not yet ready for public use.

## License

Proprietary. All rights reserved.
