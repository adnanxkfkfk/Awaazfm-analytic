# Real-Time Analytics Engine

A production-grade analytics backend built with Cloudflare Workers, Durable Objects, and Firebase.

## Core Features
- **Real-time Ingestion:** WebSocket-based event stream.
- **Strict Identity:** Durable Object-enforced UUID locking to prevent replay attacks and spoofing.
- **Scalable Storage:** Sharded Firebase writes to stay within free-tier limits.
- **Session Management:** Server-side session lifecycle and metric aggregation.

## Architecture
- **Runtime:** Cloudflare Workers
- **State:** Durable Objects (1 per Analytics UUID)
- **Database:** Firebase (Firestore/RTDB) via REST

## API

### HTTP
- `POST /create` - Provision a new Analytics UUID.
- `POST /connect` - Bind a UUID to a User/Device (One-time lock).

### WebSocket
- `GET /analytics` - Stream events.

## Event Envelope
```json
{
  "type": "screen_view",
  "timestamp": 1706859200,
  "session_id": "sess_123",
  "payload": { ... }
}
```