# Analytics API Connection Guide

This document outlines how to integrate with the AwaazFM Real-Time Analytics Engine.

## Base URL
**Production:** `https://awaazfm-analytic-api.api74.workers.dev`

---

## 1. Identity Creation (HTTP)
**Endpoint:** `POST /create`

Call this **once** when a user first installs or opens the app. Cache the `analytics_id` permanently on the device.

**Request:**
- Method: `POST`
- Headers: `None`
- Body: `None`

**Response (200 OK):**
```json
{
  "analytics_id": "0.a1b2c3d4-...",
  "metadata": { "region": "global" }
}
```

---

## 2. Analytics Ingestion (WebSocket)
**Endpoint:** `wss://awaazfm-analytic-api.api74.workers.dev/analytics`

Use this for all event tracking. The connection is stateful and manages user sessions automatically.

**Connection URL:**
`wss://awaazfm-analytic-api.api74.workers.dev/analytics?analytics_id=YOUR_ANALYTICS_ID`

**Protocol:**
1.  **Connect:** Open the WebSocket with the `analytics_id` query parameter.
2.  **Session:** The server automatically starts a session on the first event.
3.  **Timeout:** If no events are sent for 30 minutes, the session automatically ends (server-side).
4.  **Send Events:** Send JSON payloads containing an array of events.

**Payload Format:**
```json
{
  "events": [
    {
      "type": "screen_view",
      "ts": 1706859000000,
      "payload": { "screen_name": "Home" }
    },
    {
      "type": "button_click",
      "ts": 1706859005000,
      "payload": { "btn_id": "play_music" }
    }
  ]
}
```

**Automatic Fields:**
The server **automatically adds** these fields to every event you send:
- `session_id`: Unique UUID for the current session.
- `ts`: Server timestamp (if you omit it).

**System Events:**
The server automatically generates these events (you do not need to send them):
- `session_start`: Generated when a new session begins.
- `session_end`: Generated when a session times out (30 mins inactivity).

---

## 3. Implementation Best Practices

1.  **Buffer Client-Side:** If the network is offline, buffer events on the device and send them when the WebSocket reconnects.
2.  **One Connection:** Maintain a single WebSocket connection while the app is in the foreground. Close it when the app goes to the background to save battery/data.
3.  **Handle Reconnects:** If the socket closes unexpectedly, simply reconnect using the same `analytics_id`. The server will resume the existing session if it hasn't timed out.
4.  **No "End Session" Call:** You do not need to tell the server when a session ends. The server handles this via the 30-minute timeout rule.
