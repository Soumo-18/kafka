# Real-Time Location Tracking System

## Overview
A real-time location tracking application using WebSockets and Kafka for scalable event streaming. Users authenticate via OIDC, share their location through Socket.IO, which is processed through Kafka and broadcast to all connected clients.

## Tech Stack
- **Backend**: Node.js, Express
- **Real-time**: Socket.IO
- **Event Streaming**: Apache Kafka (KafkaJS)
- **Authentication**: OIDC (OpenID Connect)
- **Package Manager**: pnpm

## Setup Steps

1. **Clone and install dependencies**
   ```bash
   pnpm install
   ```

2. **Start Kafka**
   ```bash
   docker-compose up -d
   ```

3. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your credentials
   ```

4. **Run the application**
   ```bash
   node index.js
   ```

5. **Access the app**
   ```
   http://localhost:8000
   ```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `PORT` | Server port (default: 8000) |
| `CLIENT_ID` | OIDC client ID |
| `CLIENT_SECRET` | OIDC client secret |
| `REDIRECT_URI` | OAuth callback URL |
| `OIDC_ISSUER` | OIDC provider URL |
| `PUBLIC_KEY` | JWT verification key |
| `KAFKA_BROKER` | Kafka broker URL |

## OIDC Auth Setup

1. **Login Flow**: User visits `/login` → redirected to OIDC provider
2. **Callback**: Provider redirects to `/api/auth/callback` with auth code
3. **Token Exchange**: Server exchanges code for ID token
4. **Session**: Token stored in httpOnly cookie
5. **Protection**: All routes (except `/health`) require valid token
6. **Socket Auth**: WebSocket connections validate token from cookie header

## Socket Event Flow

```
Client → server
├─ client:location:update { lat, lng }

Server → client
├─ server:location-update { id, lat, lng }
└─ server:user-disconnected { userId }
```

**Flow**:
1. Client connects via Socket.IO (authenticated)
2. Client emits `client:location:update` with coordinates
3. Server publishes to Kafka topic `location-updates`
4. Server broadcasts `server:location-update` to all clients

## Kafka Event Flow

```
Producer (Socket Handler)
  ↓
Topic: location-updates
  ↓
Consumer (Socket Server)
  ↓
Broadcast to all Socket.IO clients
```

**Details**:
- **Topic**: `location-updates`
- **Producer**: Publishes location updates from socket events
- **Consumer**: Subscribes with group ID `socket-server-{PORT}`
- **Message Format**: `{ id: userId, lat: number, lng: number }`

## Demo Video Link
[Add your demo video link here]

## Assumptions and Limitations

**Assumptions**:
- Single Kafka broker (not production-ready cluster)
- OIDC provider is available and configured
- Clients have geolocation API access
- All users share the same map view

**Limitations**:
- No persistent storage (locations lost on restart)
- Single server instance (horizontal scaling requires Redis adapter)
- No rate limiting on location updates
- Basic error handling
- No user presence/status management
- Kafka runs in KRaft mode (no ZooKeeper)
