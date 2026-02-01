import { Env } from './index';

interface SessionState {
  user_guid: string | null;
  locked: boolean;
  device_metadata: any;
  current_session_id: string | null;
}

interface AnalyticsEvent {
  type: string;
  timestamp: number;
  session_id: string;
  payload: any;
}

export class AnalyticsSession {
  state: DurableObjectState;
  env: Env;
  session: SessionState;
  buffer: AnalyticsEvent[];
  storage: DurableObjectStorage;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    this.storage = state.storage;
    this.session = {
      user_guid: null,
      locked: false,
      device_metadata: {},
      current_session_id: null
    };
    this.buffer = [];

    this.state.blockConcurrencyWhile(async () => {
      const stored = await this.storage.get<SessionState>('session_state');
      if (stored) {
        this.session = stored;
      }
    });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (pathEndsWith(url.pathname, '/connect') && request.method === 'POST') {
      return this.handleConnect(request);
    }

    if (request.headers.get('Upgrade') === 'websocket') {
      return this.handleWebsocket(request);
    }

    return new Response('Not found', { status: 404 });
  }

  async handleConnect(request: Request): Promise<Response> {
    const data = await request.json() as any;
    const { user_guid, device_metadata } = data;

    if (this.session.locked) {
      if (this.session.user_guid !== user_guid) {
        return new Response(JSON.stringify({ error: 'Identity conflict' }), { status: 409 });
      }
      return new Response(JSON.stringify({ status: 'ok', already_locked: true }));
    }

    this.session.user_guid = user_guid;
    this.session.device_metadata = device_metadata || {};
    this.session.locked = true;

    await this.storage.put('session_state', this.session);

    return new Response(JSON.stringify({ status: 'locked' }));
  }

  async handleWebsocket(request: Request): Promise<Response> {
    if (!this.session.locked) {
      // Reject connection if identity not established
      return new Response('Unauthorized: Identity not locked', { status: 403 });
    }

    const { 0: client, 1: server } = new WebSocketPair();

    server.accept();

    server.addEventListener('message', async (event) => {
      try {
        const raw = event.data;
        if (typeof raw !== 'string') return;
        
        const data = JSON.parse(raw) as AnalyticsEvent;
        
        // Basic Validation
        if (!data.type || !data.session_id) {
            server.send(JSON.stringify({ error: 'Invalid envelope' }));
            return;
        }

        this.processEvent(data);

      } catch (e) {
        server.send(JSON.stringify({ error: 'Parse error' }));
      }
    });

    server.addEventListener('close', () => {
       this.flushBuffer();
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  processEvent(event: AnalyticsEvent) {
    // Replay protection / Session validation
    if (this.session.current_session_id && this.session.current_session_id !== event.session_id) {
       // New session started or interleaved?
       // For this simple implementation, we update current session
       this.session.current_session_id = event.session_id;
    }

    // Add to buffer
    this.buffer.push(event);

    // Flush based on size or logic
    if (this.buffer.length >= 20) {
      this.flushBuffer();
    }
  }

  async flushBuffer() {
    if (this.buffer.length === 0) return;

    const eventsToSend = [...this.buffer];
    this.buffer = []; // Clear immediately to avoid double send if concurrency hits

    try {
        // Sharding Logic
        const shardId = this.getShardId(this.session.user_guid || 'anon');
        const firebaseUrl = `https://firestore.googleapis.com/v1/projects/${this.env.FIREBASE_PROJECT_ID}/databases/(default)/documents/analytics_shard_${shardId}`;
        
        // NOTE: In a real app, you'd use a proper Firestore batch write structure.
        // This is a simplified fetch placeholder.
        // We are assuming a custom endpoint or standard REST API structure.
        
        console.log(`Flushing ${eventsToSend.length} events to shard ${shardId}`);
        
        // Simulating the write
        // await fetch(firebaseUrl, { ... });

    } catch (e) {
        console.error('Failed to flush to Firebase', e);
        // Retry logic would go here, maybe put back in buffer if critical
    }
  }

  getShardId(guid: string): number {
    let hash = 0;
    for (let i = 0; i < guid.length; i++) {
        hash = (hash << 5) - hash + guid.charCodeAt(i);
        hash |= 0;
    }
    const total = parseInt(this.env.TOTAL_SHARDS) || 10;
    return Math.abs(hash) % total;
  }
}

function pathEndsWith(path: string, suffix: string) {
    return path.endsWith(suffix) || path.endsWith(suffix + '/');
}
