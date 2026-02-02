import { Env } from './index';

// --- CONTROL PLANE: REGISTRY DO ---
export class Registry {
  state: DurableObjectState;
  env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request: Request): Promise<Response> {
    const statsUrl = `${this.env.MAIN_SHARD_URL}/system_stats.json`;
    let stats: any = {};
    try { stats = await (await fetch(statsUrl)).json() as any; } catch (e) {}
    if (!stats) stats = { total_users: 0 };
    
    const userId = (stats.total_users || 0) + 1;
    let shardUrls: string[] = [];
    try { shardUrls = JSON.parse(this.env.SHARD_CONFIG); } catch (e) {}
    
    const shardIndex = userId % (shardUrls.length || 1);
    const uniquePart = crypto.randomUUID();
    const analyticsId = `${shardIndex}.${uniquePart}`;
    const safeId = uniquePart;

    const userRecord = {
      internal_id: userId,
      shard_index: shardIndex,
      created_at: Date.now()
    };
    
    fetch(`${this.env.MAIN_SHARD_URL}/identity_map/${safeId}.json`, {
      method: 'PUT',
      body: JSON.stringify(userRecord)
    });
    fetch(statsUrl, {
      method: 'PATCH',
      body: JSON.stringify({ total_users: userId })
    });

    return new Response(JSON.stringify({
      analytics_id: analyticsId,
      metadata: { region: 'global' }
    }), { headers: { 'Content-Type': 'application/json' } });
  }
}

// --- DATA PLANE: ANALYTICS SESSION DO (WebSocket Edition) ---
export class AnalyticsSession {
  state: DurableObjectState;
  env: Env;
  
  analyticsId: string | null = null;
  shardConfig: string[] = [];
  buffer: any[] = [];
  sessions: WebSocket[] = [];

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    try {
      this.shardConfig = JSON.parse(env.SHARD_CONFIG);
    } catch (e) {}
    
    this.state.blockConcurrencyWhile(async () => {
        this.analyticsId = await this.state.storage.get<string>('analytics_id');
    });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/analytics') {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Expected websocket', { status: 426 });
      }

      const providedId = url.searchParams.get('analytics_id');
      if (providedId && !this.analyticsId) {
        this.analyticsId = providedId;
        await this.state.storage.put('analytics_id', providedId);
      }

      // Create WebSocket Pair
      const { 0: client, 1: server } = new WebSocketPair();
      this.handleSession(server);

      return new Response(null, { status: 101, webSocket: client });
    }

    return new Response('Not Found', { status: 404 });
  }

  handleSession(webSocket: WebSocket) {
    this.sessions.push(webSocket);
    
    // Accept the connection
    webSocket.accept();

    webSocket.addEventListener('message', async (event) => {
      try {
        const raw = event.data;
        if (typeof raw !== 'string') return;
        const msg = JSON.parse(raw);

        // Expecting { events: [...] } or single event object
        // For simplicity: We expect { events: [...] }
        if (msg.events && Array.isArray(msg.events)) {
            this.buffer.push(...msg.events);
        } else {
            // Treat whole message as one event if needed, or ignore
            // ignoring malformed for now
        }

        // Flush Threshold
        if (this.buffer.length >= 5) {
            await this.flush();
        }

      } catch (err) {
        // webSocket.send(JSON.stringify({ error: 'Parse Error' }));
      }
    });

    webSocket.addEventListener('close', async () => {
      // Remove from session list
      this.sessions = this.sessions.filter(s => s !== webSocket);
      // Flush on disconnect
      await this.flush();
    });
  }

  getShardUrl(): string | null {
      if (!this.analyticsId) return null;
      const parts = this.analyticsId.split('.');
      if (parts.length < 2) return null;
      const index = parseInt(parts[0]);
      if (isNaN(index) || index >= this.shardConfig.length) return null;
      return this.shardConfig[index];
  }

  async flush() {
    const targetUrlRoot = this.getShardUrl();
    if (this.buffer.length === 0 || !targetUrlRoot || !this.analyticsId) return;

    const eventsPayload = [...this.buffer];
    this.buffer = [];

    const timestamp = Date.now();
    const safeAnalyticsId = this.analyticsId.replace(/\./g, '_');
    const targetUrl = `${targetUrlRoot}/events/${safeAnalyticsId}/${timestamp}.json`;

    try {
      await fetch(targetUrl, {
        method: 'PUT',
        body: JSON.stringify(eventsPayload),
        headers: { 'Content-Type': 'application/json' }
      });
      // Optional: Ack to client?
    } catch (e) {
      console.error("Flush Failed", e);
      // Put back in buffer? Or drop. Dropping to prevent memory leaks in DO.
    }
  }
}
