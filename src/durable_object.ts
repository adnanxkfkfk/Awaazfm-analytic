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
    
    // Fire & Forget writes
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

// --- DATA PLANE: ANALYTICS SESSION DO ---
export class AnalyticsSession {
  state: DurableObjectState;
  env: Env;
  
  // Configuration & State
  analyticsId: string | null = null;
  shardConfig: string[] = [];
  buffer: any[] = [];
  sessions: WebSocket[] = [];
  
  // Session Lifecycle State
  sessionId: string | null = null;
  lastActive: number = 0;

  // Constants
  static SESSION_TIMEOUT_MS = 30 * 60 * 1000; // 30 Minutes

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    try {
      this.shardConfig = JSON.parse(env.SHARD_CONFIG);
    } catch (e) {}
    
    this.state.blockConcurrencyWhile(async () => {
        this.analyticsId = await this.state.storage.get<string>('analytics_id');
        this.sessionId = await this.state.storage.get<string>('session_id');
        this.lastActive = await this.state.storage.get<number>('last_active') || 0;
    });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // WebSocket Endpoint
    if (url.pathname === '/analytics') {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Expected websocket', { status: 426 });
      }

      const providedId = url.searchParams.get('analytics_id');
      if (providedId && !this.analyticsId) {
        this.analyticsId = providedId;
        await this.state.storage.put('analytics_id', providedId);
      }

      const { 0: client, 1: server } = new WebSocketPair();
      this.handleSession(server);

      return new Response(null, { status: 101, webSocket: client });
    }

    // HTTP Endpoint (Fallback/Legacy)
    if (url.pathname === '/track') {
      const body = await request.clone().json() as any;
      const { events, analytics_id } = body;

      if (analytics_id && !this.analyticsId) {
         this.analyticsId = analytics_id;
         await this.state.storage.put('analytics_id', analytics_id);
      }

      if (Array.isArray(events)) {
        await this.processEvents(events);
      }
      
      return new Response(JSON.stringify({ queued: events.length }));
    }

    return new Response('Not Found', { status: 404 });
  }

  // Handle WebSocket Connection
  handleSession(webSocket: WebSocket) {
    this.sessions.push(webSocket);
    webSocket.accept();

    webSocket.addEventListener('message', async (event) => {
      try {
        const raw = event.data;
        if (typeof raw !== 'string') return;
        const msg = JSON.parse(raw);

        // Expect { events: [...] }
        if (msg.events && Array.isArray(msg.events)) {
            await this.processEvents(msg.events);
        }
      } catch (err) {
        // Ignore parse errors
      }
    });

    webSocket.addEventListener('close', async () => {
      this.sessions = this.sessions.filter(s => s !== webSocket);
      // We do NOT end the session on socket close, because mobile connections flake.
      // We rely on the Alarm Timeout to end the session.
      await this.flush();
    });
  }

  // Core Logic: Session Management + Event Enrichment
  async processEvents(events: any[]) {
      const now = Date.now();

      // 1. Check if we need to START a new session
      if (!this.sessionId || (now - this.lastActive > AnalyticsSession.SESSION_TIMEOUT_MS)) {
          // If we had a stale session, flush it out first
          if (this.buffer.length > 0) await this.flush();

          this.sessionId = crypto.randomUUID();
          
          // Inject 'session_start' event
          this.buffer.push({
              type: 'session_start',
              ts: now,
              session_id: this.sessionId,
              system: true
          });
      }

      // 2. Update Activity Timers
      this.lastActive = now;
      await this.state.storage.put('last_active', this.lastActive);
      await this.state.storage.put('session_id', this.sessionId);
      
      // 3. Reset Timeout Alarm (Extend session window)
      await this.state.storage.setAlarm(now + AnalyticsSession.SESSION_TIMEOUT_MS);

      // 4. Enrich & Buffer Incoming Events
      for (const event of events) {
          event.session_id = this.sessionId; // Auto-attach Session ID
          if (!event.ts) event.ts = now;
          this.buffer.push(event);
      }

      // 5. Flush if Threshold Reached
      if (this.buffer.length >= 5) {
          await this.flush();
      }
  }

  // Cloudflare Alarm: Handles Auto-End on Timeout
  async alarm() {
      const now = Date.now();
      
      // If timed out
      if (this.sessionId && (now - this.lastActive >= AnalyticsSession.SESSION_TIMEOUT_MS)) {
          // Log 'session_end'
          this.buffer.push({
              type: 'session_end',
              ts: now,
              session_id: this.sessionId,
              system: true,
              reason: 'timeout'
          });

          // Reset Session State
          this.sessionId = null;
          await this.state.storage.delete('session_id');
          
          // Final Flush
          await this.flush();
      }
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
    } catch (e) {
      console.error("Flush Failed", e);
    }
  }
}