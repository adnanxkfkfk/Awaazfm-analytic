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
    let stats = await (await fetch(statsUrl)).json() as any;
    if (!stats) stats = { total_users: 0 };
    const userId = (stats.total_users || 0) + 1;

    let shardUrls: string[] = [];
    try {
        shardUrls = JSON.parse(this.env.SHARD_CONFIG);
    } catch (e) {
        shardUrls = [];
    }
    
    const shardIndex = userId % (shardUrls.length || 1);
    
    // ID Format: "INDEX.UUID"
    const uniquePart = crypto.randomUUID();
    const analyticsId = `${shardIndex}.${uniquePart}`;

    // Update Main Shard
    const userRecord = {
      internal_id: userId,
      shard_index: shardIndex,
      created_at: Date.now()
    };
    
    // Sanitize ID for Firebase Key (Replace . with _)
    const safeId = uniquePart; // We index by the UUID part in the Main Shard map
    
    await fetch(`${this.env.MAIN_SHARD_URL}/identity_map/${safeId}.json`, {
      method: 'PUT',
      body: JSON.stringify(userRecord)
    });

    await fetch(statsUrl, {
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
  
  analyticsId: string | null = null;
  shardConfig: string[] = [];
  buffer: any[] = [];

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
    const body = await request.clone().json() as any;
    const providedId = body.analytics_id;

    if (providedId && !this.analyticsId) {
       this.analyticsId = providedId;
       await this.state.storage.put('analytics_id', providedId);
    }

    if (url.pathname === '/track') {
      const { events } = body;
      if (Array.isArray(events)) {
        this.buffer.push(...events);
      }
      
      if (this.buffer.length >= 5) {
        await this.flush();
      }
      return new Response(JSON.stringify({ queued: events.length }));
    }

    if (url.pathname === '/connect') {
      return new Response(JSON.stringify({ status: 'connected' }));
    }

    return new Response('Not Found', { status: 404 });
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

    // SANITIZATION FIX:
    // Firebase Keys cannot have dots. We replace '.' with '_' for storage path.
    const safeAnalyticsId = this.analyticsId.replace(/\./g, '_');

    // Path: SHARD_URL / events / SAFE_ANALYTICS_ID / TIMESTAMP.json
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