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
    
    // Debug: Log the config we parsed
    let shardUrls: string[] = [];
    try {
        shardUrls = JSON.parse(this.env.SHARD_CONFIG);
    } catch (e) {
        return new Response(JSON.stringify({ error: "Config Parse Failed" }), { status: 500 });
    }
    
    // Fetch stats
    let stats: any = {};
    try {
        stats = await (await fetch(statsUrl)).json() as any;
    } catch (e) { /* ignore network error for now */ }

    if (!stats) stats = { total_users: 0 };
    const userId = (stats.total_users || 0) + 1;
    
    const shardIndex = userId % (shardUrls.length || 1);
    const uniquePart = crypto.randomUUID();
    const analyticsId = `${shardIndex}.${uniquePart}`;
    const safeId = uniquePart;

    const userRecord = {
      internal_id: userId,
      shard_index: shardIndex,
      created_at: Date.now()
    };
    
    // Write to Main Shard
    const mapResult = await fetch(`${this.env.MAIN_SHARD_URL}/identity_map/${safeId}.json`, {
      method: 'PUT',
      body: JSON.stringify(userRecord)
    });

    await fetch(statsUrl, {
      method: 'PATCH',
      body: JSON.stringify({ total_users: userId })
    });

    return new Response(JSON.stringify({
      analytics_id: analyticsId,
      debug_config_len: shardUrls.length,
      debug_main_shard_status: mapResult.status
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
      
      let flushDebug = null;
      // Force flush if we have events, for debugging purposes
      if (this.buffer.length >= 1) { 
        flushDebug = await this.flush();
      }
      
      return new Response(JSON.stringify({ 
          queued: events.length,
          debug_flush: flushDebug 
      }));
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
    
    // Debug info return
    if (!this.analyticsId) return { error: "No Analytics ID" };
    if (!targetUrlRoot) return { error: "No Shard URL Found", config_len: this.shardConfig.length, id: this.analyticsId };
    if (this.buffer.length === 0) return { status: "Buffer Empty" };

    const eventsPayload = [...this.buffer];
    this.buffer = []; // Clear buffer BEFORE fetch to prevent duplicates if logic fails later, but for debug maybe keep if fail? No, standard is clear.

    const timestamp = Date.now();
    const safeAnalyticsId = this.analyticsId.replace(/\./g, '_');
    const targetUrl = `${targetUrlRoot}/events/${safeAnalyticsId}/${timestamp}.json`;

    try {
      const response = await fetch(targetUrl, {
        method: 'PUT',
        body: JSON.stringify(eventsPayload),
        headers: { 'Content-Type': 'application/json' }
      });
      
      const responseText = await response.text();
      
      return {
          status: response.status,
          statusText: response.statusText,
          url: targetUrl,
          response_body: responseText
      };

    } catch (e: any) {
      return { error: "Fetch Exception", details: e.message, url: targetUrl };
    }
  }
}
