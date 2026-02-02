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
    
    // 1. Get Global User Count (for stats only)
    let stats = await (await fetch(statsUrl)).json() as any;
    if (!stats) stats = { total_users: 0 };
    const userId = (stats.total_users || 0) + 1;

    // 2. Load Balancing (Round Robin)
    let shardUrls: string[] = [];
    try {
        shardUrls = JSON.parse(this.env.SHARD_CONFIG);
    } catch (e) {
        shardUrls = []; // Fail safe
    }
    
    // Default to Shard 0 if config fails
    const shardIndex = userId % (shardUrls.length || 1);
    
    // 3. GENERATE SELF-DESCRIBING ID
    // Format: "SHARD_INDEX.RANDOM_UUID"
    // Example: "2.550e8400-e29b-41d4-a716-446655440000"
    const uniquePart = crypto.randomUUID();
    const analyticsId = `${shardIndex}.${uniquePart}`;

    // 4. Update Main Shard (Async - Fire & Forget preferred in real prod)
    // We still store the user in Main Shard for "Directory" purposes, but routing won't need it.
    const userRecord = {
      internal_id: userId,
      shard_index: shardIndex,
      created_at: Date.now()
    };
    
    await fetch(`${this.env.MAIN_SHARD_URL}/identity_map/${uniquePart}.json`, {
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
  assignedShardUrl: string | null = null;
  shardConfig: string[] = [];
  buffer: any[] = [];

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    
    // Load config once
    try {
      this.shardConfig = JSON.parse(env.SHARD_CONFIG);
    } catch (e) {
      console.error("Config Error");
    }

    this.state.blockConcurrencyWhile(async () => {
        this.analyticsId = await this.state.storage.get<string>('analytics_id');
        // We don't need to store assignedShardUrl anymore, we can derive it!
        // But caching the full URL is still a nice micro-optimization.
    });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const body = await request.clone().json() as any;
    const providedId = body.analytics_id;

    // 1. Resolve Routing INSTANTLY from ID
    if (providedId && !this.analyticsId) {
       this.analyticsId = providedId;
       await this.state.storage.put('analytics_id', providedId);
    }

    if (url.pathname === '/track') {
      const { events } = body;
      if (Array.isArray(events)) {
        this.buffer.push(...events);
      }
      
      // Flush logic
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

      // PARSE THE ID: "2.550e..."
      const parts = this.analyticsId.split('.');
      if (parts.length < 2) return null; // Invalid format

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
    // Path: SHARD_URL / events / ANALYTICS_ID / TIMESTAMP.json
    const targetUrl = `${targetUrlRoot}/events/${this.analyticsId}/${timestamp}.json`;

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
