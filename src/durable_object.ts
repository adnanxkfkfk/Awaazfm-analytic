import { Env } from './index';

// --- REGISTRY DO (Monotonic ID Generation) ---
export class Registry {
  state: DurableObjectState;
  constructor(state: DurableObjectState) {
    this.state = state;
  }

  async fetch(request: Request): Promise<Response> {
    let lastId = (await this.state.storage.get<number>('last_id')) || 1000;
    lastId++;
    await this.state.storage.put('last_id', lastId);

    // Calculate logical shard (1 bucket = 1000 users)
    const logicalShardId = Math.ceil(lastId / 1000);
    
    return new Response(JSON.stringify({
      user_id: lastId,
      shard_id: logicalShardId,
      metadata: { region: 'global' }
    }), { headers: { 'Content-Type': 'application/json' } });
  }
}

// --- ANALYTICS SESSION DO (State Ownership & Sharded Flushing) ---
export class AnalyticsSession {
  state: DurableObjectState;
  env: Env;
  locked: boolean = false;
  user_guid: string | null = null;
  buffer: any[] = [];
  shardUrls: string[];

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    
    // Parse the Shard Config from Environment
    try {
        this.shardUrls = JSON.parse(env.SHARD_CONFIG);
    } catch (e) {
        this.shardUrls = [];
        console.error("Failed to parse SHARD_CONFIG");
    }

    this.state.blockConcurrencyWhile(async () => {
        this.locked = (await this.state.storage.get<boolean>('locked')) || false;
        this.user_guid = (await this.state.storage.get<string>('user_guid')) || null;
    });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/connect') {
      const { user_guid } = await request.json() as any;
      if (this.locked && this.user_guid !== user_guid) {
        return new Response('Identity Conflict', { status: 403 });
      }
      this.locked = true;
      this.user_guid = user_guid;
      await this.state.storage.put('locked', true);
      await this.state.storage.put('user_guid', user_guid);
      return new Response(JSON.stringify({ status: 'connected' }));
    }

    if (url.pathname === '/track') {
      if (!this.locked) return new Response('Not Connected', { status: 403 });
      
      const { events } = await request.json() as any;
      if (Array.isArray(events)) {
        this.buffer.push(...events);
      }

      // Flush if buffer gets large enough
      if (this.buffer.length >= 10) {
        await this.flushToFirebase();
      }

      return new Response(JSON.stringify({ queued: events.length }));
    }

    return new Response('Not Found', { status: 404 });
  }

  async flushToFirebase() {
    if (this.buffer.length === 0 || !this.user_guid) return;

    const data = [...this.buffer];
    this.buffer = [];

    // 1. Determine the Shard URL
    // We assume the DO name IS the user_id (set in index.ts)
    const userId = parseInt(this.state.id.toString()) || 0; // Note: In real production, we pass ID via constructor or storage if name isn't numeric
    // Better approach: We stored user_guid, let's assume user_guid IS the numeric ID for this math, 
    // or we act based on the stored user_guid if it is numeric.
    
    // Fallback: If we can't parse an ID, we hash the GUID to pick a shard.
    // Ideally, the system ensures user_guid is the monotonic ID from Registry.
    let shardIndex = 0;
    const parsedId = parseInt(this.user_guid);
    
    if (!isNaN(parsedId)) {
        // User 1-1000 -> Index 0
        // User 1001-2000 -> Index 1
        shardIndex = Math.ceil(parsedId / 1000) - 1;
    } else {
        // Fallback for non-numeric GUIDs: Hash modulo
        shardIndex = (this.user_guid.charCodeAt(0) % this.shardUrls.length);
    }

    // Safety: Wrap around if we run out of configured DBs
    const finalShardIndex = shardIndex % this.shardUrls.length;
    const dbUrl = this.shardUrls[finalShardIndex];

    const timestamp = Date.now();
    const targetUrl = `${dbUrl}/raw_events/${this.user_guid}/${timestamp}.json`;

    try {
      console.log(`Flushing to Shard ${finalShardIndex}: ${dbUrl}`);
      await fetch(targetUrl, {
        method: 'PUT',
        body: JSON.stringify(data),
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (e) {
      console.error('Firebase Flush Failed', e);
    }
  }
}