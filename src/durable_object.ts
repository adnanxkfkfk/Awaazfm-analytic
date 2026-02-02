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

    const shardId = Math.ceil(lastId / 1000);
    return new Response(JSON.stringify({
      user_id: lastId,
      shard_id: shardId,
      metadata: { region: 'us-central1' }
    }), { headers: { 'Content-Type': 'application/json' } });
  }
}

// --- ANALYTICS SESSION DO (State Ownership) ---
export class AnalyticsSession {
  state: DurableObjectState;
  env: Env;
  locked: boolean = false;
  user_guid: string | null = null;
  buffer: any[] = [];

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
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

      // Principal Architect Logic: Batch flush to RTDB
      if (this.buffer.length >= 10) {
        await this.flushToFirebase();
      }

      return new Response(JSON.stringify({ queued: events.length }));
    }

    return new Response('Not Found', { status: 404 });
  }

  async flushToFirebase() {
    if (this.buffer.length === 0) return;

    const data = [...this.buffer];
    this.buffer = [];

    const timestamp = Date.now();
    // Using the exact RTDB URL provided with .json suffix for REST API
    const targetUrl = `${this.env.FIREBASE_RTDB_URL}/raw_events/${this.user_guid}/${timestamp}.json`;

    try {
      await fetch(targetUrl, {
        method: 'PUT', // PUT replaces data at this specific path
        body: JSON.stringify(data),
        headers: { 'Content-Type': 'application/json' }
      });
    } catch (e) {
      console.error('Firebase Flush Failed', e);
      // In production, we would retry or move to a dead-letter queue
    }
  }
}
