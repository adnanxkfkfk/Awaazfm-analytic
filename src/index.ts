import { AnalyticsSession, Registry } from './durable_object';

export interface Env {
  ANALYTICS_SESSION: DurableObjectNamespace;
  REGISTRY: DurableObjectNamespace;
  FIREBASE_RTDB_URL: string;
}

export { AnalyticsSession, Registry };

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // API 1: Global Identity Creation
    if (path === '/create' && request.method === 'POST') {
      const id = env.REGISTRY.idFromName('global_registry');
      const stub = env.REGISTRY.get(id);
      return stub.fetch(request);
    }

    // API 2: Connection & Identity Locking
    if (path === '/connect' && request.method === 'POST') {
      const body = await request.clone().json() as any;
      if (!body.user_id) return new Response('Missing user_id', { status: 400 });
      
      const id = env.ANALYTICS_SESSION.idFromName(body.user_id.toString());
      const stub = env.ANALYTICS_SESSION.get(id);
      return stub.fetch(request);
    }

    // API 3: Batched Event Tracking
    if (path === '/track' && request.method === 'POST') {
      const body = await request.clone().json() as any;
      if (!body.user_id) return new Response('Missing user_id', { status: 400 });

      const id = env.ANALYTICS_SESSION.idFromName(body.user_id.toString());
      const stub = env.ANALYTICS_SESSION.get(id);
      return stub.fetch(request);
    }

    return new Response('Endpoint Not Allowed', { status: 405 });
  }
};
