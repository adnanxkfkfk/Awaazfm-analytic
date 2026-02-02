import { AnalyticsSession, Registry } from './durable_object';

export interface Env {
  ANALYTICS_SESSION: DurableObjectNamespace;
  REGISTRY: DurableObjectNamespace;
  SHARD_CONFIG: string; 
  MAIN_SHARD_URL: string;
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

    // API 2 & 3: Connection & Tracking (using analytics_id only)
    if ((path === '/connect' || path === '/track') && request.method === 'POST') {
      const body = await request.clone().json() as any;
      if (!body.analytics_id) return new Response('Missing analytics_id', { status: 400 });
      
      // Use analytics_id as the unique key for the Durable Object
      const id = env.ANALYTICS_SESSION.idFromName(body.analytics_id);
      const stub = env.ANALYTICS_SESSION.get(id);
      return stub.fetch(request);
    }

    return new Response('Endpoint Not Allowed', { status: 405 });
  }
};
