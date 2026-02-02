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

    // API 1: Global Identity Creation (HTTP)
    if (path === '/create' && request.method === 'POST') {
      const id = env.REGISTRY.idFromName('global_registry');
      const stub = env.REGISTRY.get(id);
      return stub.fetch(request);
    }

    // API 2: WebSocket Analytics Ingestion
    // URL: wss://<worker>/analytics?analytics_id=...
    if (path === '/analytics') {
      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader !== 'websocket') {
        return new Response('Expected Upgrade: websocket', { status: 426 });
      }

      const analyticsId = url.searchParams.get('analytics_id');
      if (!analyticsId) {
        return new Response('Missing analytics_id', { status: 400 });
      }

      const id = env.ANALYTICS_SESSION.idFromName(analyticsId);
      const stub = env.ANALYTICS_SESSION.get(id);
      return stub.fetch(request);
    }

    // API 3: Manual Connect (Optional/Legacy)
    if (path === '/connect' && request.method === 'POST') {
       // ... existing logic if needed, or remove. 
       // Keeping simple for now as we focus on WS.
       return new Response('Use WebSocket at /analytics', { status: 426 });
    }

    return new Response('Not Found', { status: 404 });
  }
};