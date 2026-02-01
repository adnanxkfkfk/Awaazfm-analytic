import { AnalyticsSession } from './durable_object';

export interface Env {
  ANALYTICS_SESSION: DurableObjectNamespace;
  FIREBASE_PROJECT_ID: string;
  FIREBASE_API_KEY: string;
  TOTAL_SHARDS: string;
}

export { AnalyticsSession };

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === 'POST' && path === '/create') {
      return handleCreate(env);
    }

    if (request.method === 'POST' && path === '/connect') {
      return handleConnect(request, env);
    }

    if (path === '/analytics') {
      return handleWebsocket(request, env);
    }

    return new Response('Not Found', { status: 404 });
  }
};

async function handleCreate(env: Env): Promise<Response> {
  const uuid = crypto.randomUUID();
  // We don't instantiate the DO yet, just reserve the ID conceptually.
  // Real instantiation happens on first connect/analytics push to save cost 
  // until actual usage, or we can "touch" it here if strict pre-validation is needed.
  return new Response(JSON.stringify({ analytics_uuid: uuid }), {
    headers: { 'Content-Type': 'application/json' }
  });
}

async function handleConnect(request: Request, env: Env): Promise<Response> {
  try {
    const body = await request.json() as any;
    const { analytics_uuid, user_guid, device_metadata } = body;

    if (!analytics_uuid || !user_guid) {
      return new Response('Missing analytics_uuid or user_guid', { status: 400 });
    }

    const id = env.ANALYTICS_SESSION.idFromName(analytics_uuid);
    const stub = env.ANALYTICS_SESSION.get(id);

    // Forward to DO to lock the identity
    const response = await stub.fetch(new Request('https://do/connect', {
      method: 'POST',
      body: JSON.stringify({ user_guid, device_metadata })
    }));

    return response;
  } catch (e) {
    return new Response('Invalid JSON', { status: 400 });
  }
}

async function handleWebsocket(request: Request, env: Env): Promise<Response> {
  const upgradeHeader = request.headers.get('Upgrade');
  if (!upgradeHeader || upgradeHeader !== 'websocket') {
    return new Response('Expected Upgrade: websocket', { status: 426 });
  }

  const url = new URL(request.url);
  const analytics_uuid = url.searchParams.get('id');
  const token = url.searchParams.get('token'); // For auth

  if (!analytics_uuid) {
    return new Response('Missing analytics_uuid (id param)', { status: 400 });
  }

  // TODO: Validate token here before contacting DO to save DO invocations
  // For now, we pass through to DO for full validation logic

  const id = env.ANALYTICS_SESSION.idFromName(analytics_uuid);
  const stub = env.ANALYTICS_SESSION.get(id);

  return stub.fetch(request);
}
