const WebSocket = require('ws');
const https = require('https');

// Helper for HTTP GET/POST
function request(url, method = 'GET', body = null) {
    return new Promise((resolve, reject) => {
        const req = https.request(url, { method, headers: { 'Content-Type': 'application/json' } }, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => resolve(data));
        });
        req.on('error', reject);
        if (body) req.write(body);
        req.end();
    });
}

async function run() {
    console.log("1. Creating User...");
    const createRes = await request('https://awaazfm-analytic-api.api74.workers.dev/create', 'POST');
    const userData = JSON.parse(createRes);
    const id = userData.analytics_id;
    console.log("   User ID:", id);

    console.log("2. Connecting WebSocket...");
    const wsUrl = `wss://awaazfm-analytic-api.api74.workers.dev/analytics?analytics_id=${id}`;
    const ws = new WebSocket(wsUrl);

    ws.on('open', () => {
        console.log("   WebSocket Open!");
        
        console.log("3. Sending 5 Events...");
        const payload = {
            events: [
                { type: 'ws_test_1', ts: Date.now() },
                { type: 'ws_test_2', ts: Date.now() },
                { type: 'ws_test_3', ts: Date.now() },
                { type: 'ws_test_4', ts: Date.now() },
                { type: 'ws_test_5', ts: Date.now() }
            ]
        };
        ws.send(JSON.stringify(payload));
        console.log("   Sent.");
        
        // Wait 2 seconds then close to force flush
        setTimeout(() => {
            console.log("4. Closing WebSocket...");
            ws.close();
        }, 2000);
    });

    ws.on('close', async () => {
        console.log("   WebSocket Closed.");
        
        console.log("5. Verifying Firebase...");
        // Delay to allow async flush to finish
        await new Promise(r => setTimeout(r, 2000));
        
        const safeId = id.replace(/\./g, '_');
        const shardIndex = id.split('.')[0];
        const dbUrl = shardIndex === '0' 
            ? 'https://comstorytv-default-rtdb.firebaseio.com' 
            : 'https://awaaz-fm-default-rtdb.firebaseio.com';
            
        const verifyUrl = `${dbUrl}/events/${safeId}.json`;
        console.log("   Checking:", verifyUrl);
        
        const fbRes = await request(verifyUrl);
        console.log("   Result:", fbRes);
    });
    
    ws.on('error', (e) => console.error("WS Error:", e));
}

run();
