const Redis = require('ioredis');

let client;

function getRedis() {
  if (!client) {
    client = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
      maxRetriesPerRequest: 1,
      lazyConnect: true,
    });
    client.on('error', (err) => console.warn('redis err', err.message));
  }
  return client;
}

async function cacheGet(workflowId) {
  try {
    const r = getRedis();
    if (r.status !== 'ready') await r.connect();
    const raw = await r.get('workflow:' + workflowId);
    return raw ? JSON.parse(raw) : null;
  } catch (e) {
    return null;
  }
}

async function cacheSet(workflowId, data) {
  try {
    const r = getRedis();
    if (r.status !== 'ready') await r.connect();
    // 30 sec is enough for demos
    await r.set('workflow:' + workflowId, JSON.stringify(data), 'EX', 30);
  } catch (e) {
    // ignore cache failures
  }
}

async function cacheClear(workflowId) {
  try {
    const r = getRedis();
    if (r.status !== 'ready') await r.connect();
    await r.del('workflow:' + workflowId);
  } catch (e) {}
}

async function redisPing() {
  try {
    const r = getRedis();
    if (r.status !== 'ready') await r.connect();
    return (await r.ping()) === 'PONG';
  } catch (e) {
    return false;
  }
}

module.exports = { cacheGet, cacheSet, cacheClear, redisPing };
