#!/usr/bin/env node

const BASE = 'http://localhost:3000';

async function start(userId) {
  const res = await fetch(BASE + '/workflows', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ userId }),
  });
  return res.json();
}

async function get(id) {
  return fetch(BASE + '/workflows/' + id).then((r) => r.json());
}

async function waitDone(id) {
  for (let i = 0; i < 40; i++) {
    const wf = await get(id);
    if (['completed', 'failed', 'compensated'].includes(wf.status)) return wf;
    await new Promise((r) => setTimeout(r, 300));
  }
  return get(id);
}

async function happy() {
  const created = await start('amit-' + Date.now());
  console.log('id', created.id);
  const wf = await waitDone(created.id);
  console.log('status', wf.status);
  (wf.timeline || []).forEach((e) => console.log('-', e.message));
}

async function fail() {
  const created = await start('fail-' + Date.now());
  console.log('id', created.id);
  const wf = await waitDone(created.id);
  console.log('status', wf.status);
  (wf.timeline || []).forEach((e) => console.log('-', e.message));

  const v = await fetch('http://localhost:3001/records/' + created.id).then((r) =>
    r.json()
  );
  console.log('verification after undo:', v.status);
}

const cmd = process.argv[2] || 'happy';
if (cmd === 'happy') happy().catch(console.error);
else if (cmd === 'fail') fail().catch(console.error);
else console.log('node scripts/demo.js happy|fail');
