const express = require('express');
const { createKafka, TOPICS } = require('../../../shared/kafka');
const { query } = require('../../../shared/db');

const PORT = process.env.VERIFICATION_PORT || 3001;

async function doVerification(producer, msg) {
  const { workflowId, userId } = msg;

  // quick demo: bad-* users fail id check
  const passed = !String(userId).startsWith('bad-');

  await query(
    `INSERT INTO verification_records (workflow_id, user_id, status)
     VALUES ($1, $2, $3)
     ON CONFLICT (workflow_id)
     DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()`,
    [workflowId, userId, passed ? 'verified' : 'rejected']
  );

  console.log('verify', userId, passed ? 'ok' : 'fail');

  await producer.send({
    topic: TOPICS.RESULTS,
    messages: [
      {
        key: workflowId,
        value: JSON.stringify({
          workflowId,
          userId,
          step: 'verification',
          success: passed,
          reason: passed ? null : 'id_check_failed',
        }),
      },
    ],
  });
}

async function undoVerification(msg) {
  await query(
    `UPDATE verification_records
     SET status = 'compensated', updated_at = NOW()
     WHERE workflow_id = $1`,
    [msg.workflowId]
  );
  console.log('undo verification', msg.workflowId);
}

async function main() {
  const kafka = createKafka('verification');
  const producer = kafka.producer();
  const commands = kafka.consumer({ groupId: 'verification-commands' });
  const undos = kafka.consumer({ groupId: 'verification-undos' });

  await producer.connect();
  await commands.connect();
  await undos.connect();
  await commands.subscribe({ topic: TOPICS.COMMANDS });
  await undos.subscribe({ topic: TOPICS.COMPENSATE });

  await commands.run({
    eachMessage: async ({ message }) => {
      const msg = JSON.parse(message.value.toString());
      if (msg.command === 'start_verification') {
        await doVerification(producer, msg);
      }
    },
  });

  await undos.run({
    eachMessage: async ({ message }) => {
      const msg = JSON.parse(message.value.toString());
      if (msg.command === 'undo_verification') {
        await undoVerification(msg);
      }
    },
  });

  const app = express();
  app.get('/health', (req, res) => res.json({ ok: true, service: 'verification' }));
  app.get('/records/:workflowId', async (req, res) => {
    const { rows } = await query(
      'SELECT * FROM verification_records WHERE workflow_id = $1',
      [req.params.workflowId]
    );
    if (!rows[0]) return res.status(404).json({ error: 'not found' });
    res.json(rows[0]);
  });

  app.listen(PORT, () => console.log('verification on', PORT));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
