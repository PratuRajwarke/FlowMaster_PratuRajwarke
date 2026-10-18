const express = require('express');
const { createKafka, TOPICS } = require('../../../shared/kafka');
const { query } = require('../../../shared/db');

const PORT = process.env.CREDIT_PORT || 3002;

// fail-* users always get rejected so we can demo compensation
function checkScore(userId) {
  if (String(userId).startsWith('fail-')) {
    return { score: 420, approved: false, reason: 'score_below_threshold' };
  }
  return { score: 720, approved: true, reason: null };
}

async function main() {
  const kafka = createKafka('credit');
  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: 'credit-commands' });

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.COMMANDS });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const msg = JSON.parse(message.value.toString());
      if (msg.command !== 'start_credit_check') return;

      const { workflowId, userId } = msg;
      const result = checkScore(userId);

      await query(
        `INSERT INTO credit_checks (workflow_id, user_id, score, status)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (workflow_id)
         DO UPDATE SET score = EXCLUDED.score, status = EXCLUDED.status`,
        [
          workflowId,
          userId,
          result.score,
          result.approved ? 'approved' : 'rejected',
        ]
      );

      console.log('credit', userId, result.score, result.approved);

      await producer.send({
        topic: TOPICS.RESULTS,
        messages: [
          {
            key: workflowId,
            value: JSON.stringify({
              workflowId,
              userId,
              step: 'credit_check',
              success: result.approved,
              score: result.score,
              reason: result.reason,
            }),
          },
        ],
      });
    },
  });

  const app = express();
  app.get('/health', (req, res) => res.json({ ok: true, service: 'credit' }));

  // mostly for debugging
  app.get('/checks/:id', async (req, res) => {
    const { rows } = await query(
      'SELECT * FROM credit_checks WHERE workflow_id = $1',
      [req.params.id]
    );
    if (!rows[0]) return res.status(404).json({ error: 'not found' });
    res.json(rows[0]);
  });

  app.listen(PORT, () => console.log('credit on', PORT));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
