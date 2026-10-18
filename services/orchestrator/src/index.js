const fs = require('fs');
const path = require('path');
const express = require('express');
const yaml = require('js-yaml');
const { v4: uuidv4 } = require('uuid');

const { createKafka, TOPICS } = require('../../../shared/kafka');
const { query } = require('../../../shared/db');
const { cacheGet, cacheSet, cacheClear, redisPing } = require('../../../shared/redis');

const PORT = process.env.ORCHESTRATOR_PORT || 3000;

function loadWorkflowYaml() {
  const file = path.join(__dirname, '../../../workflows/kyc-loan.yaml');
  return yaml.load(fs.readFileSync(file, 'utf8'));
}

async function saveEvent(workflowId, message) {
  await query(
    'INSERT INTO workflow_events (workflow_id, message) VALUES ($1, $2)',
    [workflowId, message]
  );
  console.log('[event]', workflowId.slice(0, 8), message);
}

async function setStatus(workflowId, status, currentStep) {
  await query(
    `UPDATE workflows
     SET status = $2, current_step = $3, updated_at = NOW()
     WHERE id = $1`,
    [workflowId, status, currentStep]
  );
  await cacheClear(workflowId);
}

async function sendCommand(producer, topic, body) {
  await producer.send({
    topic,
    messages: [{ key: body.workflowId, value: JSON.stringify(body) }],
  });
}

async function startVerification(producer, workflowId, userId) {
  await setStatus(workflowId, 'running', 'verification');
  await saveEvent(workflowId, 'sent start_verification');

  await sendCommand(producer, TOPICS.COMMANDS, {
    workflowId,
    userId,
    command: 'start_verification',
  });
}

async function startCreditCheck(producer, workflowId, userId) {
  await setStatus(workflowId, 'running', 'credit_check');
  await saveEvent(workflowId, 'sent start_credit_check');

  await sendCommand(producer, TOPICS.COMMANDS, {
    workflowId,
    userId,
    command: 'start_credit_check',
  });
}

// credit failed after verify passed -> tell verification to undo
async function undoVerification(producer, workflowId, userId, reason) {
  await setStatus(workflowId, 'compensating', 'verification');
  await saveEvent(workflowId, 'credit failed, sending undo_verification: ' + reason);

  await sendCommand(producer, TOPICS.COMPENSATE, {
    workflowId,
    userId,
    command: 'undo_verification',
    reason,
  });

  await setStatus(workflowId, 'compensated', 'done');
  await saveEvent(workflowId, 'workflow compensated');
}

async function onWorkerResult(producer, result) {
  const { workflowId, step, success, userId, reason, score } = result;

  const wf = await query('SELECT * FROM workflows WHERE id = $1', [workflowId]);
  if (!wf.rows[0]) return;

  if (['completed', 'failed', 'compensated'].includes(wf.rows[0].status)) {
    return;
  }

  if (step === 'verification') {
    if (success) {
      await saveEvent(workflowId, 'verification passed');
      await startCreditCheck(producer, workflowId, userId);
    } else {
      await setStatus(workflowId, 'failed', 'done');
      await saveEvent(workflowId, 'verification failed: ' + reason);
    }
    return;
  }

  if (step === 'credit_check') {
    if (success) {
      await setStatus(workflowId, 'completed', 'done');
      await saveEvent(workflowId, 'credit approved score=' + score);
    } else {
      await saveEvent(workflowId, 'credit rejected: ' + reason);
      await undoVerification(
        producer,
        workflowId,
        userId,
        reason || 'credit_rejected'
      );
    }
  }
}

async function resumeUnfinished(producer) {
  const { rows } = await query(
    `SELECT * FROM workflows WHERE status = 'running'`
  );
  console.log('resume count:', rows.length);

  for (const wf of rows) {
    if (wf.current_step === 'verification') {
      await startVerification(producer, wf.id, wf.user_id);
    } else if (wf.current_step === 'credit_check') {
      await startCreditCheck(producer, wf.id, wf.user_id);
    }
  }
}

async function main() {
  const definition = loadWorkflowYaml();
  console.log('workflow:', definition.name);

  const kafka = createKafka('orchestrator');
  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: 'orchestrator-group' });

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.RESULTS });

  await resumeUnfinished(producer);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const result = JSON.parse(message.value.toString());
      await onWorkerResult(producer, result);
    },
  });

  const app = express();
  app.use(express.json());

  app.get('/health', async (req, res) => {
    res.json({
      ok: true,
      service: 'orchestrator',
      redis: (await redisPing()) ? 'up' : 'down',
    });
  });

  app.post('/workflows', async (req, res) => {
    const userId = req.body.userId || 'user-' + Date.now();
    const id = uuidv4();

    await query(
      `INSERT INTO workflows (id, user_id, status, current_step)
       VALUES ($1, $2, 'running', 'verification')`,
      [id, userId]
    );
    await saveEvent(id, 'created for ' + userId);
    await startVerification(producer, id, userId);

    res.status(202).json({
      id,
      userId,
      status: 'running',
    });
  });

  app.get('/workflows/:id', async (req, res) => {
    const cached = await cacheGet(req.params.id);
    if (cached) {
      return res.json({ ...cached, fromCache: true });
    }

    const wf = await query('SELECT * FROM workflows WHERE id = $1', [
      req.params.id,
    ]);
    if (!wf.rows[0]) return res.status(404).json({ error: 'not found' });

    const events = await query(
      `SELECT message, created_at FROM workflow_events
       WHERE workflow_id = $1 ORDER BY id`,
      [req.params.id]
    );

    const body = {
      ...wf.rows[0],
      timeline: events.rows,
      fromCache: false,
    };
    await cacheSet(req.params.id, body);
    res.json(body);
  });

  app.get('/workflows', async (req, res) => {
    const { rows } = await query(
      `SELECT id, user_id, status, current_step, created_at
       FROM workflows ORDER BY created_at DESC LIMIT 20`
    );
    res.json(rows);
  });

  app.listen(PORT, () => {
    console.log('orchestrator on', PORT);
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
