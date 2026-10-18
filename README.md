# FlowMaster

KYC loan workflow using 3 Node services + Kafka.

Flow: verify ID → credit check.  
If credit fails, verification is rolled back (saga compensation).

## Services

- `services/orchestrator` – API + saga logic (port 3000)
- `services/verification` – ID check (port 3001)
- `services/credit` – credit score (port 3002)

Services talk over Kafka topics (`workflow.commands`, `workflow.results`, `workflow.compensate`).  
State goes to Postgres. Redis caches workflow status for GET.

Workflow steps are in `workflows/kyc-loan.yaml`.

## Saga (success vs failure)

```
SUCCESS:
  POST /workflows
    -> start_verification
    -> verification ok
    -> start_credit_check
    -> credit ok
    -> status = completed

FAILURE (credit reject):
  POST /workflows
    -> start_verification
    -> verification ok
    -> start_credit_check
    -> credit reject
    -> undo_verification
    -> status = compensated
```

## Setup

Need Postgres, Kafka, Redis, Node 18+.

```bash
# db (once)
psql -h localhost -d postgres -c "CREATE USER flowmaster WITH PASSWORD 'flowmaster' SUPERUSER;"
psql -h localhost -d postgres -c "CREATE DATABASE flowmaster OWNER flowmaster;"
psql -h localhost -U flowmaster -d flowmaster -f scripts/init.sql

# topics (once)
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic workflow.commands --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic workflow.results --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic workflow.compensate --partitions 3 --replication-factor 1

npm run install:all
```

Or use `docker compose up -d` if you have Docker (starts postgres/kafka/redis).

## Run

Three terminals:

```bash
npm run start:verification
npm run start:credit
npm run start:orchestrator
```

## Test

```bash
# normal user -> completed
npm run demo:happy

# userId starts with fail- -> credit rejects, verification undone
npm run demo:fail
```

```bash
curl -X POST http://localhost:3000/workflows \
  -H 'content-type: application/json' \
  -d '{"userId":"amit"}'

curl http://localhost:3000/workflows/<id>
```

| userId prefix | result |
|---------------|--------|
| normal        | completed |
| `bad-`        | verification fails |
| `fail-`       | compensated |

## Crash recovery

Orchestrator stores `status` + `current_step` in Postgres.  
On restart it picks up rows still `running` and re-sends that step.

## API

- `POST /workflows` – start
- `GET /workflows/:id` – status (+ timeline)
- `GET /workflows` – recent list
- `GET /health`
