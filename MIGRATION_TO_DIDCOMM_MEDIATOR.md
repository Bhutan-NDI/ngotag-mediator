# Migration to `ngotag-didcomm-mediator` on ECS Fargate

This runbook describes how to move from the old `ngotag-mediator` service to the new `ngotag-didcomm-mediator` service.

The important point: **for the final production migration, stop the old mediator before copying Askar storage into Drizzle**. If the old mediator keeps running while migration is happening, it can keep writing new connections, mediation records, queued messages, or push records into the old Postgres/Askar store after the migration task has already copied data. That creates data drift.

You can prepare almost everything before downtime, but the actual Askar-to-Drizzle copy should happen while the old service is scaled to `0`.

## Target Architecture

Old mediator:

- ECS Fargate service running `ngotag-mediator`
- Postgres/Askar storage
- Env names like `WALLET_NAME`, `WALLET_KEY`, `POSTGRES_HOST`

New mediator:

- ECS Fargate service running `ngotag-didcomm-mediator`
- Drizzle/Postgres for main Credo storage
- Askar still used for KMS, pointing to the old wallet/store
- DynamoDB for message pickup queue
- Redis for cache and multi-instance delivery

Postgres plan:

- Keep the existing old Askar database as-is.
- In pgAdmin, create a second database in the same Postgres instance for Drizzle.
- Example old DB/store: `mediator`
- Example new DB: `mediator_drizzle`

Do not restore the old Postgres DB directly into the new Drizzle DB. They are different schemas.

## Should The Old Mediator Run?

Before final migration:

- Yes, the old mediator can keep running while you build/push the new image, create DynamoDB/Redis, create the new Postgres database in pgAdmin, and prepare task definitions.
- Yes, you can run the Drizzle schema migration against the empty new Drizzle database before downtime because it does not read from or modify the old Askar database.

During final data migration:

- No. Scale the old mediator service to `0` before running `migrate-askar-to-drizzle`.
- Keep it stopped until the new mediator service is started and validated, or until you intentionally roll back.

After migration:

- Start the new `ngotag-didcomm-mediator` service as a separate ECS service with a separate steady-state task definition.
- Do not run old and new mediators against the same wallet/store at the same time.

## Queue Caveat

The old mediator stores queued messages as `MessageRecord` records in Askar/Postgres. The migration can copy those records into Drizzle, but if the new runtime uses:

```dotenv
MESSAGE_PICKUP__STORAGE__TYPE=dynamodb
```

then the new mediator reads queued messages from DynamoDB, not from migrated Drizzle `MessageRecord` rows.

Choose one strategy:

1. Preferred: drain old queued messages before downtime, then cut over with an empty DynamoDB queue.
2. Safer for pending messages: start the new mediator temporarily with `MESSAGE_PICKUP__STORAGE__TYPE=credo`, let clients pick up migrated queued messages from Drizzle, then switch the service to DynamoDB.
3. Strict no-loss queue migration: write a dedicated old `MessageRecord` to DynamoDB migration. This repo does not currently include that.

For most production cutovers, use option 1 and schedule a short maintenance window.

## Env Mapping

Use the same public mediator domain in the new service so clients do not need new endpoints.

| Old env | New env | Notes |
| --- | --- | --- |
| `AGENT_PORT` | `AGENT_PORT` | Usually `3000`. |
| `AGENT_NAME` | `AGENT_NAME` | Public mediator label. |
| `AGENT_ENDPOINTS` | `AGENT_ENDPOINTS` | Keep current `https://...` and `wss://...`. |
| `INVITATION_URL` | `INVITATION_URL` | Keep if currently used. |
| `WALLET_NAME` | `ASKAR__STORE_ID` | Must match old wallet/store name. |
| `WALLET_KEY` | `ASKAR__STORE_KEY` | Must match old wallet key. |
| `POSTGRES_HOST` | `ASKAR__DATABASE__HOST` | Same host and port. |
| `POSTGRES_USER` | `ASKAR__DATABASE__USER` | Old Postgres user. |
| `POSTGRES_PASSWORD` | `ASKAR__DATABASE__PASSWORD` | Old Postgres password. |
| `POSTGRES_ADMIN_USER` | `ASKAR__DATABASE__ADMIN_USER` | Use if available. |
| `POSTGRES_ADMIN_PASSWORD` | `ASKAR__DATABASE__ADMIN_PASSWORD` | Use if available. |
| `LOG_LEVEL` | `LOG_LEVEL` | New values are strings like `info`, `debug`, `warn`. |
| `NOTIFICATION_WEBHOOK_URL` | `PUSH_NOTIFICATIONS__WEBHOOK_URL` | If push webhook is still needed. |

## ECS Task Definitions

Use the same Docker image for both migration and runtime:

```text
<account>.dkr.ecr.<region>.amazonaws.com/ngotag-didcomm-mediator:<tag>
```

Create two task definition families:

1. `ngotag-didcomm-mediator-migration`
   - Same image.
   - Same VPC/subnets/security groups as the app.
   - Used only with ECS `RunTask`.
   - Override entrypoint/command per migration step.
   - Can use the same S3 `environmentFiles` pattern as the current deployment.

2. `ngotag-didcomm-mediator-service`
   - Same image.
   - No command override.
   - Runs the default container entrypoint: `node apps/mediator/build/index.js`.
   - Used by the long-running ECS service after migration.

You can also use one task definition and override command for one-off `RunTask`, but separate task definitions make the migration/runtime intent clearer.

## Runtime Env For New Service

Set these on the new long-running service task definition or its S3 env file.

```dotenv
# Node/runtime
NODE_OPTIONS=--max-old-space-size=512
MALLOC_CONF=background_thread:true,metadata_thp:auto
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2

# Agent
AGENT_PORT=3000
AGENT_NAME=NGOTag Mediator
AGENT_ENDPOINTS=https://<current-mediator-domain>,wss://<current-mediator-domain>
INVITATION_URL=https://<current-mediator-domain>/invitation
LOG_LEVEL=info
CREATE_NEW_INVITATION=false

# KMS stays on Askar using old wallet/store
KMS__TYPE=askar
ASKAR__STORE_ID=<old WALLET_NAME>
ASKAR__STORE_KEY=<old WALLET_KEY>
ASKAR__KEY_DERIVATION_METHOD=kdf:argon2i:mod
ASKAR__DATABASE__TYPE=postgres
ASKAR__DATABASE__HOST=<old POSTGRES_HOST>
ASKAR__DATABASE__USER=<old POSTGRES_USER>
ASKAR__DATABASE__PASSWORD=<old POSTGRES_PASSWORD>
ASKAR__DATABASE__ADMIN_USER=<old POSTGRES_ADMIN_USER>
ASKAR__DATABASE__ADMIN_PASSWORD=<old POSTGRES_ADMIN_PASSWORD>

# New Drizzle DB created in pgAdmin
STORAGE__TYPE=drizzle
STORAGE__DIALECT=postgres
STORAGE__DATABASE_URL=postgresql://<user>:<password>@<same-postgres-host>:5432/<new_drizzle_db>

# DynamoDB queue
MESSAGE_PICKUP__FORWARDING_STRATEGY=QueueAndLiveModeDelivery
MESSAGE_PICKUP__STORAGE__TYPE=dynamodb
MESSAGE_PICKUP__STORAGE__REGION=<aws-region>
MESSAGE_PICKUP__STORAGE__ACCESS_KEY_ID=<access-key-id>
MESSAGE_PICKUP__STORAGE__SECRET_ACCESS_KEY=<secret-access-key>
MESSAGE_PICKUP__STORAGE__TABLE_NAME=ngotag-mediator-queued-messages

# Redis
CACHE__TYPE=redis
CACHE__REDIS_URL=redis://<redis-endpoint>:6379
MESSAGE_PICKUP__MULTI_INSTANCE_DELIVERY__TYPE=redis

# Optional push webhook
PUSH_NOTIFICATIONS__WEBHOOK_URL=<https-webhook-url>
```

The new config currently requires `MESSAGE_PICKUP__STORAGE__ACCESS_KEY_ID` and `MESSAGE_PICKUP__STORAGE__SECRET_ACCESS_KEY`. Store these in ECS secrets or an S3 env file, not directly in committed task definition JSON.

## One-Time Migration Env

The migration task needs the same Askar and Drizzle env as the runtime. The Drizzle schema migration also needs `DRIZZLE_DATABASE_URL`.

```dotenv
DRIZZLE_DATABASE_URL=postgresql://<user>:<password>@<same-postgres-host>:5432/<new_drizzle_db>

STORAGE__TYPE=drizzle
STORAGE__DIALECT=postgres
STORAGE__DATABASE_URL=postgresql://<user>:<password>@<same-postgres-host>:5432/<new_drizzle_db>

KMS__TYPE=askar
ASKAR__STORE_ID=<old WALLET_NAME>
ASKAR__STORE_KEY=<old WALLET_KEY>
ASKAR__KEY_DERIVATION_METHOD=kdf:argon2i:mod
ASKAR__DATABASE__TYPE=postgres
ASKAR__DATABASE__HOST=<old POSTGRES_HOST>
ASKAR__DATABASE__USER=<old POSTGRES_USER>
ASKAR__DATABASE__PASSWORD=<old POSTGRES_PASSWORD>
ASKAR__DATABASE__ADMIN_USER=<old POSTGRES_ADMIN_USER>
ASKAR__DATABASE__ADMIN_PASSWORD=<old POSTGRES_ADMIN_PASSWORD>
```

## Migration Flow

### 1. Capture Current Production Values

Before changing anything, record:

```dotenv
OLD_ECS_CLUSTER=
OLD_ECS_SERVICE=
OLD_TASK_DEFINITION=
OLD_IMAGE_TAG=
OLD_ENV_FILE_S3_ARN=
OLD_TARGET_GROUP=
OLD_DESIRED_COUNT=

AGENT_PORT=
AGENT_NAME=
AGENT_ENDPOINTS=
INVITATION_URL=
WALLET_NAME=
WALLET_KEY=
POSTGRES_HOST=
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_ADMIN_USER=
POSTGRES_ADMIN_PASSWORD=
NOTIFICATION_WEBHOOK_URL=
```

Also take an RDS snapshot or a `pg_dump` backup before the cutover window.

### 2. Build And Push The New Image

From `../ngotag-didcomm-mediator`:

```sh
docker build -f apps/mediator/Dockerfile \
  -t <account>.dkr.ecr.<region>.amazonaws.com/ngotag-didcomm-mediator:<tag> .

docker push <account>.dkr.ecr.<region>.amazonaws.com/ngotag-didcomm-mediator:<tag>
```

Required values:

```dotenv
AWS_REGION=
AWS_ACCOUNT_ID=
ECR_REPOSITORY=ngotag-didcomm-mediator
IMAGE_TAG=
```

### 3. Create The New Drizzle Database In pgAdmin

In your pgAdmin Docker container UI:

1. Connect to the same existing Postgres instance used by the old mediator.
2. Create a new database, for example `mediator_drizzle`.
3. Use a Postgres user that the ECS task can connect with.
4. Do not modify or drop the old wallet/store database.

The new Drizzle connection string will look like:

```dotenv
STORAGE__DATABASE_URL=postgresql://<user>:<password>@<postgres-host>:5432/mediator_drizzle
DRIZZLE_DATABASE_URL=postgresql://<user>:<password>@<postgres-host>:5432/mediator_drizzle
```

### 4. Provision Redis And DynamoDB

Redis:

- Use ElastiCache or MemoryDB reachable from the ECS task subnets.
- Allow inbound Redis traffic from the ECS task security group.

DynamoDB:

- Table name: `ngotag-mediator-queued-messages`
- Partition key: `connectionId` as string
- Sort key: `messageId` as number

The mediator can try to create the table itself on startup, but pre-creating it in AWS is cleaner. Give the task role permission for:

```text
dynamodb:CreateTable
dynamodb:DescribeTable
dynamodb:Query
dynamodb:Scan
dynamodb:UpdateItem
dynamodb:DeleteItem
```

### 5. Create The Migration Task Definition

Create `ngotag-didcomm-mediator-migration` with:

- Launch type: Fargate
- Network mode: `awsvpc`
- Same subnets/security groups as the app service
- Same image as the future app service
- Environment from a temporary migration S3 env file or ECS secrets
- CloudWatch logs enabled

Do not make this an ECS service. Run it as a one-off ECS task.

### 6. Run Drizzle Schema Migration

This step can be run before stopping the old mediator because it only prepares the new empty Drizzle database.

Run one ECS Fargate task from `ngotag-didcomm-mediator-migration` with:

```json
{
  "entryPoint": ["sh", "-lc"],
  "command": ["pnpm --filter didcomm-mediator-service run drizzle:migrate:postgres"]
}
```

Required env:

```dotenv
DRIZZLE_DATABASE_URL=postgresql://<user>:<password>@<postgres-host>:5432/<new_drizzle_db>
```

Check CloudWatch logs. The task should exit with code `0`.

### 7. Start The Downtime Window

Scale the old mediator to `0`:

```sh
aws ecs update-service \
  --cluster <old-cluster> \
  --service <old-mediator-service> \
  --desired-count 0

aws ecs wait services-stable \
  --cluster <old-cluster> \
  --services <old-mediator-service>
```

Confirm no old mediator tasks are running. From this point until cutover, clients may be unable to use mediation. That is intentional to avoid data drift.

### 8. Run Askar-To-Drizzle Data Migration

Run a second one-off ECS Fargate task from the same migration task definition with:

```json
{
  "entryPoint": ["sh", "-lc"],
  "command": ["pnpm --filter didcomm-mediator-service run migrate-askar-to-drizzle"]
}
```

Required env:

```dotenv
STORAGE__TYPE=drizzle
STORAGE__DIALECT=postgres
STORAGE__DATABASE_URL=postgresql://<user>:<password>@<postgres-host>:5432/<new_drizzle_db>

KMS__TYPE=askar
ASKAR__STORE_ID=<old WALLET_NAME>
ASKAR__STORE_KEY=<old WALLET_KEY>
ASKAR__KEY_DERIVATION_METHOD=kdf:argon2i:mod
ASKAR__DATABASE__TYPE=postgres
ASKAR__DATABASE__HOST=<old POSTGRES_HOST>
ASKAR__DATABASE__USER=<old POSTGRES_USER>
ASKAR__DATABASE__PASSWORD=<old POSTGRES_PASSWORD>
ASKAR__DATABASE__ADMIN_USER=<old POSTGRES_ADMIN_USER>
ASKAR__DATABASE__ADMIN_PASSWORD=<old POSTGRES_ADMIN_PASSWORD>
```

Check CloudWatch logs. The task should exit with code `0`.

Do not run `migrate-askar-to-drizzle-delete-storage-records` during cutover. Keep old storage intact until production has been stable.

### 9. Create The New Runtime Task Definition

Create `ngotag-didcomm-mediator-service` with:

- Same image
- Default container entrypoint
- Port `3000`
- Same public `AGENT_ENDPOINTS`
- Drizzle runtime env
- Askar KMS env
- DynamoDB queue env
- Redis env
- CloudWatch logs

This is the task definition used by the long-running service.

### 10. Start The New ECS Service

Create a new ECS service or update your deployment pipeline to create one:

```sh
aws ecs create-service \
  --cluster <cluster> \
  --service-name ngotag-didcomm-mediator \
  --task-definition ngotag-didcomm-mediator-service:<revision> \
  --desired-count 1 \
  --launch-type FARGATE \
  --network-configuration '<awsvpc config>' \
  --load-balancers '<target group config>'
```

Keep the same ALB listener/domain that clients already use. Only the backing target service changes.

After service is stable:

```sh
aws ecs wait services-stable \
  --cluster <cluster> \
  --services ngotag-didcomm-mediator
```

### 11. Validate Cutover

Check:

- ECS service reaches steady state.
- CloudWatch logs show agent initialization without wallet/key errors.
- New service connects to the new Drizzle database.
- New service connects to the old Askar store for KMS.
- New service connects to Redis.
- New service can describe/create/use the DynamoDB table.
- Existing clients reconnect to the same mediator domain.
- New mediation invitation works.
- Offline messages queue in DynamoDB and are picked up after reconnect.
- Push webhook works if enabled.

### 12. Rollback Plan

Before cleanup, rollback is simple:

```sh
aws ecs update-service \
  --cluster <new-cluster> \
  --service ngotag-didcomm-mediator \
  --desired-count 0

aws ecs update-service \
  --cluster <old-cluster> \
  --service <old-mediator-service> \
  --desired-count <old-desired-count>
```

Rollback is cleanest if you decide quickly before clients process much traffic through the new mediator. If the new service handled real traffic for a while, review consistency before running the old mediator again.

### 13. Cleanup Later

Only after production is stable and backups are verified:

- Keep or archive the old task definition.
- Keep the RDS snapshot.
- Optionally run the delete-storage migration if you want to remove migrated storage records from Askar:

```sh
pnpm --filter didcomm-mediator-service run migrate-askar-to-drizzle-delete-storage-records
```

Run that as a one-off ECS task only after you are fully confident. Askar is still needed for KMS, so do not delete the old Askar database itself.
