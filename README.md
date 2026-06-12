# StreamCore Pipeline

Serverless AWS event-processing pipeline built on Lambda, Kinesis, DynamoDB, and Step Functions.

## Project layout

| Path | Purpose |
|------|---------|
| `functions/` | One folder per Lambda function |
| `layers/` | Shared Lambda layers |
| `statemachines/` | Step Functions definitions |
| `events/` | Sample JSON test events for local invocation |
| `tests/` | Unit and integration tests |
| `template.yaml` | SAM/CloudFormation infrastructure declaration |
| `samconfig.toml` | Per-environment deploy configuration |
| `docs/` | Specs, ADRs, build ledger, work orders |

## Quick start

```bash
sam build
sam deploy --config-env dev
```

## Region
eu-west-1 (Ireland) — GDPR constraint C-01