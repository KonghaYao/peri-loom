# P6 — Cloud Platform

## 目标

在 P0–P5 已证明数据库模型、diskless storage、stateless worker 和 distributed ownership 正确后，再进行完整产品化。

P6 不再改变核心数据库正确性模型，主要做平台能力扩展。

## 1. Gateway / Router

- DB → Worker routing
- connection forwarding
- reconnect / retry
- migration-aware routing
- regional routing
- TLS
- streaming result forwarding

## 2. Management API

- create DB
- delete DB
- suspend / resume
- branch
- snapshot
- clone
- restore
- migration
- database status

## 3. Multi-tenancy

- organization
- project
- tenant
- DB authorization
- quotas
- rate limits
- resource accounting
- noisy-neighbor protection

## 4. Security

- API keys
- short-lived DB token
- service account
- RBAC
- TLS
- encryption at rest
- KMS
- audit

## 5. DBA / Admin

- SQL console
- schema browser
- branch graph
- generation viewer
- WAL status
- cache stats
- force checkpoint
- restore/recovery tooling
- GC inspection

## 6. Observability

关键 metrics：

- query latency
- commit latency
- remote WAL durability latency
- segment GET latency
- segment PUT latency
- cache hit ratio
- cold-start latency
- checkpoint latency
- WAL backlog
- active DBs / Worker
- Worker saturation
- failover latency

## 7. SDK / CLI

- management CLI
- SQL shell
- Rust SDK
- Go SDK
- TypeScript SDK
- local development mode

## 8. Deployment

- Docker image
- Docker Compose
- Kubernetes / Helm
- S3-compatible deployment
- MinIO local development profile

## Metadata Storage

P6 再决定 Control Plane metadata 的最终持久化方案。

候选可以包括：

- peri-loom dogfood peri-loom
- embedded metadata store
- external KV / consensus system
- standalone database

**P0–P5 不提前绑定 PostgreSQL 或其他外部数据库。**
