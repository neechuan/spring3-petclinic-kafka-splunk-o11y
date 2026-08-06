# OTel Kafka Metrics Receiver Configuration

## Status
✅ **ACTIVE** - OTel collector is running with both OTLP ingest and Kafka metrics scraping.

## Architecture
- **OTel Collector**: `quay.io/signalfx/splunk-otel-collector:latest` 
  - Container: `splunk-otel-collector`
  - Ports: 4317 (OTLP/gRPC), 4318 (OTLP/HTTP), 13133 (health)
  - Network: `petclinic-net` (shared with Kafka)
  
- **Kafka Broker**: `docker.io/apache/kafka:3.9.0`
  - Container: `petclinic-kafka`
  - Ports:
    - 9092 (`PLAINTEXT`) for container-network clients (e.g., collector)
    - 29092 (`PLAINTEXT_HOST`) for host JVM clients (frontend/backend)
  - Mode: KRaft (single-node, no Zookeeper)
  - Advertised Addresses:
    - `petclinic-kafka:9092` (DNS via `petclinic-net`)
    - `localhost:29092` (host-local app access)

## Configuration Files

### 1. `otel-kafka-metrics.yaml` (OTel Receiver Config)
- **Receivers**:
  - `otlp` on `0.0.0.0:4317` (gRPC) and `0.0.0.0:4318` (HTTP)
  - `kafka_metrics` (canonical name; replaces deprecated `kafkametrics` alias)
- **Kafka Metrics Receiver**:
  - Broker: `petclinic-kafka:9092`
  - Protocol Version: `2.0.0`
  - Scrapers: brokers, topics, consumers
  - Collection Interval: 1 minute
  - Initial Delay: 45 seconds
  - Topic Filter: `^[^_].*$` (excludes internal topics)
- **Extensions**:
  - `health_check` on `0.0.0.0:13133`
- **Pipelines**:
  - `traces`: `otlp -> batch -> signalfx`
  - `metrics`: `otlp + kafka_metrics -> batch -> signalfx`
- **Exporter**: SignalFx to `https://ingest.us1.signalfx.com` (realm from `.env`)

### 2. `run-collector.sh` (Startup Script)
- Mounts `otel-kafka-metrics.yaml` into container at `/etc/otel/collector/kafka_metrics_config.yaml`
- Exposes ports 4317, 4318, 13133
- Uses `--network petclinic-net` for DNS resolution
- Commands:
  - `start` (default)
  - `stop`
  - `status`
  - `restart`
  - `logs`
- Backward-compatible aliases:
  - `up` = `start`
  - `down` = `stop`

### 3. `run-all.sh` (Kafka Startup Script)
- Starts Kafka on `petclinic-net` network
- Configures dual listeners for container + host clients
- Uses KRaft mode with single controller+broker node

## Network Setup
Both containers run on the `petclinic-net` custom Podman network to enable DNS resolution:
```bash
podman network create petclinic-net  # (already exists)
```

## Startup Instructions

### Start Kafka + Collector
```bash
./run-all.sh kafka &    # Start Kafka in background
./run-collector.sh start
```

### Start Applications (Host JVMs)
```bash
./run-otel.sh apps
```
Applications connect to Kafka via `localhost:29092`.

### Verify Collector Health
```bash
curl -s http://localhost:13133/
```

### Stop Everything
```bash
./run-collector.sh stop
./stop-all.sh kafka
```

## Telemetry Flows
```
Frontend/Backend JVMs (host)
  ↓ OTLP (localhost:4318/4317)
OTel Collector (splunk-otel-collector)
    ↓ (SignalFx exporter)
Splunk Observability Cloud (us1 realm)
```

```
Kafka Broker (petclinic-kafka:9092)
  ↓ (kafka_metrics receiver every 1 min)
OTel Collector (splunk-otel-collector)
  ↓ (SignalFx exporter)
Splunk Observability Cloud (us1 realm)
    ↓
kafka.* metrics visible in Splunk
```

## Key Fixes Applied
1. **Kafka Hostname Resolution**: Added dual Kafka listeners so host apps do not resolve `petclinic-kafka` directly.
2. **Advertised Listeners Split**: `petclinic-kafka:9092` for container network, `localhost:29092` for host clients.
3. **Receiver Canonicalization**: Switched to `kafka_metrics` receiver key (no deprecated alias warning).
4. **Startup Scrape Stability**: Added 45s initial delay for Kafka metrics scrape to avoid startup connection-refused noise.
5. **OTLP Ingest Restored**: Added `otlp` receiver and traces pipeline to fix agent errors (`unexpected end of stream` to `:4318`).
6. **Health Endpoint Restored**: Added `health_check` extension so `:13133` returns HTTP 200 with status JSON.

## Next Steps
1. Monitor `logs/frontend.log` and `logs/backend.log` after startup for any new Kafka/OTLP errors.
2. Validate `kafka.*` metrics in Splunk Observability Cloud.
3. Add alert rules for consumer lag, partitions, and broker availability.
