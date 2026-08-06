# Spring PetClinic — Kafka-connected edition

A distributed take on the classic [Spring PetClinic](https://github.com/spring-projects/spring-petclinic)
sample. The application is split into **two standalone Spring Boot apps** that
communicate over an **Apache Kafka** broker using the **request/reply** pattern
implemented with Spring Kafka's `ReplyingKafkaTemplate`.

| Application        | Folder                   | Port     | Responsibility                                                                                                                     |
| ------------------ | ------------------------ | -------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| **Frontend** | [`frontend/`](frontend) | `8080` | Thymeleaf UI + controllers. Owns no database — every read/write is a synchronous Kafka request/reply call to the backend.         |
| **Backend**  | [`backend/`](backend)   | `8081` | JPA persistence on an in-memory **HSQLDB** (seeded on startup). Consumes RPC topics, executes the operation, and replies. |

## Architecture

```mermaid
flowchart LR
    Browser -->|HTTP :8080| Frontend
    Frontend -->|"request/reply<br/>petclinic.rpc.*"| Kafka[(Apache Kafka\nKRaft mode)]
    Kafka --> Backend
    Backend -->|JPA| HSQLDB[(HSQLDB in-memory)]
```

- The **frontend** sends a request to `petclinic.rpc.<operation>` and blocks on the
  reply (default timeout `10000 ms`, see `kafka.request.timeout-ms`).
- The **backend** listens on each `petclinic.rpc.<operation>` topic, executes the JPA
  operation, and sends the reply to the caller's reply topic.
- Message payloads are JSON (Jackson 2, shipped with Spring Boot 3.5).

### RPC topic contract

| Topic                                      | Operation                         |
| ------------------------------------------ | --------------------------------- |
| `petclinic.rpc.owner.findById`           | Load one owner (with pets/visits) |
| `petclinic.rpc.owner.findByLastName`     | Paged owner search                |
| `petclinic.rpc.owner.save`              | Create/update an owner aggregate  |
| `petclinic.rpc.pettype.findAll`         | List pet types                    |
| `petclinic.rpc.vet.findAll`             | List vets                         |
| `petclinic.rpc.vet.findAllPaged`        | Paged vet list                    |

Topic constants are defined in both apps' `RpcTopics` classes
([backend](backend/src/main/java/org/springframework/samples/petclinic/messaging/RpcTopics.java),
[frontend](frontend/src/main/java/org/springframework/samples/petclinic/messaging/RpcTopics.java)),
where `PREFIX = "petclinic.rpc."` is prepended to each operation suffix above.

#### Messaging implementation

- **Backend listener:** [`KafkaRpcListener`](backend/src/main/java/org/springframework/samples/petclinic/messaging/KafkaRpcListener.java)
  uses `@KafkaListener` + `@SendTo` for each operation topic; [`KafkaConfig`](backend/src/main/java/org/springframework/samples/petclinic/messaging/KafkaConfig.java)
  declares each topic as a `NewTopic` bean and wires `KafkaTemplate` as the reply sender.
- **Frontend client:** [`KafkaRpcClient`](frontend/src/main/java/org/springframework/samples/petclinic/messaging/KafkaRpcClient.java)
  uses `ReplyingKafkaTemplate` to send a request and await the reply synchronously.
- **Broker connection** (default in `application.properties`, overridable via environment):
  - `spring.kafka.bootstrap-servers=localhost:29092` — host JVM access via the `PLAINTEXT_HOST` listener

> **Implementation note:** both apps run on **Spring Boot 3.5** with **Spring Kafka**.
> The Kafka broker runs in **KRaft mode** (no Zookeeper) with dual listeners so both
> containerised clients (port `9092`) and host JVMs (port `29092`) can reach it.

## Technology Stack

### Application Framework

- **Spring Boot 3.5** — modern Java application framework
- **Spring Kafka** — Kafka producer/consumer with `ReplyingKafkaTemplate` for synchronous request/reply
- **Java 17** — bundled as Azul Zulu 17.0.19; set `JAVA_HOME` to override
- **Maven** — build tool (via system `mvn`)
- **Jackson 2** — JSON serialization (package `com.fasterxml.jackson`)

### Application Components

- **Frontend** (`frontend/pom.xml`) — **Thymeleaf** UI + Spring MVC controllers; exports OTLP traces/metrics via Splunk OTel Java agent
- **Backend** (`backend/pom.xml`) — **JPA/Hibernate** + **HSQLDB** in-memory; listens on RPC topics and persists data

### Event Broker & Messaging

- **Apache Kafka 3.9.0** — runs as a **Podman** container (`docker.io/apache/kafka:3.9.0`), KRaft mode (no Zookeeper)
- **Dual listeners:**
  - `PLAINTEXT` on `:9092` — for containerised clients on `petclinic-net` (e.g., the OTel Collector's `kafka_metrics` receiver)
  - `PLAINTEXT_HOST` on `:29092` — for host JVM clients (frontend/backend running via `run-all.sh` / `run-otel.sh`)
- **Topic pattern:** `petclinic.rpc.<operation>` — frontend requestor, backend replier

### Observability

- **Splunk Distribution of OpenTelemetry Java agent** — bootstrapped via `-javaagent`; sends traces + metrics + logs (disabled by default) to OTLP/HTTP `:4318`
- **Splunk Distribution of OpenTelemetry Collector** — runs as a **Podman** container (`quay.io/signalfx/splunk-otel-collector:latest`) on `petclinic-net`; gateway mode that forwards to Splunk Observability Cloud (`realm=us1` by default)
- **Kafka metrics** — the Collector scrapes the Kafka broker every 60 s via the `kafka_metrics` receiver in [`otel-kafka-metrics.yaml`](otel-kafka-metrics.yaml); metrics are forwarded to Splunk alongside APM data
- **OpenTelemetry pipelines** — traces (OTLP/HTTP → Splunk), metrics (OTLP/HTTP → Splunk, includes Kafka broker metrics), logs (HEC, requires Log Observer)

### Runtime & Containerization

- **Podman v6.0.2+** — container orchestration (macOS: applehv VM `podman-machine-default`, rootless)
- **`petclinic-net`** — custom Podman network shared by the Kafka and OTel Collector containers for DNS resolution (`petclinic-kafka:9092`)
- **Scripting** — bash orchestration (`run-all.sh`, `run-otel.sh`, `run-collector.sh`, `stop-all.sh`)
- **Ports:**
  - Frontend: `:8080`
  - Backend: `:8081`
  - Kafka (host JVMs): `:29092`
  - Kafka (container network): `:9092`
  - OTel Collector OTLP/gRPC: `:4317`
  - OTel Collector OTLP/HTTP: `:4318`
  - OTel Collector health: `:13133`

## Prerequisites

- **JDK 17+** (full JDK, not a JRE)
- **Maven** on your `PATH` (the bundled `./mvnw` wrapper is not configured in this
  repo, so the scripts fall back to system `mvn`)
- **Podman** (or Docker), to run the Kafka broker and OTel Collector
- `curl`, `nc`, and `lsof` (used by the start/stop scripts for health checks and
  shutdown; preinstalled on macOS)
- **`petclinic-net` Podman network** — shared by Kafka and the OTel Collector for
  DNS resolution; create it once if it does not already exist:
  ```bash
  podman network create petclinic-net
  ```

## Running the distributed edition

The quickest way is the **`run-all.sh`** orchestrator, which starts the broker,
waits for it, then brings up the apps in order:

```bash
./run-all.sh            # start kafka + backend + frontend (default)
./run-all.sh apps       # backend then frontend (broker already running)
./run-all.sh kafka      # just the broker (detached container)
./run-all.sh backend    # just the backend (foreground, live logs, Ctrl+C stops)
./run-all.sh frontend   # just the frontend (foreground, live logs, Ctrl+C stops)
```

- A **single** requested app runs in the foreground with live logs (Ctrl+C stops it).
- **Multiple** apps run in the background with logs written to [`logs/`](logs) and
  are stopped together with Ctrl+C.
- The **broker** always runs detached; stop it with `./stop-all.sh kafka`.

Stop services with the mirror script **`stop-all.sh`** (reverse order:
frontend, backend, kafka):

```bash
./stop-all.sh           # stop everything (default)
./stop-all.sh apps      # stop frontend + backend, leave the broker up
./stop-all.sh frontend  # stop just the frontend
./stop-all.sh kafka     # stop and remove the broker container
```

### Running with the Splunk OpenTelemetry Java agent

To bring the stack up with each app instrumented by the **Splunk Distribution of
OpenTelemetry Java agent**, use **`run-otel.sh`** instead of `run-all.sh`. It
launches the packaged Spring Boot fat jars directly (one JVM per app) with
`-javaagent` bootstrapped, so each app reports as its own service in Splunk APM:

```bash
./run-otel.sh            # broker + backend + frontend, agent attached (default)
./run-otel.sh apps       # backend then frontend (broker already up)
./run-otel.sh backend    # just the backend (foreground, live logs)
./run-otel.sh build      # force a `mvn package` rebuild before starting
OTEL_ENABLED=false ./run-otel.sh apps   # run the jars without the agent
```

The script launches the apps as packaged Spring Boot fat jars (not via Maven) with the Splunk OTel Java agent attached via `-javaagent`. Each app reports to Splunk APM as its own service:

- **Backend:** `gary-petclinic-kafka-backend`
- **Frontend:** `gary-petclinic-kafka-frontend`

**Runtime environment:** Apps run on the bundled **Azul Zulu 17.0.19** JRE in [`jre/`](jre); override with `JAVA_HOME` if needed.

**Telemetry routing:** Traces and metrics are always sent to the **local Collector** on `localhost:4318` (OTLP/HTTP). Logs are disabled by default (see the [logs caveat](#collector-logs-a-404-not-found-on-v1log-and-drops-data) below). The script forces `-Dsplunk.realm=none` on the agent to prevent a `SPLUNK_REALM` value in `.env` from making the agent bypass the Collector and send directly to Splunk. If the Collector is not already running, `run-otel.sh` starts it automatically (see [The Splunk OpenTelemetry Collector](#the-splunk-opentelemetry-collector) below).

**Configuration:** Agent settings are in a config block at the top of `run-otel.sh` and can be overridden from the environment (e.g. `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_LOGS_EXPORTER`). For defaults, see the [Observability defaults table](#observability-defaults-in-run-otelsh) above.

**Credentials:** The realm and access token belong to the **Collector**, not the agent. Both `run-otel.sh` and `run-collector.sh` auto-load them from a gitignored **`.env`** file in the repo root — copy [`.env.example`](.env.example) to `.env` and fill in your Splunk realm and org access token.

**Stopping:** Use `./stop-all.sh` to stop the apps and broker (the Collector stays running). Stop just the Collector with `./run-collector.sh stop`.

Once the stack is up, open the **launcher page** [`index.html`](index.html) in a
browser for quick links, or go straight to:

| Service                | URL                                                                           | Notes                            |
| ---------------------- | ----------------------------------------------------------------------------- | -------------------------------- |
| PetClinic UI           | [http://localhost:8080/](http://localhost:8080/)                               | Thymeleaf frontend               |
| Backend health         | [http://localhost:8081/actuator/health](http://localhost:8081/actuator/health) | Spring Boot Actuator             |
| OTel Collector health  | [http://localhost:13133/](http://localhost:13133/)                             | Returns JSON status              |

### The Splunk OpenTelemetry Collector

Instead of shipping telemetry from each JVM straight to Splunk Observability
Cloud, the apps export to a **local Splunk Distribution of the OpenTelemetry
Collector** running as a Podman container on `petclinic-net`. The Collector fans
the data out to the cloud and also scrapes Kafka broker metrics directly:

```mermaid
flowchart LR
    subgraph JVMs["App JVMs (Splunk OTel Java agent)"]
        BE[backend]
        FE[frontend]
    end
    BE -->|"OTLP http/protobuf<br/>localhost:4318"| COL
    FE -->|"OTLP http/protobuf<br/>localhost:4318"| COL
    Kafka[(petclinic-kafka:9092)] -->|"kafka_metrics<br/>every 60s"| COL
    COL["Splunk OTel Collector<br/>(Podman, gateway mode)"] -->|traces · otlp_http| Cloud[(Splunk Observability Cloud<br/>realm us1)]
    COL -->|metrics · otlp_http| Cloud
    COL -.->|logs · splunk_hec| Cloud
```

**Manage it with [`run-collector.sh`](run-collector.sh):**

```bash
./run-collector.sh start    # pull (if needed) and (re)start the Collector [default]
./run-collector.sh status   # show container state and published ports
./run-collector.sh logs     # follow the Collector logs
./run-collector.sh restart  # stop then start the Collector
./run-collector.sh stop     # stop and remove the Collector container
```

> Aliases: `up` = `start`, `down` = `stop` (backward-compatible with older scripts).

`run-otel.sh` also calls the Collector automatically: before it launches the app
JVMs it checks the health endpoint and runs `./run-collector.sh start` if nothing is
listening, so `./run-otel.sh` is enough to bring up the whole pipeline.

**How it starts.** `run-collector.sh start` runs the image detached with a restart
policy on `petclinic-net`, mounts [`otel-kafka-metrics.yaml`](otel-kafka-metrics.yaml)
as the active config, injects credentials from `.env` via the environment, and waits
for the container to report healthy on `:13133`:

```bash
podman run -d --replace --name splunk-otel-collector --restart unless-stopped \
  --network petclinic-net \
  -v ./otel-kafka-metrics.yaml:/etc/otel/collector/kafka_metrics_config.yaml \
  -e SPLUNK_ACCESS_TOKEN -e SPLUNK_REALM \
  -e SPLUNK_CONFIG=/etc/otel/collector/kafka_metrics_config.yaml \
  -e SPLUNK_MEMORY_TOTAL_MIB=512 -e SPLUNK_LISTEN_INTERFACE=0.0.0.0 \
  -p 4317:4317 -p 4318:4318 -p 13133:13133 \
  quay.io/signalfx/splunk-otel-collector:latest
```

**Configuration.** Everything is driven by environment variables (set them in
`.env`, or export them to override the script defaults):

| Variable                    | Default                                           | Purpose                                                        |
| --------------------------- | ------------------------------------------------- | -------------------------------------------------------------- |
| `SPLUNK_REALM`            | _(required)_                                    | Splunk O11y realm, e.g.`us1` — derives the cloud endpoints. |
| `SPLUNK_ACCESS_TOKEN`     | _(required)_                                    | Org access token used to authenticate ingest.                  |
| `SPLUNK_CONFIG`           | `/etc/otel/collector/kafka_metrics_config.yaml` | Mounted overlay config (OTLP + Kafka metrics receivers).       |
| `SPLUNK_MEMORY_TOTAL_MIB` | `512`                                           | Total memory budget for the`memory_limiter` processor.       |
| `SPLUNK_LISTEN_INTERFACE` | `0.0.0.0`                                       | Bind address inside the container (so published ports work).   |
| `SPLUNK_COLLECTOR_IMAGE`  | `quay.io/signalfx/splunk-otel-collector:latest` | Collector image to run.                                        |
| `SPLUNK_COLLECTOR_NAME`   | `splunk-otel-collector`                         | Container name.                                                |
| `COLLECTOR_HEALTH_URL`    | `http://localhost:13133`                        | Health endpoint the scripts poll before continuing.            |

**Published ports:** `4317` (OTLP/gRPC), `4318` (OTLP/HTTP — the agent target),
and `13133` (health check).

#### OTel Collector config: `otel-kafka-metrics.yaml`

The Collector loads [`otel-kafka-metrics.yaml`](otel-kafka-metrics.yaml) (mounted
at startup) instead of the stock `gateway_config.yaml`. This overlay adds the
`kafka_metrics` receiver and wires both OTLP ingest and Kafka metrics into the
same pipelines:

| Pipeline  | Receivers                    | Processors              | Exporters                     |
| --------- | ---------------------------- | ----------------------- | ----------------------------- |
| `traces`  | `otlp`                     | `attributes`, `batch` | `otlp_http/traces`          |
| `metrics` | `otlp`, `kafka_metrics`    | `attributes`, `batch` | `otlp_http/metrics`, `debug` |

**Kafka metrics receiver** (`kafka_metrics`):

| Setting               | Value                     |
| --------------------- | ------------------------- |
| Broker                | `petclinic-kafka:9092`  |
| Protocol version      | `2.0.0`                 |
| Collection interval   | `1m`                    |
| Initial delay         | `45s` (waits for broker startup) |
| Scrapers              | brokers, topics, consumers |
| Topic filter          | `^[^_].*$` (excludes internal `_` topics) |

> Use `initial_delay: 45s` to suppress `connect: connection refused` scrape errors
> while Kafka is still initialising.

**Bundled export endpoints** (all derived from `SPLUNK_REALM`):

| Signal  | Exporter            | Destination                                                    |
| ------- | ------------------- | -------------------------------------------------------------- |
| Traces  | `otlp_http/traces`  | `https://ingest.<realm>.signalfx.com/v2/trace/otlp`          |
| Metrics | `otlp_http/metrics` | `https://ingest.<realm>.observability.splunkcloud.com/v2/datapoint/otlp` |
| Logs    | `splunk_hec`        | `${SPLUNK_HEC_URL}` (`/v1/log`)                              |

> The collector image is **distroless** (no shell/`cat`); to inspect the mounted
> config or the bundled configs, copy them out with
> `podman cp splunk-otel-collector:/etc/otel/collector/ ./collector-config/`.
>
> **Logs caveat:** Splunk Observability Cloud only ingests logs when the org has
> **Log Observer** (a valid `SPLUNK_HEC_URL` + HEC token); otherwise the
> `splunk_hec` exporter 404s on `/v1/log` and drops the data. `run-otel.sh`
> therefore ships **traces and metrics only** by default (`OTEL_LOGS_EXPORTER=none`).
> Wire a working `SPLUNK_HEC_URL`/`SPLUNK_HEC_TOKEN` into the Collector and start
> the apps with `OTEL_LOGS_EXPORTER=otlp` to forward logs as well.

#### Getting into / debugging the Collector container

You **can't** open a shell inside the Collector — the image is distroless, so its
only executable is the `/otelcol` entrypoint (there's no `/bin/sh`, `ls`, or
`cat`, and `podman exec -it … ` fails with exit code `125`). Use these instead:

```bash
# 1. Run the collector binary (the only executable in the image)
podman exec splunk-otel-collector /otelcol --version      # -> otelcol version vX.Y.Z
podman exec splunk-otel-collector /otelcol components      # list receivers/exporters/etc.

# 2. Read files out of the container (no shell needed)
podman cp splunk-otel-collector:/etc/otel/collector/gateway_config.yaml -   # to stdout
podman cp splunk-otel-collector:/etc/otel/collector/ ./collector-config/    # to a folder

# 3. Get a real shell that shares the collector's network + PID namespaces,
#    then browse its filesystem via /proc/1/root
podman run --rm -it \
  --pid=container:splunk-otel-collector \
  --network=container:splunk-otel-collector \
  docker.io/nicolaka/netshoot
#   inside: ls -l /proc/1/root/etc/otel/collector/ ; curl -s localhost:13133 ; ss -ltnp

# 4. Logs and metadata from the host (no exec)
podman logs -f splunk-otel-collector      # or: ./run-collector.sh logs
podman inspect splunk-otel-collector
```

To SSH into the **Podman VM** itself (not the container) use `podman machine ssh`.
To get a shell *inside* the Collector — since Splunk publishes only distroless
images — build your own debug image by copying the binary and config from the
official image into a base with a shell (e.g. `FROM debian:bookworm-slim`), then
point `SPLUNK_COLLECTOR_IMAGE` to your local image in `.env`.

### Starting the services manually

If you prefer to run each piece yourself:

0. **Create the shared network** (once):

   ```bash
   podman network create petclinic-net
   ```

1. **Start the Kafka broker** (KRaft mode, no Zookeeper):

   ```bash
   podman run -d --name petclinic-kafka \
     --network petclinic-net \
     -p 9092:9092 \
     -p 29092:29092 \
     -e KAFKA_NODE_ID=0 \
     -e KAFKA_PROCESS_ROLES=controller,broker \
     -e KAFKA_LISTENERS=PLAINTEXT://:9092,PLAINTEXT_HOST://:29092,CONTROLLER://:9093 \
     -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://petclinic-kafka:9092,PLAINTEXT_HOST://localhost:29092 \
     -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
     -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
     -e KAFKA_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 \
     -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
     -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
     docker.io/apache/kafka:3.9.0
   ```

   Wait for port `29092` to be open before starting the apps:
   ```bash
   until nc -z localhost 29092; do sleep 1; done
   ```
2. **Start the backend** (persistence + replier):

   ```bash
   mvn -f backend/pom.xml spring-boot:run
   ```
3. **Start the frontend** (UI + requestor):

   ```bash
   mvn -f frontend/pom.xml spring-boot:run
   ```
4. Open the PetClinic UI at [http://localhost:8080/](http://localhost:8080/).

Both apps read their broker coordinates from `spring.kafka.*` properties in their
`application.properties`:

| Property                              | Default            |
| ------------------------------------- | ------------------ |
| `spring.kafka.bootstrap-servers`    | `localhost:29092` |
| `spring.kafka.consumer.group-id`    | `petclinic-backend` (backend only) |
| `kafka.request.timeout-ms`          | `10000`           |

Override them with environment variables or `--spring.kafka.bootstrap-servers=...`
when pointing at a different broker.

### Observability defaults in `run-otel.sh`

When using `./run-otel.sh`, the bundled Splunk OpenTelemetry Java agent applies these
defaults (override from environment):

| Variable                        | Default                                            | Purpose                                                |
| ------------------------------- | -------------------------------------------------- | ------------------------------------------------------ |
| `OTEL_SERVICE_NAME`           | _(per-app: kafka-backend/kafka-frontend)_        | Service name reported to Splunk APM.                   |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318`                          | Collector HTTP endpoint (OTLP/HTTP, not direct cloud). |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf`                                  | OTLP protocol.                                         |
| `OTEL_LOGS_EXPORTER`          | `none`                                           | Disable agent log export (requires Log Observer).      |
| `OTEL_RESOURCE_ATTRIBUTES`    | `deployment.environment=lab,service.version=1.0` | Resource metadata.                                     |
| `OTEL_ENABLED`                | `true`                                           | Set to `false` to run jars without the agent.         |

## Viewing logs

When `run-all.sh` runs apps in the background, each writes to its own file under
[`logs/`](logs). Tail them to watch what a service is doing:

```bash
# frontend (UI + Kafka requestor)
tail -f logs/frontend.log

# backend (persistence + Kafka replier)
tail -f logs/backend.log
```

If you started an app in the **foreground** (e.g. `./run-all.sh backend`), its log
is printed straight to the terminal instead of a file.

The **Kafka broker** logs come from the container:

```bash
# follow the broker's logs
podman logs -f petclinic-kafka

# last 200 lines only
podman logs --tail 200 petclinic-kafka
```

## Troubleshooting

### Collector logs a `404 Not Found` on `/v1/log` and drops data

**Symptom** — the Collector logs repeat an error like:

```
Exporting failed. Dropping data. ... "error": "Permanent error: \"HTTP/... 404 Not Found\"" ...
exporter: splunk_hec ... url: .../v1/log
```

**Cause** — the app's OTel Java agent is exporting **logs** to the Collector,
whose `splunk_hec` exporter POSTs them to `.../v1/log`. Splunk Observability Cloud
only accepts that endpoint when the org has **Log Observer** provisioned (a valid
`SPLUNK_HEC_URL` + HEC token). Without it, every log batch 404s, is retried, and
then dropped. Traces and metrics are unaffected.

**Fix** — `run-otel.sh` disables agent log export by default
(`OTEL_LOGS_EXPORTER=none`, passed to the agent as `-Dotel.logs.exporter=none`), so
no logs reach the Collector and there is nothing for `splunk_hec` to drop. The
change only takes effect on app **restart**; if the Collector is still retrying a
queued batch, restart it too with `./run-collector.sh restart`.

**To actually send logs** — provision Log Observer, wire a working
`SPLUNK_HEC_URL` / `SPLUNK_HEC_TOKEN` into the Collector's `splunk_hec` exporter,
then start the apps with log export turned back on:

```bash
OTEL_LOGS_EXPORTER=otlp ./run-otel.sh
```

Verify the errors are gone after restarting:

```bash
podman logs splunk-otel-collector 2>&1 | grep -aE 'splunk_hec|/v1/log|404|Dropping data'
```

### Kafka metrics: `connect: connection refused` during Collector startup

**Symptom** — the Collector logs show scrape errors on the `kafka_metrics` receiver
shortly after starting:

```
Failed to scrape ... dial tcp petclinic-kafka:9092: connect: connection refused
```

**Cause** — the `kafka_metrics` receiver starts scraping immediately and the Kafka
broker has not yet finished initialising.

**Fix** — [`otel-kafka-metrics.yaml`](otel-kafka-metrics.yaml) sets
`initial_delay: 45s` on the receiver. If you still see errors, increase this value.
The errors are transient and stop once the broker is accepting connections.

### Backend or frontend cannot connect to Kafka

**Symptom** — app fails to start or hangs with errors like:

```
org.apache.kafka.common.errors.TimeoutException: Topic not yet available
WARN o.a.k.c.NetworkClient - Connection to node -1 (/localhost:29092) could not be established
```

**Checks:**
1. Confirm Kafka is running: `podman ps | grep petclinic-kafka`
2. Confirm port `29092` is reachable from the host: `nc -z localhost 29092`
3. If the container exists but is stopped: `podman start petclinic-kafka`
4. Both apps default to `spring.kafka.bootstrap-servers=localhost:29092`. If you
   run the apps inside a container, change this to `petclinic-kafka:9092` (the
   container-network listener) and ensure the container is on `petclinic-net`.

### Cannot get a shell inside the OTel Collector container

The Collector image is **distroless** — there is no `/bin/sh`, `ls`, or `cat`.
`podman exec -it splunk-otel-collector /bin/sh` will fail. Use these instead:

```bash
# 1. Run the collector binary (the only executable in the image)
podman exec splunk-otel-collector /otelcol --version
podman exec splunk-otel-collector /otelcol components

# 2. Read files out of the container (no shell needed)
podman cp splunk-otel-collector:/etc/otel/collector/ ./collector-config/

# 3. Get a real shell that shares the collector's network + PID namespaces
podman run --rm -it \
  --pid=container:splunk-otel-collector \
  --network=container:splunk-otel-collector \
  docker.io/nicolaka/netshoot
#   inside: curl -s localhost:13133 ; ss -ltnp

# 4. Logs from the host (no exec needed)
podman logs -f splunk-otel-collector      # or: ./run-collector.sh logs
podman inspect splunk-otel-collector
```

## Building container images

There is no `Dockerfile`. Build an OCI image for each app with the Spring Boot
build plugin:

```bash
mvn -f backend/pom.xml spring-boot:build-image
mvn -f frontend/pom.xml spring-boot:build-image
```

Run the backend image (Kafka must already be running on `petclinic-net`):

```bash
podman run --network petclinic-net \
  -e SPRING_KAFKA_BOOTSTRAP_SERVERS=petclinic-kafka:9092 \
  -p 8081:8081 \
  docker.io/library/spring-petclinic-backend:4.0.0-SNAPSHOT
```

Run the frontend image similarly with `-e SPRING_KAFKA_BOOTSTRAP_SERVERS=petclinic-kafka:9092 -p 8080:8080`.

## License

The Spring PetClinic sample application is released under version 2.0 of the
[Apache License](https://www.apache.org/licenses/LICENSE-2.0). See
[LICENSE.txt](LICENSE.txt).
