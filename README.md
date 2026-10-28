# Transjakarta Sistem Manajemen Armada (Fleet Management)

A Go backend that receives vehicle locations over MQTT, stores them in PostgreSQL, serves them over a
REST API, and sends a RabbitMQ event when a vehicle enters a geofence. Everything runs with Docker Compose.

## 1. Run it

Prerequisite: Docker with Compose v2. Nothing else is needed to run the system.

```bash
docker compose up --build
```

That starts PostgreSQL, Mosquitto, RabbitMQ, the API, the worker and the mock publisher, and waits for each
to be healthy before starting the next. To feed the system by hand instead, run `docker compose stop publisher`
(section 2). Stop with `Ctrl+C`; `docker compose down -v` also deletes the data.

| What | Where |
|---|---|
| REST API | http://localhost:8080 |
| RabbitMQ management UI | http://localhost:15672 (user `fleet`, password `fleet`) |
| MQTT broker | `localhost:1883` (anonymous, demo only) |
| PostgreSQL | `localhost:5432` (user/password/db `fleet`) |

The infrastructure ports are bound to `127.0.0.1`, so the demo credentials are not reachable from the network;
only the API (8080) is. Upgrading from an earlier checkout? Run `docker compose down -v` once: the
`geofence_alerts` queue gained a dead-letter argument, which RabbitMQ will not apply to an existing queue.

## 2. Try it

There are two ways to feed the system, and they **deliberately use different vehicle ids**:

| Source | Vehicle ids | Timestamps |
|---|---|---|
| Mock publisher (runs by default) | `B2001TJ`, `B2002TJ` | current time |
| By hand, with the test document's example | `B1234XYZ` | `1715003456` (a 2024 time) |

Why: geofence state only moves forward in time, so a reading older than the newest one seen for that vehicle is
stored but, as late data, raises no event (section 6). If the publisher also used `B1234XYZ`, its current-time
readings would make the document's 2024 example "late", and it would never trigger an alert. Keeping the ids
apart means the document's example behaves as expected even while the publisher is running.

### Watch the demo vehicles

Two vehicles publish every 2 seconds.

```bash
curl localhost:8080/vehicles/B2001TJ/location
curl "localhost:8080/vehicles/B2001TJ/history?start=$(( $(date +%s) - 120 ))&end=$(date +%s)"
docker compose logs -f worker        # geofence alerts appear here
```

`B2001TJ` walks toward the geofence, enters it, leaves, and enters again (two events per ~32 s cycle).
`B2002TJ` circles at a distance and never enters.

### Send the test document's example by hand

```bash
docker compose exec mosquitto mosquitto_pub -q 1 -t /fleet/vehicle/B1234XYZ/location \
  -m '{"vehicle_id":"B1234XYZ","latitude":-6.2088,"longitude":106.8456,"timestamp":1715003456}'

curl localhost:8080/vehicles/B1234XYZ/location
curl "localhost:8080/vehicles/B1234XYZ/history?start=1715000000&end=1715009999"
docker compose logs worker           # shows a geofence alert for this reading
```

To send more readings for the same vehicle, give each a newer `timestamp` than the last; otherwise the older one
is stored but raises no event.

On Windows Git Bash, prefix the `docker compose exec` line with `MSYS_NO_PATHCONV=1`, otherwise the
leading `/` of the topic is rewritten into a Windows path.

A Postman collection is in [postman/fleet.postman_collection.json](postman/fleet.postman_collection.json);
its requests carry assertions, so "Run collection" doubles as a smoke test.

## 3. Architecture

```mermaid
flowchart LR
    publisher["cmd/publisher<br/>mock vehicles"]
    broker[("Mosquitto :1883")]
    client["curl / Postman"]

    subgraph api["cmd/api"]
        subscriber["MQTT subscriber<br/>decode and validate"]
        txn["One DB transaction:<br/>(1) insert location, deduplicated<br/>(2) lock geofence state<br/>(3) geofence.Evaluate<br/>(4) on entry, write outbox row"]
        relay["Outbox relay<br/>every 1 s, waits for confirm"]
        restapi["REST API :8080<br/>/vehicles/:id/location<br/>/vehicles/:id/history<br/>/healthz"]
        subscriber --> txn
    end

    db[("PostgreSQL<br/>vehicle_locations<br/>vehicle_geofence_state<br/>geofence_outbox")]
    exchange["RabbitMQ exchange<br/>fleet.events (direct)"]
    queue["Queue geofence_alerts"]
    dlq["Queue geofence_alerts.dlq<br/>(rejected messages)"]
    worker["cmd/worker<br/>manual ack, logs the alert"]

    publisher -->|"MQTT QoS 1"| broker
    broker -->|"/fleet/vehicle/+/location"| subscriber
    txn --> db
    db -->|"unpublished rows"| relay
    client --> restapi
    restapi -->|"reads"| db
    relay -->|"publish and confirm"| exchange
    exchange -->|"key geofence.entry"| queue
    queue --> worker
    queue -.->|"rejected"| dlq
```

Read it left to right: a reading is stored and evaluated in a single transaction, so the geofence event is saved
atomically with the state change. The relay then delivers it to RabbitMQ, which is why a RabbitMQ outage delays
events but never loses them (section 7).

One Go module, three binaries, one Docker image (the compose service picks the binary):

| Binary | Role |
|---|---|
| `api` | MQTT ingest, geofence evaluation, outbox relay, REST API (one process, one DB pool) |
| `worker` | Consumes `geofence_alerts` and logs each alert |
| `publisher` | Mock vehicles, 2-second interval |

## 4. REST API

### `GET /vehicles/{vehicle_id}/location`
Latest location (the reading with the greatest timestamp, not the last one to arrive).

```json
{"vehicle_id":"B1234XYZ","latitude":-6.2088,"longitude":106.8456,"timestamp":1715003456}
```

### `GET /vehicles/{vehicle_id}/history?start=1715000000&end=1715009999`
Readings with `start <= timestamp <= end` (Unix seconds, both inclusive), **oldest first**, as a bare JSON array.
The test document gives no response example for this endpoint; the array reuses the shape above.

```json
[{"vehicle_id":"B1234XYZ","latitude":-6.2088,"longitude":106.8456,"timestamp":1715003456}]
```

- No data in the range → `200` with `[]` (not 404).
- `start` and `end` are required integers, `0 <= start <= end`, and the range may not exceed
  `MAX_HISTORY_RANGE` (default 31 days) → otherwise `400`.
- At most 10,000 rows per response. If more matched, the oldest 10,000 are returned and the header
  `X-Result-Truncated: true` is set; narrow the range to get the rest.

### `GET /healthz`
`200` while the database answers, `503` when it does not. The body says what else is healthy:

```json
{"checks":{"database":"ok","mqtt":"connected","outbox_pending":0},"status":"ok"}
```

`status` is `degraded` (still `200`, because the REST API keeps working) when MQTT is disconnected or the
outbox cannot be read. `outbox_pending` is the number of geofence events waiting to reach RabbitMQ; a number
that keeps growing means RabbitMQ is unreachable.

### Errors
```json
{"error":{"code":"not_found","message":"no location found for this vehicle"}}
```
`404 not_found` (unknown vehicle or path), `400 invalid_request` (bad vehicle id or query), `500 internal`.

## 5. MQTT input

Topic `/fleet/vehicle/{vehicle_id}/location` (the leading `/` is part of the topic, as in the test document);
the service subscribes to `/fleet/vehicle/+/location` with QoS 1.

A message is stored only if all of these hold. Otherwise it is logged as a warning (topic and size, not the
payload) and dropped:

- at most 4096 bytes (a location is about 100; the broker refuses larger packets too);
- valid JSON object with `vehicle_id`, `latitude`, `longitude`, `timestamp` all present and of the right type
  (a `0` coordinate is valid, a missing one is not);
- `vehicle_id`: 1–32 characters from `A-Z a-z 0-9 _ -`, **and equal to the vehicle id in the topic**;
- `latitude` in [-90, 90], `longitude` in [-180, 180];
- `timestamp`: positive Unix seconds, at most `MAX_FUTURE_SKEW` (default 5 min) ahead of the server clock.
  Old timestamps are accepted: late data is still history.

## 6. Geofence and events

- **Fence**: a circle, default centre **-6.2088, 106.8456** (the example coordinate from the test document,
  a demo default and not an official Transjakarta location), radius **50 m**. Set with
  `GEOFENCE_CENTER_LAT`, `GEOFENCE_CENTER_LON`, `GEOFENCE_RADIUS_M`. Distance is great-circle (haversine);
  exactly 50 m counts as inside.
- **Entry rule**: an event is sent when a vehicle is inside and its previous state was outside. A vehicle
  with no previous state counts as outside, so **a first reading inside the fence is an entry**.
  Staying inside sends nothing; leaving sends nothing; entering again sends a new event.
- **Late data**: a reading older than the newest one already processed is stored (it is history) but does
  not change the geofence state, so a late message cannot fake an exit or an entry.
- **Message** (exactly the format in the test document; the body has no extra fields):

  ```json
  {"vehicle_id":"B1234XYZ","event":"geofence_entry","location":{"latitude":-6.2088,"longitude":106.8456},"timestamp":1715003456}
  ```

- **Broker layout**: durable direct exchange `fleet.events`, durable queue `geofence_alerts`, bound with routing
  key `geofence.entry`. Messages are persistent and carry the AMQP `MessageId` `{vehicle_id}:{timestamp}`.
  The spec names the exchange and queue only; the exchange type and routing key are our choice.
- **Worker**: consumes `geofence_alerts` with manual acknowledgements and prefetch 10, validates each event and
  logs it (`docker compose logs worker`). A malformed message is rejected without requeue, since it can never
  succeed, and is **dead-lettered** to the queue `geofence_alerts.dlq` (via the fanout exchange `fleet.events.dlx`)
  so it can be inspected instead of disappearing.

## 7. Reliability

| Situation | Behaviour |
|---|---|
| MQTT redelivers a message (QoS 1 is at-least-once) | `UNIQUE (vehicle_id, timestamp)` + `ON CONFLICT DO NOTHING`: no second row, no second event |
| API is down while vehicles publish | Persistent MQTT session (fixed client id, `clean_session=false`) + QoS 1: Mosquitto queues the messages and delivers them on restart |
| Two readings of one vehicle processed at once | The vehicle's state row is locked (`SELECT … FOR UPDATE`) inside the transaction: exactly one sees "outside → inside" |
| RabbitMQ is down when a vehicle enters | The event is saved in `geofence_outbox` in the same transaction as the state change, and the relay sends it when RabbitMQ returns. REST and ingest keep working |
| Queue or binding deleted | The broker returns the unroutable message, the relay treats it as a failure, reconnects (which re-declares the topology) and retries |
| API crashes after RabbitMQ confirmed but before the outbox row was marked | The event is sent again on restart with the same `MessageId`. Delivery is **at-least-once**; the worker's action (a log line) is harmless to repeat |
| PostgreSQL briefly unavailable during ingest | The handler retries 5 times (0.2 s doubling, about 3 s) and then logs an error and drops the message |
| A reading the database rejects for good (constraint or data error, SQLSTATE class 22/23) | Dropped once with a warning; retrying could never succeed |
| RabbitMQ stays down | The relay backs off (1 s, then doubling up to 30 s) instead of redialling every second, and goes back to 1 s after the first success. `outbox_pending` in `/healthz` shows the backlog |
| Outbox table growth | Published rows are deleted after `OUTBOX_RETENTION` (default 24 h); unpublished rows are never deleted |
| Worker loses its connection | Reconnects with exponential backoff (1 s up to 30 s) |
| Worker rejects a malformed message | Dead-lettered to `geofence_alerts.dlq` |

Shutdown is graceful (Uber Fx runs stop hooks in reverse start order: outbox relay → HTTP server → RabbitMQ
publisher → MQTT disconnect → ingest retries cancelled → database pool). Startup also fails fast, with one line
and exit code 1, on a malformed setting such as an unsupported `MQTT_BROKER_URL` scheme, a bad `RABBITMQ_URL` or
a non-positive duration.

## 8. Data model

[migrations/0001_init.up.sql](migrations/0001_init.up.sql), applied automatically when the API starts.

- `vehicle_locations(id, vehicle_id, latitude, longitude, timestamp, received_at)` with
  `UNIQUE (vehicle_id, timestamp)`, which also serves the "latest" and "history" queries, and range CHECKs.
- `vehicle_geofence_state(vehicle_id, inside, last_timestamp, updated_at)`
- `geofence_outbox(id, message_id UNIQUE, payload TEXT, created_at, published_at)`.
  `payload` is `TEXT` rather than `JSONB` because JSONB reorders keys and the body must keep the spec's order.

The test document names the table **`vehicle_loctions`**. We read that as a typo and created
**`vehicle_locations`**; a view called `vehicle_loctions` (the four spec fields) is also created, so a query
written with the document's spelling returns the same data.

## 9. Configuration

Environment variables. Defaults suit local development; `docker-compose.yml` sets the container values.
[.env.example](.env.example) lists them.

| Variable | Default | Used by |
|---|---|---|
| `DATABASE_URL` | – (required) | api |
| `RABBITMQ_URL` | – (required) | api, worker |
| `MQTT_BROKER_URL` | `tcp://localhost:1883` | api, publisher |
| `MQTT_CLIENT_ID` | `fleet-api` | api |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | empty (anonymous) | api, publisher. Kept out of the URL, and URLs are never logged with credentials |
| `HTTP_ADDR` | `:8080` | api |
| `LOG_LEVEL` | `info` (`debug` also logs every stored reading) | all |
| `GEOFENCE_CENTER_LAT` / `_LON` / `GEOFENCE_RADIUS_M` | -6.2088 / 106.8456 / 50 | api, publisher |
| `MAX_FUTURE_SKEW` | `5m` | api |
| `MAX_HISTORY_RANGE` | `744h` (31 days) | api |
| `OUTBOX_RETENTION` | `24h` | api |
| `PUBLISH_INTERVAL` | `2s` | publisher |
| `PUBLISHER_CLIENT_ID` | `fleet-publisher` | publisher |

## 10. Development

Prerequisites: Go 1.27+, Docker.

```bash
docker compose up -d postgres mosquitto rabbitmq      # infrastructure only

export DATABASE_URL='postgres://fleet:fleet@localhost:5432/fleet?sslmode=disable'
export RABBITMQ_URL='amqp://fleet:fleet@localhost:5672/'
go run ./cmd/api          # and, in other terminals:  go run ./cmd/worker   go run ./cmd/publisher

gofmt -l . && go vet ./...
go test ./...                                          # unit tests, no services needed
go test -tags integration ./...                        # also needs DATABASE_URL and RABBITMQ_URL (see below)
```

Integration tests use the Compose PostgreSQL and RabbitMQ. The relay tests create and drop their own scratch
database; the RabbitMQ tests declare uniquely named exchanges and queues; the others use uniquely named vehicles
and delete what they create. CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs everything with the
race detector.

```
cmd/{api,worker,publisher}   entry points (the fx.Provide list in api and worker IS the dependency graph)
internal/geofence            haversine, entry rule, event format (pure, no I/O)
internal/tracking            location model + validation, SQL, service, REST handlers
internal/ingest              MQTT message → tracking.Service
internal/outbox              outbox relay → RabbitMQ
internal/alert               what the worker does with an event
internal/simulator           deterministic routes for the mock publisher
internal/platform/{postgres,mqttclient,rabbitmq}   connections, migrations, topology
internal/httpserver          Fiber app, JSON errors, /healthz
migrations/  deploy/mosquitto/  postman/
```