# GoGuardLib live demo

A control room for showing what GoGuardLib does when a dependency dies. It generates HTTP and Kafka traffic, sends every order through four downstream APIs, lets you take any of them down, and shows every request moving through the system in real time.

```bash
cd demo
docker compose up --build
```

Then open **http://localhost:4280**. (Ports are published on localhost only; `BIND=0.0.0.0 docker compose up --build` shares it on your network, where anyone who can reach it can take services down.) (If that port is taken, `UI_PORT=5000 API_PORT=5001 docker compose up --build` picks others.) The first build downloads Go and npm dependencies and takes a few minutes.

## What you are looking at

Every order passes through **Inventory → Payments → Shipping → Notifications**, whether it arrives as an HTTP request or as a Kafka message. In front of each service sits a **circuit breaker** (the shield). All of that is the real library; the demo code only wires it up.

| On screen | What it is |
|---|---|
| Cyan dots | HTTP requests. The caller waits for the answer. |
| Violet dots | Kafka messages (the `orders` topic, three partitions, a guarded consumer with four workers). |
| Green burst | The order finished. |
| Purple bounce at a shield | The breaker is open, so the call was **skipped in about a millisecond** instead of waiting on a dead service. |
| Red burst at a service | The call failed (refused, timed out, error). |
| Amber dot flowing into the log | A Kafka message that could not finish, **stored on disk with a checkpoint** of the steps already done. |
| Teal dot leaving the log | The **redriver** replaying it, from the failed step, once the circuit closes. |

The commentary panel explains each moment in plain language, the breaker cards show failure rates, the chart shows outcomes per second, and the live request table shows every step of every request (click a row for details).

## The two-minute story

Press **✦ Guided demo** and let it run, or do it by hand:

1. **Start traffic.** Everything is green. Orders flow through all four services.
2. Press the red **Take down** button under **Payments**. Its port really closes.
3. Watch the failures climb for a few seconds, then the **Payments shield turns red**:
   - HTTP requests are now **rejected instantly** (purple) instead of hanging.
   - The **Kafka consumer pauses**. Messages wait safely in Kafka, not copied anywhere.
   - Orders already in flight are **stored in the durable dead-letter log**, each with a checkpoint (Inventory done, Payments blocked).
4. Press **Restore** on Payments. The shield goes **amber**: health checks passed, the next call is a canary.
5. The shield goes **green**. The redriver drains the log **from the Payments step** (Inventory is not repeated), ramping up gently so the recovering service is not flooded.
6. The commentary and counters confirm: nothing was lost.

Note: while orders are waiting in the log, new Kafka orders for the *same customer* are stored behind them (amber, "held in order") so they cannot overtake an earlier order. That is per-key ordering doing its job, and the backlog drains as the redriver catches up.

**Crash the consumer** (⚡) hard-kills the orchestrator process with no warning. Docker restarts it, the dead-letter log is still on its volume, and the ledger shows the stored orders survived and are redriven.

**Bad order** (☠) sends one order the Payments service will always refuse (an invalid card). Retrying can never help, so the pipeline **parks** it for a person: the *parked for a human* counter goes up, it is never retried and never deleted, and the other orders carry on. It is sent under its own Kafka key on purpose: a parked order holds back later orders with the *same* key until someone deals with it, which is the ordering guarantee working as designed.

Other things worth trying: **slow** (calls exceed the timeout), **flaky** (about half fail, so the breaker hovers), taking down **two** services, the **Storm** preset, and slow motion (0.5×).

## Is the data real?

Every number comes from the running system. The service counters come from the fake APIs, breaker figures from the library's `Stats()`, DLQ figures from the write-ahead log, and the animation from the same event stream. Nothing is scripted. Where a figure is an estimate it says so ("Failed-call time avoided" multiplies skipped calls by the measured time a failing call takes, and shows a dash until one has been measured). The **delivery ledger** follows every Kafka order from events alone and is cross-checked against what the write-ahead log itself reports; it turns amber if the two disagree or an order starts a step and never finishes. The dots are a sample (at most about 300 at once); the numbers are exact. If the event stream cannot keep up, the header says how many events were dropped.

## How it is built

```
 browser ──► nginx (ui) ──► orchestrator ──► services (4 fake APIs + control API)
   ▲   /api                  │  │  ▲
   └── server-sent events ◄──┘  │  └── Kafka (KRaft, 3 partitions)
                                └── write-ahead log on a volume (the DLQ)
```

- `backend/cmd/services` simulates Inventory, Payments, Shipping and Notifications. Each has `/work` and `/health`. Taking one down closes its listener.
- `backend/cmd/orchestrator` generates traffic and runs the flows with GoGuardLib: `breaker` (with `health` checks), `pipeline` (steps that resume where they stopped), `dlq` (the durable log and the redriver) and `kafka` (the guarded consumer). It streams events and snapshots to the UI over server-sent events.
- `backend/internal/simkafka` is an in-memory topic used only when `KAFKA_BROKERS` is unset (handy without Docker). The consumer is still the real one.
- `ui` is an Angular app. The diagram is SVG with a canvas overlay for the particles.

The HTTP flow calls each service through `breaker.Do`. In an application you would normally use the drop-in `goguard.New(...)` `http.RoundTripper` instead, which adds retries, a bulkhead and per-endpoint policies with no code changes to your handlers.

## Running without Docker

You need Go 1.25+ and Node 22+.

```bash
# 1. the simulated services
cd demo/backend && go run ./cmd/services

# 2. the orchestrator (uses the simulated topic; set KAFKA_BROKERS=host:9092 to use a real broker)
cd demo/backend && go run ./cmd/orchestrator

# 3. the UI, with /api proxied to the orchestrator
cd demo/ui && npm ci && npm start          # http://localhost:4200
```

## Control API

The UI uses these; you can too.

| | |
|---|---|
| `GET /api/stream` | Server-sent events: `e` (one event per step of every request) and `s` (a snapshot every 400 ms). |
| `GET /api/state` | The latest snapshot. |
| `POST /api/traffic` | `{"httpRps": 10, "kafkaRps": 10}` |
| `POST /api/services/{inventory,payments,shipping,notifications}/mode` | `{"mode": "up" \| "down" \| "slow" \| "flaky"}` |
| `POST /api/poison` | Produce one order that ends up parked (see **Bad order**). |
| `POST /api/crash` | Hard-kill the orchestrator; Docker restarts it and it recovers from the log. |
| `POST /api/reset` | Bring every service back up. |

## Cleaning up

```bash
docker compose down -v   # also removes the dead-letter log volume
```
