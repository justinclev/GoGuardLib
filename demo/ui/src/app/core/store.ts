import { Injectable, computed, signal } from '@angular/core';
import {
  BreakerSnapshot,
  RequestRecord,
  RequestStatus,
  STEP_LABEL,
  Snapshot,
  StepName,
  StoryItem,
  StreamEvent,
  emptySteps,
  isStep,
} from './models';

export interface Bucket {
  t: number;
  ok: number;
  failed: number;
  rejected: number;
  deferred: number;
}

const MAX_RECORDS = 500;
const TIMEOUT_MS = 1500; // the demo's per-call timeout (only quoted where a service is slow)

/**
 * Everything the panels show. Events arrive at traffic speed, so they mutate plain
 * objects and the signals that panels read are bumped a few times a second.
 */
@Injectable({ providedIn: 'root' })
export class DemoStore {
  readonly snapshot = signal<Snapshot | null>(null);
  readonly connected = signal(false);
  readonly story = signal<StoryItem[]>([]);
  readonly buckets = signal<Bucket[]>([]);
  /** Calls the breaker skipped. */
  readonly rejectedCalls = signal(0);
  /** Measured, not assumed: averages over real failed and rejected calls, and how many were seen. */
  readonly avgFailMs = signal(0);
  readonly avgRejectMs = signal(0);
  readonly failSamples = signal(0);
  readonly rejectSamples = signal(0);
  private readonly feedVersion = signal(0);

  private records = new Map<string, RequestRecord>();
  private recent: RequestRecord[] = [];
  private seq = 0;
  private storyId = 0;
  private listeners = new Set<(e: StreamEvent) => void>();
  private cur: Bucket = this.newBucket();
  private closed: Bucket[] = [];
  private throttle = new Map<string, number>();
  private setAside = new Map<string, number>();
  private redriving = new Map<string, number>();
  private prev: Snapshot | null = null;
  private failedSteps = new Set<string>();
  private crashed = false;
  private recoveredNote = false;
  private hadSnapshot = false;

  constructor() {
    setInterval(() => this.tick(), 1000);
    setInterval(() => this.flushAggregates(), 2500);
    setInterval(() => this.feedVersion.update((v) => v + 1), 250);
  }

  // ---- derived state ----

  /** Newest first. */
  readonly feed = computed(() => {
    this.feedVersion();
    return this.recent.slice(-160).reverse();
  });

  readonly throughput = computed(() => {
    const b = this.buckets().slice(-5);
    if (!b.length) return 0;
    return b.reduce((n, x) => n + x.ok + x.failed + x.rejected + x.deferred, 0) / b.length;
  });

  /** Null when nothing completed in the window: there is no rate to show. */
  readonly successRate = computed<number | null>(() => {
    const b = this.buckets().slice(-30);
    let ok = 0;
    let all = 0;
    for (const x of b) {
      ok += x.ok;
      all += x.ok + x.failed + x.rejected + x.deferred;
    }
    return all ? (ok / all) * 100 : null;
  });

  /**
   * Time the callers did not spend on calls that would have failed: skipped calls times
   * the measured average latency of calls that really failed. Zero until a failure has
   * been measured; an estimate, and labelled as one.
   */
  readonly avoidedSec = computed(() => (this.rejectedCalls() * this.avgFailMs()) / 1000);

  readonly downCount = computed(() => (this.snapshot()?.services ?? []).filter((s) => s.mode === 'down').length);

  breaker(name: StepName): BreakerSnapshot | undefined {
    return this.snapshot()?.breakers.find((b) => b.name === name);
  }

  // ---- input ----

  onEvent(cb: (e: StreamEvent) => void): () => void {
    this.listeners.add(cb);
    return () => this.listeners.delete(cb);
  }

  handle(ev: StreamEvent): void {
    for (const l of this.listeners) l(ev);
    switch (ev.type) {
      case 'request':
        this.onRequest(ev);
        break;
      case 'step':
        this.onStep(ev);
        break;
      case 'dlq':
        this.onDlq(ev);
        break;
      case 'breaker':
        this.onBreaker(ev);
        break;
    }
  }

  setSnapshot(s: Snapshot): void {
    this.diffSnapshot(this.prev, s);
    this.prev = s;
    this.snapshot.set(s);
    this.hadSnapshot = true;
    if (this.recoveredNote) {
      this.recoveredNote = false;
      const stored = s.dlq.pending + s.dlq.leased + s.dlq.parked;
      this.say(
        '🔁',
        'good',
        'Back online: recovered from disk',
        stored > 0
          ? `The process was killed with no warning, no flush and no graceful shutdown, yet ${stored} order${stored === 1 ? ' is' : 's are'} still in the dead-letter log on disk. The redriver is replaying them. Kafka offsets are committed only after an order is safe, so unfinished messages are read again. Traffic is paused after a restart: press Start traffic to continue.`
          : `The process restarted and recovered its state from disk. Nothing had been stored, so there was nothing to replay. Traffic is paused after a restart: press Start traffic to continue.`,
      );
    }
  }

  noteConnected(): void {
    if (this.crashed && this.hadSnapshot) {
      this.crashed = false;
      this.recoveredNote = true;
    }
    this.connected.set(true);
  }

  noteDisconnected(): void {
    if (this.connected() && !this.crashed) {
      this.crashed = true;
      this.say(
        '💀',
        'bad',
        'Connection lost: the orchestrator was killed',
        'A hard kill: no shutdown hooks, no final flush. Anything in memory is gone, so what matters now is what was already on disk.',
      );
    }
    this.connected.set(false);
  }

  // ---- requests ----

  private record(ev: StreamEvent): RequestRecord | undefined {
    if (!ev.id || !ev.flow) return undefined;
    let r = this.records.get(ev.id);
    if (!r) {
      r = {
        seq: ++this.seq,
        id: ev.id,
        flow: ev.flow,
        startedAt: ev.t,
        status: 'active',
        redriven: false,
        inDlq: false,
        steps: emptySteps(),
      };
      this.records.set(ev.id, r);
      this.recent.push(r);
      if (this.recent.length > MAX_RECORDS) {
        const old = this.recent.shift();
        if (old) this.records.delete(old.id);
      }
    }
    return r;
  }

  private finish(r: RequestRecord, status: RequestStatus, t: number, atStep?: string, detail?: string): void {
    r.status = status;
    r.endedAt = t;
    if (atStep) r.atStep = atStep;
    if (detail) r.detail = detail;
    if (status === 'success') this.cur.ok++;
    else if (status === 'failed') this.cur.failed++;
    else if (status === 'rejected') this.cur.rejected++;
    else if (status === 'deferred') this.cur.deferred++;
  }

  private onRequest(ev: StreamEvent): void {
    const r = this.record(ev);
    if (!r || ev.status === 'new') return;
    if (ev.redriven) r.redriven = true;
    if (ev.status === 'success') {
      r.inDlq = false;
      this.finish(r, 'success', ev.t);
    } else if (ev.status === 'failed' || ev.status === 'rejected') {
      this.finish(r, ev.status, ev.t, ev.step, ev.detail);
    } else if (ev.status === 'parked') {
      r.inDlq = false;
      r.status = 'parked';
      r.detail = ev.detail;
    }
  }

  private onStep(ev: StreamEvent): void {
    const r = this.record(ev);
    if (!r || !isStep(ev.step)) return;
    const info = r.steps[ev.step];
    if (ev.redriven) r.redriven = true;
    switch (ev.status) {
      case 'started':
        info.status = 'running';
        if (r.inDlq) {
          r.inDlq = false;
          r.status = 'active';
        }
        break;
      case 'ok':
        info.status = 'ok';
        info.latencyMs = ev.latencyMs;
        break;
      case 'failed':
        info.status = 'failed';
        info.latencyMs = ev.latencyMs;
        info.detail = ev.detail;
        this.ema('avgFailMs', 'failSamples', ev.latencyMs ?? 0);
        break;
      case 'rejected':
        info.status = 'rejected';
        info.latencyMs = ev.latencyMs;
        info.detail = ev.detail;
        this.ema('avgRejectMs', 'rejectSamples', ev.latencyMs ?? 0);
        this.rejectedCalls.update((v) => v + 1);
        break;
    }
  }

  private onDlq(ev: StreamEvent): void {
    const r = this.record(ev);
    if (!r) return;
    const step = isStep(ev.step) ? ev.step : undefined;
    switch (ev.status) {
      case 'stored': {
        r.inDlq = true;
        if (step) {
          const info = r.steps[step];
          if (ev.cause === 'rejected') {
            info.status = 'rejected';
            info.detail = ev.detail;
            this.rejectedCalls.update((v) => v + 1);
          }
          for (const s of Object.keys(r.steps) as StepName[]) {
            if (s !== step && r.steps[s].status === 'pending') r.steps[s].status = 'skipped';
          }
        }
        this.finish(r, 'deferred', ev.t, ev.step, ev.detail);
        const key = ev.cause === 'held' ? 'held' : (ev.step ?? '');
        this.setAside.set(key, (this.setAside.get(key) ?? 0) + 1);
        break;
      }
      case 'redrive':
        r.redriven = true;
        r.status = 'active';
        r.endedAt = undefined;
        for (const s of Object.keys(r.steps) as StepName[]) if (r.steps[s].status === 'skipped') r.steps[s].status = 'pending';
        if (step && r.steps[step].status !== 'ok') r.steps[step].status = 'pending';
        this.redriving.set(ev.step ?? '', (this.redriving.get(ev.step ?? '') ?? 0) + 1);
        break;
      case 'requeued':
        r.inDlq = true;
        r.status = 'deferred';
        break;
    }
  }

  private ema(which: 'avgFailMs' | 'avgRejectMs', count: 'failSamples' | 'rejectSamples', v: number): void {
    const first = this[count]() === 0;
    this[count].update((n) => n + 1);
    this[which].update((cur) => (first ? v : cur * 0.9 + v * 0.1));
  }

  // ---- time series ----

  private newBucket(): Bucket {
    return { t: Math.floor(Date.now() / 1000), ok: 0, failed: 0, rejected: 0, deferred: 0 };
  }

  private tick(): void {
    this.closed.push(this.cur);
    if (this.closed.length > 90) this.closed.shift();
    this.cur = this.newBucket();
    this.buckets.set(this.closed.slice(-60));
  }

  // ---- the story ----

  private say(icon: string, tone: StoryItem['tone'], title: string, body: string): void {
    const item: StoryItem = { id: ++this.storyId, t: Date.now(), icon, tone, title, body };
    this.story.update((s) => [item, ...s].slice(0, 40));
  }

  private once(key: string, ms: number): boolean {
    const now = Date.now();
    if (now - (this.throttle.get(key) ?? 0) < ms) return false;
    this.throttle.set(key, now);
    return true;
  }

  private onBreaker(ev: StreamEvent): void {
    if (!isStep(ev.service)) return;
    const name = STEP_LABEL[ev.service];
    const svc = ev.service;
    if (ev.to === 'open' && ev.from === 'closed') {
      this.failedSteps.add(svc);
      this.say(
        '⛔',
        'bad',
        `${name} circuit OPENED`,
        `Failures crossed the threshold, so the breaker stopped calling ${name}. New HTTP requests are rejected immediately instead of being attempted against a service that is failing, and Kafka messages are set aside with their progress.`,
      );
    } else if (ev.to === 'open') {
      this.say('⛔', 'bad', `${name} still failing`, `The canary call failed, so the circuit opened again. Health checks keep probing with backoff.`);
    } else if (ev.to === 'half-open') {
      this.say(
        '🩺',
        'warn',
        `${name} passed its health checks`,
        `Two healthy /health responses in a row. The circuit is half-open: the next real call is a canary before traffic is trusted again.`,
      );
    } else if (ev.to === 'closed') {
      this.failedSteps.delete(svc);
      this.say(
        '✅',
        'good',
        `${name} circuit CLOSED`,
        `The canary succeeded. The redriver now replays what was set aside, from the step that failed, at a gentle rate so the recovering service is not flooded.`,
      );
    }
  }

  private diffSnapshot(prev: Snapshot | null, cur: Snapshot): void {
    if (!prev) return;
    for (const s of cur.services) {
      const was = prev.services.find((x) => x.name === s.name)?.mode;
      if (!was || was === s.mode || was === 'unknown' || s.mode === 'unknown') continue;
      const name = STEP_LABEL[s.name];
      if (s.mode === 'down') {
        this.say('💥', 'accent', `You took down ${name}`, `Its port is closed, so callers see a real "connection refused". Watch the failure rate climb until the breaker opens.`);
      } else if (s.mode === 'up') {
        this.say('🔧', 'accent', `${name} is back online`, `The breaker does not take its word for it: health probes must pass twice, then a canary call, before the circuit closes.`);
      } else if (s.mode === 'slow') {
        this.say('🐢', 'accent', `${name} is now slow`, `Its responses now take longer than the demo's ${TIMEOUT_MS} ms call timeout, so calls time out and count as failures.`);
      } else if (s.mode === 'flaky') {
        this.say('🎲', 'accent', `${name} is now flaky`, `About half of its calls fail. Watch the failure rate hover around the threshold.`);
      }
    }
    if (cur.kafka.pausedPartitions > 0 && prev.kafka.pausedPartitions === 0) {
      this.say(
        '⏸',
        'warn',
        'Kafka consumer paused',
        `A circuit is open, so the consumer paused ${cur.kafka.pausedPartitions} partition(s). Messages stay in Kafka, which is a far bigger buffer than any local queue, and the consumer keeps polling so the group does not drop it.`,
      );
    } else if (cur.kafka.pausedPartitions === 0 && prev.kafka.pausedPartitions > 0) {
      this.say('▶️', 'good', 'Kafka consumer resumed', `The circuit is no longer open, so consumption continues where it stopped. Nothing was skipped.`);
    }
    if (cur.dlq.parked > prev.dlq.parked) {
      this.say('🧯', 'bad', 'Message parked for a human', `A message that can never succeed (for example an invalid card) was set aside for an operator instead of being retried. It is never deleted automatically, and the orders around it carry on.`);
    }
  }

  private fmtMs(v: number): string {
    return v < 1 ? 'under 1 ms' : `${Math.round(v)} ms`;
  }

  private flushAggregates(): void {
    if (this.setAside.size) {
      let total = 0;
      let held = 0;
      const where: string[] = [];
      for (const [k, n] of this.setAside) {
        if (k === 'held') held += n;
        else {
          total += n;
          if (isStep(k)) where.push(STEP_LABEL[k]);
        }
      }
      this.setAside.clear();
      if (total > 0) {
        this.say(
          '📥',
          'warn',
          `${total} order${total === 1 ? '' : 's'} set aside safely`,
          `Stored in the durable dead-letter log with a checkpoint of the steps already done${where.length ? ` (blocked at ${[...new Set(where)].join(', ')})` : ''}. The Kafka offset is committed only once this is on disk, so nothing is lost.`,
        );
      }
      if (held > 0) {
        this.say('🔗', 'info', `${held} order${held === 1 ? '' : 's'} held in order`, `Later orders for the same customer are stored behind the waiting one so they cannot overtake it.`);
      }
    }
    if (this.redriving.size) {
      let n = 0;
      const where: string[] = [];
      for (const [k, v] of this.redriving) {
        n += v;
        if (isStep(k)) where.push(STEP_LABEL[k]);
      }
      this.redriving.clear();
      this.say(
        '♻️',
        'good',
        `Redriving ${n} order${n === 1 ? '' : 's'}`,
        `Resuming at ${[...new Set(where)].join(', ') || 'the failed step'}: the steps that already succeeded are not repeated, and each call carries its idempotency key.`,
      );
    }
    const s = this.snapshot();
    if (s && this.downCount() > 0 && s.traffic.httpRps > 0 && this.rejectSamples() > 0 && this.failSamples() > 0 && this.once('fastfail', 12000)) {
      // Only with real measurements of both: a rejected call and a call that really failed.
      const rej = this.cur.rejected + (this.closed.at(-1)?.rejected ?? 0);
      if (rej > 0) {
        this.say(
          '⚡',
          'info',
          'Failing fast',
          `Measured so far: a call the breaker skips returns in ~${this.fmtMs(this.avgRejectMs())}, while a call that actually reached the failing service took ~${this.fmtMs(this.avgFailMs())} to fail. The skipped calls also put no load on the struggling service.`,
        );
      }
    }
  }
}
