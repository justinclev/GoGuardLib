import { COLORS, NodeKey, NODES, Pt, TopicNode, gateOf, pos, svcOf, topicNode, H, W } from './layout';
import { StreamEvent, isStep } from './models';

/**
 * Turns the stream of events into particles moving through the diagram. It draws on
 * a canvas laid over the SVG and owns nothing but pictures: every number on screen
 * comes from the orchestrator, never from what the animation happens to show.
 */

type Action =
  | { k: 'go'; to: NodeKey | Pt; dur: number; curve?: number }
  | { k: 'color'; color: string }
  | { k: 'pulse' }
  | { k: 'burst'; color: string; n: number }
  | { k: 'fade'; dur: number }
  | { k: 'hold'; dur: number }
  | { k: 'home'; home: 'topic' | 'dlq' | null };

interface Running {
  a: Action;
  t0: number;
  from: Pt;
  to: Pt;
  ctrl: Pt;
  dur: number;
}

class Particle {
  x: number;
  y: number;
  alpha = 1;
  r = 4.6;
  bump = 0;
  color: string;
  trail: Pt[] = [];
  queue: Action[] = [];
  cur?: Running;
  home: 'topic' | 'dlq' | null = null;
  /** The topic node this message waits in (orders or refunds). */
  tn: TopicNode = 'topic';
  slot: Pt;
  angle = Math.random() * Math.PI * 2;
  target: NodeKey | null = null;
  dead = false;
  born = 0;

  constructor(
    public id: string,
    at: Pt,
    color: string,
    now: number,
  ) {
    this.x = at.x;
    this.y = at.y;
    this.color = color;
    this.born = now;
    this.slot = { x: (Math.random() - 0.5) * 2, y: (Math.random() - 0.5) * 2 };
  }
}

interface Spark {
  x: number;
  y: number;
  vx: number;
  vy: number;
  life: number;
  max: number;
  color: string;
  size: number;
}

interface Ripple {
  x: number;
  y: number;
  t0: number;
  dur: number;
  color: string;
  maxR: number;
}

const MAX_PARTICLES = 320;
const MAX_WAIT_TOPIC = 44;
const MAX_WAIT_DLQ = 70;

const easeInOut = (u: number) => (u < 0.5 ? 4 * u * u * u : 1 - Math.pow(-2 * u + 2, 3) / 2);

export class FlowEngine {
  /** Playback speed: 0.5 is slow motion, 2 is fast. */
  speed = 1;

  private ps = new Map<string, Particle>();
  private sparks: Spark[] = [];
  private ripples: Ripple[] = [];
  private last = 0;

  get particleCount(): number {
    return this.ps.size;
  }

  // ---- events in ----

  handle(ev: StreamEvent, now = performance.now()): void {
    switch (ev.type) {
      case 'request':
        this.onRequest(ev, now);
        break;
      case 'step':
        this.onStep(ev, now);
        break;
      case 'dlq':
        this.onDlq(ev, now);
        break;
      case 'probe':
        if (isStep(ev.service)) {
          const g = pos(gateOf(ev.service));
          this.ripples.push({
            x: g.x,
            y: g.y + 44,
            t0: now,
            dur: 900,
            color: ev.ok ? COLORS.ok : COLORS.failed,
            maxR: 26,
          });
        }
        break;
      case 'breaker':
        if (isStep(ev.service)) {
          const g = pos(gateOf(ev.service));
          const color = ev.to === 'open' ? COLORS.failed : ev.to === 'closed' ? COLORS.ok : COLORS.deferred;
          this.ripples.push({ x: g.x, y: g.y, t0: now, dur: 1400, color, maxR: 120 });
          this.ripples.push({ x: g.x, y: g.y, t0: now + 180, dur: 1400, color, maxR: 80 });
        }
        break;
    }
  }

  private waiting(home: 'topic' | 'dlq'): number {
    let n = 0;
    for (const p of this.ps.values()) if (p.home === home) n++;
    return n;
  }

  private spawn(id: string, at: NodeKey | Pt, color: string, now: number): Particle | undefined {
    if (this.ps.size >= MAX_PARTICLES) return undefined;
    const p = new Particle(id, typeof at === 'string' ? { ...pos(at) } : at, color, now);
    this.ps.set(id, p);
    return p;
  }

  private go(p: Particle, to: NodeKey, dur = 380, curve = 0.14): void {
    if (p.target === to) return;
    p.target = to;
    p.queue.push({ k: 'go', to, dur, curve });
  }

  private onRequest(ev: StreamEvent, now: number): void {
    const id = ev.id;
    if (!id) return;
    if (ev.status === 'new') {
      if (ev.flow === 'http') {
        const p = this.spawn(id, 'http', COLORS.http, now);
        if (p) this.go(p, 'gateway', 420);
      } else if (this.waiting('topic') < MAX_WAIT_TOPIC) {
        const p = this.spawn(id, 'producer', COLORS.kafka, now);
        if (p) {
          p.tn = topicNode(ev.topic);
          this.go(p, p.tn, 460);
          p.queue.push({ k: 'home', home: 'topic' });
        }
      }
      return;
    }
    const p = this.ps.get(id);
    if (!p) return;
    if (ev.status === 'success') {
      p.queue.push({ k: 'color', color: COLORS.ok }, { k: 'pulse' }, { k: 'burst', color: COLORS.ok, n: 10 }, { k: 'fade', dur: 420 });
    } else if (ev.flow === 'http' && ev.status === 'deferred') {
      // Saved for later: the dead-letter event already sent it to the log, where it waits
      // until the redriver sends it. Nothing to add here.
    } else if (ev.flow === 'http') {
      // failed or rejected: the step event already drew the impact
      p.queue.push({ k: 'hold', dur: 260 }, { k: 'fade', dur: 420 });
    }
  }

  /** A kafka message that is not on screen yet (for instance because the topic was crowded). */
  private ensureKafka(ev: StreamEvent, now: number): Particle | undefined {
    const id = ev.id;
    if (!id) return undefined;
    let p = this.ps.get(id);
    if (!p && ev.flow === 'kafka') {
      p = this.spawn(id, ev.redriven ? 'dlq' : topicNode(ev.topic), ev.redriven ? COLORS.redriven : COLORS.kafka, now);
      if (p) p.tn = topicNode(ev.topic);
      if (p && !ev.redriven) p.home = 'topic';
    }
    return p;
  }

  private leaveTopic(p: Particle): void {
    if (p.home === 'topic') {
      p.home = null;
      p.queue.push({ k: 'home', home: null });
      this.go(p, 'consumer', 320);
    }
  }

  private onStep(ev: StreamEvent, now: number): void {
    if (!isStep(ev.step)) return;
    const s = ev.step;
    const p = ev.flow === 'kafka' ? this.ensureKafka(ev, now) : ev.id ? this.ps.get(ev.id) : undefined;
    if (!p) return;
    switch (ev.status) {
      case 'started':
        if (ev.flow === 'kafka' && !ev.redriven) this.leaveTopic(p);
        this.go(p, gateOf(s), 330);
        break;
      case 'ok':
        this.go(p, gateOf(s), 260);
        this.go(p, svcOf(s), 260, 0);
        p.queue.push({ k: 'pulse' });
        break;
      case 'failed':
        this.go(p, gateOf(s), 260);
        this.go(p, svcOf(s), 260, 0);
        p.queue.push({ k: 'color', color: COLORS.failed }, { k: 'pulse' }, { k: 'burst', color: COLORS.failed, n: 16 });
        break;
      case 'rejected': {
        this.go(p, gateOf(s), 260);
        const g = pos(gateOf(s));
        p.queue.push(
          { k: 'color', color: COLORS.rejected },
          { k: 'pulse' },
          { k: 'burst', color: COLORS.rejected, n: 14 },
          { k: 'go', to: { x: g.x - 46, y: g.y - 26 }, dur: 260, curve: -0.3 },
        );
        p.target = null;
        break;
      }
    }
  }

  private toDlq(p: Particle): void {
    p.queue.push({ k: 'color', color: COLORS.deferred });
    p.target = null;
    this.go(p, 'dlq', 620, -0.2);
    if (this.waiting('dlq') >= MAX_WAIT_DLQ) {
      p.queue.push({ k: 'fade', dur: 260 });
    } else {
      p.queue.push({ k: 'home', home: 'dlq' });
    }
  }

  private onDlq(ev: StreamEvent, now: number): void {
    const p = this.ensureKafka(ev, now) ?? (ev.id ? this.ps.get(ev.id) : undefined);
    if (!p) return;
    switch (ev.status) {
      case 'stored':
        if (ev.cause === 'rejected' && isStep(ev.step)) {
          this.leaveTopic(p);
          this.go(p, gateOf(ev.step), 330);
          p.queue.push({ k: 'color', color: COLORS.rejected }, { k: 'pulse' }, { k: 'burst', color: COLORS.rejected, n: 10 });
        } else if (ev.cause === 'held') {
          this.leaveTopic(p);
        } else {
          this.leaveTopic(p);
        }
        this.toDlq(p);
        break;
      case 'redrive':
        p.home = null;
        p.queue.length = 0;
        p.cur = undefined;
        if (Math.hypot(p.x - NODES.dlq.x, p.y - NODES.dlq.y) > 90) {
          p.x = NODES.dlq.x + p.slot.x * 40;
          p.y = NODES.dlq.y + p.slot.y * 20;
        }
        p.target = null;
        p.queue.push({ k: 'color', color: COLORS.redriven }, { k: 'pulse' });
        this.go(p, isStep(ev.step) ? gateOf(ev.step) : gateOf('inventory'), 700, 0.22);
        break;
      case 'requeued':
        this.toDlq(p);
        break;
      case 'parked':
        p.queue.push({ k: 'color', color: COLORS.parked }, { k: 'burst', color: COLORS.parked, n: 8 }, { k: 'fade', dur: 500 });
        break;
    }
  }

  // ---- frame ----

  private resolve(t: NodeKey | Pt): Pt {
    return typeof t === 'string' ? pos(t) : t;
  }

  private begin(p: Particle, a: Action, now: number): Running {
    const from = { x: p.x, y: p.y };
    let to = from;
    let ctrl = from;
    let dur = 0;
    if (a.k === 'go') {
      to = this.resolve(a.to);
      // Spread particles a little so a busy stream reads as a stream, not a line.
      to = { x: to.x + p.slot.x * 5, y: to.y + p.slot.y * 5 };
      const mx = (from.x + to.x) / 2;
      const my = (from.y + to.y) / 2;
      const dx = to.x - from.x;
      const dy = to.y - from.y;
      const c = a.curve ?? 0;
      ctrl = { x: mx - dy * c, y: my + dx * c };
      dur = a.dur;
    } else if (a.k === 'fade' || a.k === 'hold') {
      dur = a.dur;
    }
    const compress = p.queue.length > 4 ? 0.4 : p.queue.length > 2 ? 0.7 : 1;
    return { a, t0: now, from, to, ctrl, dur: (dur * compress) / this.speed };
  }

  private burst(x: number, y: number, color: string, n: number): void {
    for (let i = 0; i < n; i++) {
      const ang = Math.random() * Math.PI * 2;
      const sp = 30 + Math.random() * 90;
      this.sparks.push({ x, y, vx: Math.cos(ang) * sp, vy: Math.sin(ang) * sp, life: 0, max: 380 + Math.random() * 380, color, size: 1 + Math.random() * 1.8 });
    }
  }

  private advance(p: Particle, now: number): void {
    for (let guard = 0; guard < 12; guard++) {
      if (!p.cur) {
        const a = p.queue.shift();
        if (!a) break;
        p.cur = this.begin(p, a, now);
        // instantaneous actions
        if (a.k === 'color') {
          p.color = a.color;
          p.cur = undefined;
          continue;
        }
        if (a.k === 'pulse') {
          p.bump = 1;
          p.cur = undefined;
          continue;
        }
        if (a.k === 'burst') {
          this.burst(p.x, p.y, a.color, a.n);
          p.cur = undefined;
          continue;
        }
        if (a.k === 'home') {
          p.home = a.home;
          p.cur = undefined;
          continue;
        }
      }
      const c = p.cur!;
      const u = c.dur <= 0 ? 1 : Math.min(1, (now - c.t0) / c.dur);
      if (c.a.k === 'go') {
        const e = easeInOut(u);
        const iu = 1 - e;
        p.x = iu * iu * c.from.x + 2 * iu * e * c.ctrl.x + e * e * c.to.x;
        p.y = iu * iu * c.from.y + 2 * iu * e * c.ctrl.y + e * e * c.to.y;
      } else if (c.a.k === 'fade') {
        p.alpha = 1 - u;
      }
      if (u >= 1) {
        if (c.a.k === 'fade') p.dead = true;
        p.cur = undefined;
        continue;
      }
      break;
    }
    if (!p.cur && p.queue.length === 0 && p.home) {
      // waiting in the topic or the dead-letter queue: drift gently
      p.angle += 0.01 * this.speed;
      const h = p.home === 'topic' ? { x: NODES[p.tn].x, y: NODES[p.tn].y + 4 } : { x: NODES.dlq.x + 34, y: NODES.dlq.y + 12 };
      const rx = p.home === 'topic' ? 34 : 58;
      const ry = p.home === 'topic' ? 12 : 18;
      const tx = h.x + p.slot.x * rx + Math.cos(p.angle) * 4;
      const ty = h.y + p.slot.y * ry + Math.sin(p.angle) * 4;
      p.x += (tx - p.x) * 0.08;
      p.y += (ty - p.y) * 0.08;
    }
    p.bump *= 0.9;
    // stale particles (an event we never saw the end of) do not live for ever
    if (!p.home && !p.cur && p.queue.length === 0 && now - p.born > 60_000) p.dead = true;
  }

  /** Advance and draw one frame. `w` and `h` are the canvas size in device pixels. */
  frame(now: number, ctx: CanvasRenderingContext2D, w: number, h: number): void {
    const dt = this.last ? Math.min(60, now - this.last) : 16;
    this.last = now;
    for (const p of this.ps.values()) {
      const px = p.x;
      const py = p.y;
      this.advance(p, now);
      if (Math.abs(px - p.x) + Math.abs(py - p.y) > 0.4) {
        p.trail.push({ x: px, y: py });
        if (p.trail.length > 9) p.trail.shift();
      } else if (p.trail.length) {
        p.trail.shift();
      }
      if (p.dead) this.ps.delete(p.id);
    }
    for (const s of this.sparks) {
      s.life += dt;
      s.x += (s.vx * dt) / 1000;
      s.y += (s.vy * dt) / 1000;
      s.vx *= 0.96;
      s.vy *= 0.96;
    }
    this.sparks = this.sparks.filter((s) => s.life < s.max);
    this.ripples = this.ripples.filter((r) => now - r.t0 < r.dur);

    ctx.clearRect(0, 0, w, h);
    ctx.save();
    ctx.scale(w / W, h / H);
    ctx.globalCompositeOperation = 'lighter';

    for (const r of this.ripples) {
      const u = Math.max(0, (now - r.t0) / r.dur);
      ctx.globalAlpha = (1 - u) * 0.7;
      ctx.strokeStyle = r.color;
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.arc(r.x, r.y, 6 + r.maxR * easeInOut(u), 0, Math.PI * 2);
      ctx.stroke();
    }

    for (const p of this.ps.values()) {
      for (let i = 0; i < p.trail.length; i++) {
        const t = p.trail[i];
        const k = (i + 1) / p.trail.length;
        ctx.globalAlpha = p.alpha * k * 0.32;
        ctx.fillStyle = p.color;
        ctx.beginPath();
        ctx.arc(t.x, t.y, p.r * k * 0.85, 0, Math.PI * 2);
        ctx.fill();
      }
      const r = p.r + p.bump * 4;
      const g = ctx.createRadialGradient(p.x, p.y, 0, p.x, p.y, r * 4.2);
      g.addColorStop(0, p.color);
      g.addColorStop(1, 'rgba(0,0,0,0)');
      ctx.globalAlpha = p.alpha * 0.4;
      ctx.fillStyle = g;
      ctx.beginPath();
      ctx.arc(p.x, p.y, r * 4.2, 0, Math.PI * 2);
      ctx.fill();
      ctx.globalAlpha = p.alpha;
      ctx.fillStyle = p.color;
      ctx.beginPath();
      ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
      ctx.fill();
      ctx.globalAlpha = p.alpha * 0.9;
      ctx.fillStyle = '#ffffff';
      ctx.beginPath();
      ctx.arc(p.x, p.y, r * 0.42, 0, Math.PI * 2);
      ctx.fill();
    }

    for (const s of this.sparks) {
      ctx.globalAlpha = Math.max(0, 1 - s.life / s.max);
      ctx.fillStyle = s.color;
      ctx.beginPath();
      ctx.arc(s.x, s.y, s.size, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.restore();
  }
}
