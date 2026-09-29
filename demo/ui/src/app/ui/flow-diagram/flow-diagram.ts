import { UpperCasePipe } from '@angular/common';
import {
  AfterViewInit,
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  ElementRef,
  computed,
  effect,
  inject,
  input,
  viewChild,
} from '@angular/core';
import { ApiService } from '../../core/api.service';
import { FlowEngine } from '../../core/flow-engine';
import { COLORS, GATE_OFFSET, H, NODES, ROW_Y, SERVICE_X, STEP_INDEX, W, pos, gateOf, topicColor, topicNode } from '../../core/layout';
import { STEPS, STEP_LABEL, ServiceMode, StepName, isStep } from '../../core/models';
import { DemoStore } from '../../core/store';

const ICONS: Record<StepName, string> = {
  inventory: 'M3 7l9-4 9 4v10l-9 4-9-4V7z M3 7l9 4 9-4 M12 11v10',
  payments: 'M3 6h18a1 1 0 0 1 1 1v10a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1z M2 10h20 M6 15h4',
  shipping: 'M2 6h11v9H2z M13 9h4l3 3v3h-7z M5 17.5a1.6 1.6 0 1 0 .01 0 M16 17.5a1.6 1.6 0 1 0 .01 0',
  notifications: 'M6 16v-5a6 6 0 1 1 12 0v5l2 2H4l2-2z M10 20a2 2 0 0 0 4 0',
};

/** What the orchestrator runs, used until its first snapshot says otherwise. */
const DEFAULT_TOPICS = [
  { name: 'orders', steps: ['inventory', 'payments', 'shipping', 'notifications'] },
  { name: 'refunds', steps: ['payments', 'notifications'] },
];

@Component({
  selector: 'app-flow-diagram',
  templateUrl: './flow-diagram.html',
  styleUrl: './flow-diagram.scss',
  imports: [UpperCasePipe],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class FlowDiagram implements AfterViewInit {
  readonly store = inject(DemoStore);
  private readonly api = inject(ApiService);
  private readonly destroyRef = inject(DestroyRef);

  /** Playback speed for the animation: 0.5 is slow motion. */
  readonly speed = input(1);

  private readonly fx = viewChild.required<ElementRef<HTMLCanvasElement>>('fx');
  private readonly stage = viewChild.required<ElementRef<HTMLElement>>('stage');
  private readonly engine = new FlowEngine();

  readonly W = W;
  readonly H = H;
  readonly nodes = NODES;
  readonly rowY = ROW_Y;
  readonly viewBox = `0 0 ${W} ${H}`;

  constructor() {
    effect(() => {
      this.engine.speed = this.speed();
    });
  }

  readonly services = computed(() => {
    const snap = this.store.snapshot();
    return STEPS.map((name, i) => {
      const svc = snap?.services.find((s) => s.name === name);
      const br = snap?.breakers.find((b) => b.name === name);
      const x = SERVICE_X[name];
      const mode: ServiceMode = svc?.mode ?? 'unknown';
      return {
        name,
        label: STEP_LABEL[name],
        x,
        gx: x - GATE_OFFSET,
        icon: ICONS[name],
        mode,
        down: mode === 'down',
        served: svc?.served ?? 0,
        state: br?.state ?? 'closed',
        failureRate: br?.failureRate ?? 0,
        rejected: br?.rejected ?? 0,
        opens: br?.opens ?? 0,
        uses: this.usedBy(name),
        // where the previous stage's line ends and the next begins
        prevX: i === 0 ? null : SERVICE_X[STEPS[i - 1]] + 54,
        left: (x / W) * 100,
        toolbarTop: ((ROW_Y + 74) / H) * 100,
      };
    });
  });

  private topicDefs() {
    const t = this.store.snapshot()?.topics;
    return t?.length ? t.map((x) => ({ name: x.name, steps: x.steps })) : DEFAULT_TOPICS;
  }

  /** The colours of the flows that call a service: every topic whose pipeline has the step, and HTTP. */
  private usedBy(step: StepName): { color: string; label: string }[] {
    const kafka = this.topicDefs()
      .filter((t) => t.steps.includes(step))
      .map((t) => ({ color: topicColor(t.name), label: t.name }));
    return [...kafka, { color: COLORS.http, label: 'HTTP' }];
  }

  /**
   * Each topic's route through the services, drawn in its own colour where it differs from the plain
   * row: a lead-in from underneath when it does not start at the first service, and an arc over the
   * top wherever it skips services. The animation follows the same paths.
   */
  readonly routes = computed(() =>
    this.topicDefs().map((t) => {
      const idx = t.steps.filter(isStep).map((s) => STEP_INDEX[s]);
      const paths: string[] = [];
      if (idx.length && idx[0] > 0) {
        const g = pos(gateOf(STEPS[idx[0]]));
        paths.push(`M ${NODES.consumer.x + 72} ${NODES.consumer.y} C ${g.x - 140} ${NODES.consumer.y}, ${g.x - 4} ${ROW_Y + 150}, ${g.x} ${ROW_Y + 38}`);
      }
      for (let i = 1; i < idx.length; i++) {
        if (idx[i] - idx[i - 1] < 2) continue;
        const x0 = SERVICE_X[STEPS[idx[i - 1]]] + 18;
        const x1 = SERVICE_X[STEPS[idx[i]]] - GATE_OFFSET - 8;
        paths.push(`M ${x0} ${ROW_Y - 52} C ${x0 + 6} ${ROW_Y - 128}, ${x1 - 6} ${ROW_Y - 128}, ${x1} ${ROW_Y - 40}`);
      }
      const labels = t.steps.map((s) => (isStep(s) ? STEP_LABEL[s] : s));
      return { name: t.name, color: topicColor(t.name), paths, text: labels.join(' → '), count: labels.length };
    }),
  );

  readonly gatewayToFirst = this.curve(NODES.gateway, pos(gateOf('inventory')), 0.12, 46, 0);
  readonly consumerToFirst = this.curve(NODES.consumer, pos(gateOf('inventory')), -0.12, 46, 0);

  readonly dlqEdges = computed(() =>
    STEPS.map((s) => {
      const g = pos(gateOf(s));
      return { name: s, d: `M ${g.x} ${g.y + 30} C ${g.x} ${g.y + 130}, ${NODES.dlq.x + (g.x - NODES.dlq.x) * 0.25} ${NODES.dlq.y - 130}, ${NODES.dlq.x + (g.x - NODES.dlq.x) * 0.2} ${NODES.dlq.y - 50}` };
    }),
  );

  readonly dlqFill = computed(() => {
    const d = this.store.snapshot()?.dlq;
    if (!d) return 0;
    return Math.min(1, (d.pending + d.leased + d.parked) / 120);
  });

  /** The two topic nodes, with what the snapshot says about each. */
  readonly topicNodes = computed(() => {
    const snap = this.store.snapshot()?.topics ?? [];
    return (['orders', 'refunds'] as const).map((name) => {
      const t = snap.find((x) => x.name === name);
      return { name, node: NODES[topicNode(name)], color: topicColor(name), steps: t?.steps.length ?? (name === 'orders' ? 4 : 2), produced: t?.produced ?? 0, paused: t?.paused ?? false };
    });
  });

  /** Whether the HTTP client saves notifications it cannot send, and how many it has saved. */
  readonly deferOn = computed(() => this.store.snapshot()?.http.deferOn ?? true);
  readonly httpSaved = computed(() => this.store.snapshot()?.http.deferred ?? 0);

  toggleDefer(): void {
    void this.api.setDeferral(!this.deferOn());
  }

  private curve(a: { x: number; y: number }, b: { x: number; y: number }, bend: number, startOffset: number, endOffset: number): string {
    const dx = b.x - a.x;
    const dy = b.y - a.y;
    const mx = (a.x + b.x) / 2 - dy * bend;
    const my = (a.y + b.y) / 2 + dx * bend;
    return `M ${a.x + startOffset} ${a.y} Q ${mx} ${my} ${b.x - endOffset} ${b.y}`;
  }

  ngAfterViewInit(): void {
    const canvas = this.fx().nativeElement;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const resize = () => {
      const r = this.stage().nativeElement.getBoundingClientRect();
      const dpr = Math.min(2, window.devicePixelRatio || 1);
      canvas.width = Math.max(1, Math.round(r.width * dpr));
      canvas.height = Math.max(1, Math.round(r.height * dpr));
    };
    resize();
    const ro = new ResizeObserver(resize);
    ro.observe(this.stage().nativeElement);

    const off = this.store.onEvent((e) => this.engine.handle(e));

    let raf = 0;
    const loop = (now: number) => {
      this.engine.frame(now, ctx, canvas.width, canvas.height);
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);

    this.destroyRef.onDestroy(() => {
      cancelAnimationFrame(raf);
      ro.disconnect();
      off();
    });
  }

  /** The one button a presenter reaches for: kill it, or bring it back. */
  toggleDown(name: StepName, mode: ServiceMode): void {
    void this.api.setMode(name, mode === 'down' ? 'up' : 'down');
  }

  setMode(name: StepName, current: ServiceMode, mode: ServiceMode): void {
    void this.api.setMode(name, current === mode ? 'up' : mode);
  }
}
