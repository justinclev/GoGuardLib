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
import { GATE_OFFSET, H, NODES, ROW_Y, SERVICE_X, W, pos, gateOf, topicNode } from '../../core/layout';
import { STEPS, STEP_LABEL, ServiceMode, StepName } from '../../core/models';
import { DemoStore } from '../../core/store';

const ICONS: Record<StepName, string> = {
  inventory: 'M3 7l9-4 9 4v10l-9 4-9-4V7z M3 7l9 4 9-4 M12 11v10',
  payments: 'M3 6h18a1 1 0 0 1 1 1v10a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1z M2 10h20 M6 15h4',
  shipping: 'M2 6h11v9H2z M13 9h4l3 3v3h-7z M5 17.5a1.6 1.6 0 1 0 .01 0 M16 17.5a1.6 1.6 0 1 0 .01 0',
  notifications: 'M6 16v-5a6 6 0 1 1 12 0v5l2 2H4l2-2z M10 20a2 2 0 0 0 4 0',
};

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
        // where the previous stage's line ends and the next begins
        prevX: i === 0 ? null : SERVICE_X[STEPS[i - 1]] + 54,
        left: (x / W) * 100,
        toolbarTop: ((ROW_Y + 74) / H) * 100,
      };
    });
  });

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
      return { name, node: NODES[topicNode(name)], produced: t?.produced ?? 0, paused: t?.paused ?? false };
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
