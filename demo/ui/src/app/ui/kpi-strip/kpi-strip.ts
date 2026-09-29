import { ChangeDetectionStrategy, Component, computed, inject } from '@angular/core';
import { DemoStore } from '../../core/store';

@Component({
  selector: 'app-kpi-strip',
  templateUrl: './kpi-strip.html',
  styleUrl: './kpi-strip.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class KpiStrip {
  readonly store = inject(DemoStore);

  readonly throughput = computed(() => this.store.throughput().toFixed(1));
  readonly success = computed(() => {
    const r = this.store.successRate();
    return r === null ? '–' : r.toFixed(0);
  });
  readonly healthy = computed(() => (this.store.successRate() ?? 100) >= 90);
  readonly hasRate = computed(() => this.store.successRate() !== null);

  readonly stored = computed(() => {
    const d = this.store.snapshot()?.dlq;
    return d ? d.pending + d.leased + d.parked : 0;
  });
  readonly redriven = computed(() => this.store.snapshot()?.dlq.redriven ?? 0);

  readonly avoided = computed(() => {
    const s = this.store.avoidedSec();
    return this.store.failSamples() === 0 ? '–' : s < 10 ? s.toFixed(1) : Math.round(s).toString();
  });
  readonly avoidedCalls = computed(() => this.store.rejectedCalls());

  /** Measured latency of a call the breaker skipped; a dash until one has been seen. */
  readonly fastFail = computed(() => {
    if (this.store.rejectSamples() === 0) return '–';
    const ms = this.store.avgRejectMs();
    return ms < 1 ? '<1' : ms.toFixed(0);
  });
  readonly failedMs = computed(() => (this.store.failSamples() === 0 ? null : Math.round(this.store.avgFailMs())));
}
