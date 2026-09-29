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
  readonly success = computed(() => this.store.successRate().toFixed(0));
  readonly healthy = computed(() => this.store.successRate() >= 90);

  readonly stored = computed(() => {
    const d = this.store.snapshot()?.dlq;
    return d ? d.pending + d.leased + d.parked : 0;
  });
  readonly redriven = computed(() => this.store.snapshot()?.dlq.redriven ?? 0);

  readonly avoided = computed(() => Math.round(this.store.waitAvoidedSec()));
  readonly fastFail = computed(() => {
    const ms = this.store.avgRejectMs();
    return ms < 1 ? '<1' : ms.toFixed(0);
  });
}
