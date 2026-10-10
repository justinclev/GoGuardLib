import { ChangeDetectionStrategy, Component, computed, inject } from '@angular/core';
import { DemoStore } from '../../core/store';

const CW = 600;
const CH = 150;

@Component({
  selector: 'app-throughput-chart',
  templateUrl: './throughput-chart.html',
  styleUrl: './throughput-chart.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ThroughputChart {
  private readonly store = inject(DemoStore);
  readonly CW = CW;
  readonly CH = CH;

  /** One stacked column per second: succeeded, stored, rejected, failed. */
  readonly bars = computed(() => {
    const b = this.store.buckets();
    const max = Math.max(4, ...b.map((x) => x.ok + x.deferred + x.rejected + x.failed));
    const w = CW / 60;
    return b.map((x, i) => {
      const s = (CH - 10) / max;
      let y = CH;
      const seg = (n: number) => {
        const h = n * s;
        y -= h;
        return { y, h };
      };
      const ok = seg(x.ok);
      const deferred = seg(x.deferred);
      const rejected = seg(x.rejected);
      const failed = seg(x.failed);
      return { x: CW - (b.length - i) * w + 1, w: w - 2, ok, deferred, rejected, failed };
    });
  });

  readonly peak = computed(() => Math.max(0, ...this.store.buckets().map((x) => x.ok + x.deferred + x.rejected + x.failed)));
}
