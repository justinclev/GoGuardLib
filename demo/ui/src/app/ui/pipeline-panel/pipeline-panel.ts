import { ChangeDetectionStrategy, Component, computed, inject } from '@angular/core';
import { DemoStore } from '../../core/store';

/** Kafka, the dead-letter log and HTTP, side by side: where every message is right now. */
@Component({
  selector: 'app-pipeline-panel',
  templateUrl: './pipeline-panel.html',
  styleUrl: './pipeline-panel.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class PipelinePanel {
  private readonly store = inject(DemoStore);

  readonly kafka = computed(() => this.store.snapshot()?.kafka);
  readonly dlq = computed(() => this.store.snapshot()?.dlq);
  readonly http = computed(() => this.store.snapshot()?.http);
  readonly ledger = computed(() => this.store.snapshot()?.ledger);
  readonly accounted = computed(() => {
    const l = this.ledger();
    return l ? l.done + l.parked + l.waiting + l.inFlight + l.stored : 0;
  });

  readonly oldest = computed(() => {
    const s = this.dlq()?.oldestSec ?? 0;
    if (s < 1) return '–';
    return s < 90 ? `${Math.round(s)} s` : `${Math.round(s / 60)} min`;
  });
}
