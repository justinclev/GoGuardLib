import { ChangeDetectionStrategy, Component, computed, inject } from '@angular/core';
import { STEP_LABEL, StepName, isStep } from '../../core/models';
import { DemoStore } from '../../core/store';

interface Chip {
  name: string;
  label: string;
  state: string;
}

/**
 * The Kafka topics the consumer reads. Each runs its own pipeline, so each has its own steps and its own
 * tallies, and each pauses when a breaker in its own pipeline is open.
 */
@Component({
  selector: 'app-topics-panel',
  templateUrl: './topics-panel.html',
  styleUrl: './topics-panel.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class TopicsPanel {
  private readonly store = inject(DemoStore);

  readonly topics = computed(() =>
    (this.store.snapshot()?.topics ?? []).map((t) => {
      const chips: Chip[] = t.steps.map((s) => ({
        name: s,
        label: isStep(s) ? STEP_LABEL[s] : s,
        state: this.store.breaker(s as StepName)?.state ?? 'closed',
      }));
      const open = chips.filter((c) => c.state === 'open').map((c) => c.label);
      return { ...t, chips, blockedBy: open.join(' and ') };
    }),
  );
}
