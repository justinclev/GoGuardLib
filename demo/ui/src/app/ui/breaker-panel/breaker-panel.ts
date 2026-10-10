import { ChangeDetectionStrategy, Component, computed, inject } from '@angular/core';
import { STEP_LABEL } from '../../core/models';
import { DemoStore } from '../../core/store';

@Component({
  selector: 'app-breaker-panel',
  templateUrl: './breaker-panel.html',
  styleUrl: './breaker-panel.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class BreakerPanel {
  private readonly store = inject(DemoStore);

  readonly cards = computed(() => {
    const snap = this.store.snapshot();
    return (snap?.breakers ?? []).map((b) => {
      const svc = snap?.services.find((s) => s.name === b.name);
      const total = b.success + b.failure;
      return {
        ...b,
        label: STEP_LABEL[b.name],
        mode: svc?.mode ?? 'unknown',
        total,
        rate: Math.min(100, b.failureRate),
        note:
          b.state === 'open'
            ? 'Calls skipped. Probing /health with backoff.'
            : b.state === 'half-open'
              ? 'Health checks passed. Next call is the canary.'
              : total < 5
                ? 'Collecting samples (opens at 50% after 5 calls).'
                : 'Closed: calls flow normally.',
      };
    });
  });
}
