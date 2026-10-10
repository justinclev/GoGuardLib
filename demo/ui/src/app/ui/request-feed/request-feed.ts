import { DatePipe } from '@angular/common';
import { ChangeDetectionStrategy, Component, computed, inject, signal } from '@angular/core';
import { RequestRecord, STEPS, STEP_LABEL, StepName } from '../../core/models';
import { DemoStore } from '../../core/store';

type Filter = 'all' | 'success' | 'failed' | 'rejected' | 'deferred' | 'redriven';

@Component({
  selector: 'app-request-feed',
  imports: [DatePipe],
  templateUrl: './request-feed.html',
  styleUrl: './request-feed.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class RequestFeed {
  private readonly store = inject(DemoStore);

  readonly steps = STEPS;
  readonly label = STEP_LABEL;
  readonly filters: { key: Filter; label: string }[] = [
    { key: 'all', label: 'All' },
    { key: 'success', label: 'Succeeded' },
    { key: 'rejected', label: 'Rejected' },
    { key: 'failed', label: 'Failed' },
    { key: 'deferred', label: 'In DLQ' },
    { key: 'redriven', label: 'Redriven' },
  ];

  readonly filter = signal<Filter>('all');
  readonly open = signal<string | null>(null);

  readonly rows = computed(() => {
    const f = this.filter();
    return this.store
      .feed()
      .filter((r) => (f === 'all' ? true : f === 'redriven' ? r.redriven : r.status === f))
      .slice(0, 60);
  });

  toggle(id: string): void {
    this.open.update((cur) => (cur === id ? null : id));
  }

  ms(r: RequestRecord): string {
    if (!r.endedAt) return '…';
    const d = r.endedAt - r.startedAt;
    return d < 1000 ? `${d} ms` : `${(d / 1000).toFixed(1)} s`;
  }

  outcome(r: RequestRecord): string {
    switch (r.status) {
      case 'success':
        return r.redriven ? 'recovered' : 'succeeded';
      case 'rejected':
        return 'rejected';
      case 'failed':
        return 'failed';
      case 'deferred':
        return 'in DLQ';
      case 'parked':
        return 'parked';
      default:
        return r.redriven ? 'redriving' : 'running';
    }
  }

  tip(r: RequestRecord, s: StepName): string {
    const i = r.steps[s];
    const base = `${STEP_LABEL[s]}: ${i.status}`;
    const extra = [i.latencyMs !== undefined ? `${i.latencyMs} ms` : '', i.detail ?? ''].filter(Boolean).join(' · ');
    return extra ? `${base} (${extra})` : base;
  }
}
