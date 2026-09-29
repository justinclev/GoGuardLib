import { ChangeDetectionStrategy, Component, computed, inject, model } from '@angular/core';
import { ApiService } from '../../core/api.service';
import { ScenarioService } from '../../core/scenario.service';
import { DemoStore } from '../../core/store';

@Component({
  selector: 'app-control-bar',
  templateUrl: './control-bar.html',
  styleUrl: './control-bar.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ControlBar {
  readonly store = inject(DemoStore);
  readonly scenario = inject(ScenarioService);
  private readonly api = inject(ApiService);

  /** Animation playback speed, shared with the diagram. */
  readonly speed = model(1);
  readonly speeds = [0.5, 1, 2];

  readonly http = computed(() => Math.round(this.store.snapshot()?.traffic.httpRps ?? 0));
  readonly kafka = computed(() => Math.round(this.store.snapshot()?.traffic.kafkaRps ?? 0));
  readonly running = computed(() => this.http() + this.kafka() > 0);
  readonly anyDown = computed(() => (this.store.snapshot()?.services ?? []).some((s) => s.mode !== 'up' && s.mode !== 'unknown'));

  private timers = new Map<string, ReturnType<typeof setTimeout>>();

  private send(which: 'httpRps' | 'kafkaRps', value: number): void {
    clearTimeout(this.timers.get(which));
    this.timers.set(
      which,
      setTimeout(() => void this.api.setTraffic({ [which]: value }), 90),
    );
  }

  onHttp(e: Event): void {
    this.send('httpRps', (e.target as HTMLInputElement).valueAsNumber);
  }

  onKafka(e: Event): void {
    this.send('kafkaRps', (e.target as HTMLInputElement).valueAsNumber);
  }

  toggleTraffic(): void {
    void (this.running() ? this.api.setTraffic({ httpRps: 0, kafkaRps: 0 }) : this.api.setTraffic({ httpRps: 8, kafkaRps: 8 }));
  }

  preset(http: number, kafka: number): void {
    void this.api.setTraffic({ httpRps: http, kafkaRps: kafka });
  }

  restoreAll(): void {
    void this.api.restoreAll();
  }

  toggleScenario(): void {
    if (this.scenario.running()) this.scenario.stop();
    else void this.scenario.start();
  }
}
