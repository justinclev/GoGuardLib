import { Injectable, inject, signal } from '@angular/core';
import { ApiService } from './api.service';

interface Step {
  title: string;
  caption: string;
  seconds: number;
  run: () => Promise<void>;
}

/**
 * A scripted run for presenting: healthy traffic, an outage, and a recovery, with a
 * caption for each stage. Everything it does the presenter can do by hand.
 */
@Injectable({ providedIn: 'root' })
export class ScenarioService {
  private readonly api = inject(ApiService);

  readonly running = signal(false);
  readonly title = signal('');
  readonly caption = signal('');
  readonly progress = signal(0); // 0..1 through the current stage
  readonly stage = signal(0);
  readonly stages = signal(0);

  private token = 0;

  private script(): Step[] {
    return [
      {
        title: 'Everything healthy',
        caption:
          'Orders enter over HTTP and Kafka and pass through Inventory, Payments, Shipping and Notifications. Every call is guarded by a circuit breaker.',
        seconds: 9,
        run: async () => {
          await this.api.restoreAll();
          await this.api.setTraffic({ httpRps: 8, kafkaRps: 8 });
        },
      },
      {
        title: 'Payments goes down',
        caption: 'The Payments API stops answering. Calls fail until the breaker sees enough failures to open.',
        seconds: 9,
        run: () => this.api.setMode('payments', 'down'),
      },
      {
        title: 'The system protects itself',
        caption:
          'HTTP requests are rejected instantly instead of hanging. The Kafka consumer pauses and stores in-flight orders, with their progress, in the durable dead-letter log. Nothing is lost.',
        seconds: 14,
        run: async () => undefined,
      },
      {
        title: 'Payments comes back',
        caption:
          'Health checks notice the recovery. A canary call confirms it, the circuit closes, and the redriver replays the stored orders from the Payments step, ramping up gently.',
        seconds: 16,
        run: () => this.api.setMode('payments', 'up'),
      },
      {
        title: 'Back to normal',
        caption: 'The dead-letter log drains and traffic is healthy again. No order was lost or processed twice from the start.',
        seconds: 8,
        run: async () => undefined,
      },
    ];
  }

  async start(): Promise<void> {
    const mine = ++this.token;
    const steps = this.script();
    this.running.set(true);
    this.stages.set(steps.length);
    try {
      for (let i = 0; i < steps.length; i++) {
        const s = steps[i];
        if (mine !== this.token) return;
        this.stage.set(i + 1);
        this.title.set(s.title);
        this.caption.set(s.caption);
        this.progress.set(0);
        await s.run();
        const began = Date.now();
        while (Date.now() - began < s.seconds * 1000) {
          if (mine !== this.token) return;
          this.progress.set((Date.now() - began) / (s.seconds * 1000));
          await new Promise((r) => setTimeout(r, 100));
        }
      }
    } finally {
      if (mine === this.token) {
        this.running.set(false);
        this.progress.set(0);
      }
    }
  }

  stop(): void {
    this.token++;
    this.running.set(false);
    this.progress.set(0);
  }
}
