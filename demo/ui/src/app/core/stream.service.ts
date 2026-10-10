import { Injectable, inject } from '@angular/core';
import { Snapshot, StreamEvent } from './models';
import { DemoStore } from './store';

/** Connects to the orchestrator's server-sent events and feeds the store. */
@Injectable({ providedIn: 'root' })
export class StreamService {
  private readonly store = inject(DemoStore);
  private source?: EventSource;

  connect(url = '/api/stream'): void {
    if (this.source) return;
    const es = new EventSource(url);
    this.source = es;
    es.onopen = () => this.store.noteConnected();
    es.onerror = () => this.store.noteDisconnected(); // EventSource reconnects by itself
    es.addEventListener('e', (m) => this.store.handle(JSON.parse((m as MessageEvent<string>).data) as StreamEvent));
    es.addEventListener('s', (m) => {
      this.store.noteConnected();
      this.store.setSnapshot(JSON.parse((m as MessageEvent<string>).data) as Snapshot);
    });
  }
}
