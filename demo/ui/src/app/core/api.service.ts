import { Injectable } from '@angular/core';
import { ServiceMode, StepName } from './models';

/** Thin wrapper over the orchestrator's control API. Same origin: nginx (or the dev proxy) forwards /api. */
@Injectable({ providedIn: 'root' })
export class ApiService {
  private async post(path: string, body?: unknown): Promise<void> {
    const res = await fetch(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'goguard-demo' },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`${path} failed with ${res.status}`);
  }

  setTraffic(t: { httpRps?: number; kafkaRps?: number }): Promise<void> {
    return this.post('/api/traffic', t);
  }

  setMode(service: StepName, mode: ServiceMode): Promise<void> {
    return this.post(`/api/services/${service}/mode`, { mode });
  }

  /** Hard-kills the orchestrator (no graceful shutdown). The container restarts and recovers from disk. */
  crash(): Promise<void> {
    return this.post('/api/crash');
  }

  /** Sends one order that the payments service will always refuse, so it ends up parked for a person. */
  sendBadOrder(): Promise<void> {
    return this.post('/api/poison');
  }

  restoreAll(): Promise<void> {
    return this.post('/api/reset');
  }
}
