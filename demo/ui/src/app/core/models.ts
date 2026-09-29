export type Flow = 'http' | 'kafka';

export const STEPS = ['inventory', 'payments', 'shipping', 'notifications'] as const;
export type StepName = (typeof STEPS)[number];

export const STEP_LABEL: Record<StepName, string> = {
  inventory: 'Inventory',
  payments: 'Payments',
  shipping: 'Shipping',
  notifications: 'Notifications',
};

export type ServiceMode = 'up' | 'down' | 'slow' | 'flaky' | 'unknown';

/** One thing that happened, as streamed by the orchestrator. */
export interface StreamEvent {
  t: number;
  type: 'request' | 'step' | 'breaker' | 'probe' | 'dlq';
  id?: string;
  flow?: Flow;
  step?: string;
  status?: string;
  latencyMs?: number;
  detail?: string;
  service?: string;
  from?: string;
  to?: string;
  ok?: boolean;
  cause?: string;
  redriven?: boolean;
}

export interface ServiceState {
  name: StepName;
  port: number;
  mode: ServiceMode;
  served: number;
  failed: number;
}

export type BreakerName = 'closed' | 'open' | 'half-open';

export interface BreakerSnapshot {
  name: StepName;
  state: BreakerName;
  failureRate: number;
  success: number;
  failure: number;
  inflight: number;
  rejected: number;
  opens: number;
  avgLatencyMs: number;
  lastChangedMs: number;
}

export interface Snapshot {
  t: number;
  backend: 'kafka' | 'sim';
  traffic: { httpRps: number; kafkaRps: number };
  services: ServiceState[];
  breakers: BreakerSnapshot[];
  dlq: { pending: number; leased: number; parked: number; oldestSec: number; redriven: number; blocked: number };
  kafka: {
    produced: number;
    received: number;
    done: number;
    deferred: number;
    held: number;
    parked: number;
    pausedPartitions: number;
    inFlight: number;
    backlog: number;
  };
  http: { sent: number; ok: number; failed: number; rejected: number };
  ledger: {
    produced: number;
    done: number;
    parked: number;
    waiting: number;
    inFlight: number;
    stored: number;
    stuck: number;
    onDisk: number;
    agrees: boolean;
  };
  droppedEvents: number;
}

export type StepStatus = 'pending' | 'running' | 'ok' | 'failed' | 'rejected' | 'skipped';

export interface StepInfo {
  status: StepStatus;
  latencyMs?: number;
  detail?: string;
  redriven?: boolean;
}

export type RequestStatus = 'active' | 'success' | 'failed' | 'rejected' | 'deferred' | 'parked';

export interface RequestRecord {
  seq: number;
  id: string;
  flow: Flow;
  startedAt: number;
  endedAt?: number;
  status: RequestStatus;
  atStep?: string;
  detail?: string;
  redriven: boolean;
  inDlq: boolean;
  steps: Record<StepName, StepInfo>;
}

export interface StoryItem {
  id: number;
  t: number;
  icon: string;
  tone: 'info' | 'good' | 'warn' | 'bad' | 'accent';
  title: string;
  body: string;
}

export function emptySteps(): Record<StepName, StepInfo> {
  return {
    inventory: { status: 'pending' },
    payments: { status: 'pending' },
    shipping: { status: 'pending' },
    notifications: { status: 'pending' },
  };
}

export function isStep(s: string | undefined): s is StepName {
  return !!s && (STEPS as readonly string[]).includes(s);
}
