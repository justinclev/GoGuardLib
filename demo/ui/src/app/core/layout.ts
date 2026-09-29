import { StepName, STEPS } from './models';

/** The diagram is drawn on a fixed canvas and scaled to fit; these are its coordinates. */
export const W = 1200;
export const H = 640;

export interface Pt {
  x: number;
  y: number;
}

export const ROW_Y = 290;

export const SERVICE_X: Record<StepName, number> = {
  inventory: 615,
  payments: 790,
  shipping: 965,
  notifications: 1135,
};

export const GATE_OFFSET = 84;

export const NODES = {
  http: { x: 78, y: 118 } as Pt,
  gateway: { x: 345, y: 118 } as Pt,
  producer: { x: 78, y: 512 } as Pt,
  topic: { x: 240, y: 512 } as Pt,
  consumer: { x: 415, y: 512 } as Pt,
  dlq: { x: 880, y: 540 } as Pt,
};

export type NodeKey = keyof typeof NODES | `gate:${StepName}` | `svc:${StepName}`;

export function pos(key: NodeKey): Pt {
  if (key.startsWith('gate:')) {
    const s = key.slice(5) as StepName;
    return { x: SERVICE_X[s] - GATE_OFFSET, y: ROW_Y };
  }
  if (key.startsWith('svc:')) {
    const s = key.slice(4) as StepName;
    return { x: SERVICE_X[s], y: ROW_Y };
  }
  return NODES[key as keyof typeof NODES];
}

export function gateOf(s: StepName): NodeKey {
  return `gate:${s}`;
}

export function svcOf(s: StepName): NodeKey {
  return `svc:${s}`;
}

export const STEP_INDEX: Record<StepName, number> = Object.fromEntries(STEPS.map((s, i) => [s, i])) as Record<
  StepName,
  number
>;

export const COLORS = {
  http: '#3ee6ff',
  kafka: '#8f86ff',
  ok: '#3ef0b0',
  failed: '#ff4d6d',
  rejected: '#c56bff',
  deferred: '#ffb84d',
  redriven: '#4dfff0',
  parked: '#8b93b8',
};
