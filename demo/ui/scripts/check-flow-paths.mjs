// Checks the diagram's animation paths without a browser: it drives the real FlowEngine with a
// stand-in canvas and watches where particles go.
//
//   npm run check:paths
//
// A message must never be drawn passing through a service its pipeline does not call. Refunds run
// Payments then Notifications, so they must not cross the Inventory or Shipping boxes, which is
// where a stopped service would look as if it were still taking Kafka traffic.
import { build } from 'esbuild';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const dir = mkdtempSync(join(tmpdir(), 'flow-paths-'));
const out = join(dir, 'engine.mjs');
await build({ entryPoints: ['src/app/core/flow-engine.ts'], bundle: true, format: 'esm', outfile: out, logLevel: 'error' });
const { FlowEngine } = await import(pathToFileURL(out).href);

const ctx = new Proxy({}, { get: (t, k) => (k in t ? t[k] : () => ({ addColorStop() {} })), set: (t, k, v) => ((t[k] = v), true) });

// The service boxes on the diagram's row (see core/layout.ts): centre x, half-width 54, y 238..342.
const BOX = { inventory: [561, 669], shipping: [911, 1019] };
const inBox = (p, [x0, x1]) => p.x > x0 && p.x < x1 && p.y > 238 && p.y < 342;

function play(id, topic, steps, boxes) {
  const e = new FlowEngine();
  let now = 1000;
  const ev = (o) => e.handle({ t: now, flow: 'kafka', id, topic, ...o }, now);
  const run = (frames) => {
    let inside = 0;
    for (let i = 0; i < frames; i++) {
      now += 16;
      e.frame(now, ctx, 1200, 640);
      for (const p of e.ps.values()) if (boxes.some((b) => inBox(p, b))) inside++;
    }
    return inside;
  };
  ev({ type: 'request', status: 'new' });
  run(40);
  let inside = 0;
  for (const step of steps) {
    ev({ type: 'step', step, status: 'started' });
    inside += run(70);
    ev({ type: 'step', step, status: 'ok' });
    inside += run(40);
  }
  return inside;
}

let failed = false;
const expect = (name, got, want) => {
  const ok = want === 'none' ? got === 0 : got > 0;
  console.log(`${ok ? 'ok  ' : 'FAIL'} ${name}: ${got} samples inside the boxes`);
  if (!ok) failed = true;
};
expect('refunds skip Inventory and Shipping', play('R-1', 'refunds', ['payments', 'notifications'], [BOX.inventory, BOX.shipping]), 'none');
// The control: orders do call those services, so they must pass through the boxes (the check can fail).
expect('orders pass through every service', play('K-1', 'orders', ['inventory', 'payments', 'shipping', 'notifications'], [BOX.inventory, BOX.shipping]), 'some');
process.exit(failed ? 1 : 0);
