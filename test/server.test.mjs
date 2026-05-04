import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createApp } from "../src/server/server.mjs";

test("server exposes health and enqueue API", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "jobq-server-test-"));
  const app = await createApp({ dbPath: path.join(dir, "jobq.sqlite") });
  await app.listen({ host: "127.0.0.1", port: 0 });
  const { port } = app.server.address();

  try {
    const health = await getJson(`http://127.0.0.1:${port}/health`);
    assert.equal(health.status, "ok");

    const created = await postJson(`http://127.0.0.1:${port}/api/jobs`, {
      queue: "default",
      type: "email.digest",
      payload: { durationMs: 1 },
    });
    assert.equal(created.job.queue, "default");
    assert.equal(created.job.status, "queued");

    const metrics = await fetch(`http://127.0.0.1:${port}/metrics`).then((response) => response.text());
    assert.match(metrics, /jobq_jobs_total/);

    const toggled = await postJson(`http://127.0.0.1:${port}/api/queues/default/toggle`, {
      enabled: false,
    });
    assert.equal(toggled.queue.enabled, false);

    const openapi = await getJson(`http://127.0.0.1:${port}/openapi.json`);
    assert.equal(openapi.openapi, "3.0.3");

    const rateLimit = await postJson(`http://127.0.0.1:${port}/api/rate-limits`, {
      scope: "type",
      target: "webhook.deliver",
      limitCount: 5,
      windowMs: 60000,
    });
    assert.equal(rateLimit.rateLimit.target, "webhook.deliver");

    const autoscale = await getJson(`http://127.0.0.1:${port}/api/autoscale/recommendation`);
    assert.equal(typeof autoscale.desiredWorkers, "number");

    const snapshot = await getJson(`http://127.0.0.1:${port}/api/export`);
    assert.equal(Array.isArray(snapshot.tables.jobs), true);

    const demo = await postJson(`http://127.0.0.1:${port}/api/demo/seed`, {});
    assert.equal(demo.demo.jobs.length, 3);

    const failure = await postJson(`http://127.0.0.1:${port}/api/failures/inject`, {
      scenario: "timeout",
    });
    assert.equal(failure.scenario.scenario, "timeout");

    const idempotency = await getJson(`http://127.0.0.1:${port}/api/idempotency`);
    assert.equal(idempotency.savedDuplicateSubmissions >= 1, true);

    const workerPools = await getJson(`http://127.0.0.1:${port}/api/worker-pools`);
    assert.equal(Array.isArray(workerPools.workerPools), true);

    const throughput = await getJson(`http://127.0.0.1:${port}/api/charts/throughput`);
    assert.equal(Array.isArray(throughput.series), true);

    const maintenance = await postJson(`http://127.0.0.1:${port}/api/maintenance/prune-events`, {
      olderThanDays: 7,
      keepLast: 100,
    });
    assert.equal(typeof maintenance.result.pruned, "number");
  } finally {
    await app.close();
    await rm(dir, { recursive: true, force: true });
  }
});

async function getJson(url) {
  const response = await fetch(url);
  assert.equal(response.ok, true);
  return response.json();
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  assert.equal(response.ok, true);
  return response.json();
}
