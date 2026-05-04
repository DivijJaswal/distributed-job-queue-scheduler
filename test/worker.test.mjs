import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { JobQueueStore } from "../src/core/store.mjs";
import { JobWorker } from "../src/worker/workerRuntime.mjs";

test("worker tick claims and completes available jobs", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const job = store.enqueueJob({
      queue: "default",
      type: "email.digest",
      payload: { durationMs: 1 },
    });
    const worker = new JobWorker(store, {
      workerId: "worker-test",
      queues: ["default"],
      concurrency: 3,
      pollMs: 10,
      logger: silentLogger,
    });

    const claimed = await worker.tick();
    assert.equal(claimed.length, 1);
    await worker.active.get(job.id);

    const completed = store.findJob(job.id);
    assert.equal(completed.status, "completed");
    assert.equal(completed.output.action, "email_sent");
    assert.equal(store.findWorker("worker-test").concurrency, 3);
  } finally {
    await cleanup();
  }
});

test("worker tick moves permanent failures to dead letter after max attempts", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const job = store.enqueueJob({
      queue: "default",
      type: "webhook.deliver",
      maxAttempts: 1,
      payload: { failAlways: true, durationMs: 1 },
    });
    const worker = new JobWorker(store, {
      workerId: "worker-test",
      queues: ["default"],
      concurrency: 1,
      backoffBaseMs: 1,
      logger: silentLogger,
    });

    await worker.tick();
    await worker.active.get(job.id);

    assert.equal(store.findJob(job.id).status, "dead_letter");
  } finally {
    await cleanup();
  }
});

test("worker drain releases active leases after timeout without task rejection", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const job = store.enqueueJob({
      queue: "default",
      type: "email.digest",
      payload: { durationMs: 50 },
    });
    const worker = new JobWorker(store, {
      workerId: "worker-drain",
      queues: ["default"],
      concurrency: 1,
      logger: silentLogger,
    });

    await worker.tick();
    const activeTask = worker.active.get(job.id);
    await worker.stop({ timeoutMs: 1 });

    const released = store.findJob(job.id);
    assert.equal(released.status, "scheduled");
    assert.equal(released.leaseOwner, null);
    assert.equal(store.findWorker("worker-drain").status, "offline");
    await assert.doesNotReject(activeTask);
  } finally {
    await cleanup();
  }
});

test("worker applies job timeout during execution", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const job = store.enqueueJob({
      queue: "default",
      type: "email.digest",
      timeoutMs: 1,
      maxAttempts: 1,
      payload: { durationMs: 50 },
    });
    const worker = new JobWorker(store, {
      workerId: "worker-timeout",
      queues: ["default"],
      concurrency: 1,
      logger: silentLogger,
    });

    await worker.tick();
    await worker.active.get(job.id);

    const updated = store.findJob(job.id);
    assert.equal(updated.status, "dead_letter");
    assert.match(updated.lastError, /timed out/);
  } finally {
    await cleanup();
  }
});

test("worker passes capabilities to claim routing", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const job = store.enqueueJob({
      queue: "default",
      type: "report.generate",
      requiredCapabilities: ["report"],
      payload: { durationMs: 1 },
    });
    const worker = new JobWorker(store, {
      workerId: "worker-email",
      queues: ["default"],
      capabilities: ["email"],
      concurrency: 1,
      logger: silentLogger,
    });

    assert.equal((await worker.tick()).length, 0);

    const reportWorker = new JobWorker(store, {
      workerId: "worker-report",
      queues: ["default"],
      capabilities: ["report"],
      concurrency: 1,
      logger: silentLogger,
    });
    assert.equal((await reportWorker.tick()).length, 1);
    await reportWorker.active.get(job.id);
    assert.equal(store.findJob(job.id).status, "completed");
  } finally {
    await cleanup();
  }
});

const silentLogger = {
  log() {},
  error() {},
};

async function tempStore() {
  const dir = await mkdtemp(path.join(os.tmpdir(), "jobq-worker-test-"));
  const store = await new JobQueueStore({ dbPath: path.join(dir, "jobq.sqlite") }).init();
  return {
    store,
    cleanup: async () => {
      store.close();
      await rm(dir, { recursive: true, force: true });
    },
  };
}
