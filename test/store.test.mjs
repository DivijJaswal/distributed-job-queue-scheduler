import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { JobQueueStore } from "../src/core/store.mjs";
import { addMs, nowIso } from "../src/core/time.mjs";

test("claims due jobs by priority and gives them worker leases", async () => {
  const { store, cleanup } = await tempStore();
  try {
    store.enqueueJob({ queue: "default", type: "low", priority: 1 });
    const high = store.enqueueJob({ queue: "default", type: "high", priority: 10 });

    const claimed = store.claimJobs({
      workerId: "worker-a",
      queues: ["default"],
      limit: 1,
      leaseMs: 10000,
    });

    assert.equal(claimed.length, 1);
    assert.equal(claimed[0].id, high.id);
    assert.equal(claimed[0].status, "running");
    assert.equal(claimed[0].leaseOwner, "worker-a");
    assert.equal(claimed[0].attempts, 1);
  } finally {
    await cleanup();
  }
});

test("retryable failures are rescheduled and exhausted failures go to dead letter", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const job = store.enqueueJob({
      queue: "default",
      type: "webhook.deliver",
      maxAttempts: 2,
    });

    store.claimJobs({ workerId: "worker-a", queues: ["default"], limit: 1 });
    const retry = store.failJob(job.id, "worker-a", new Error("upstream timeout"), { backoffBaseMs: 10 });
    assert.equal(retry.status, "scheduled");
    assert.equal(retry.attempts, 1);

    store.db.prepare("UPDATE jobs SET run_at = ? WHERE id = ?").run(nowIso(), job.id);
    store.claimJobs({ workerId: "worker-a", queues: ["default"], limit: 1 });
    const dead = store.failJob(job.id, "worker-a", new Error("still failing"), { backoffBaseMs: 10 });
    assert.equal(dead.status, "dead_letter");
    assert.equal(dead.attempts, 2);
    assert.equal(dead.lastError, "still failing");
  } finally {
    await cleanup();
  }
});

test("dispatches due schedules and advances next run", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const schedule = store.createSchedule({
      name: "Fast heartbeat",
      queue: "critical",
      type: "webhook.deliver",
      intervalMs: 5000,
      nextRunAt: addMs(nowIso(), -1000),
    });

    const dispatched = store.dispatchDueSchedules({ schedulerId: "scheduler-a" });
    assert.equal(dispatched.length, 1);
    assert.equal(dispatched[0].schedule.id, schedule.id);
    assert.equal(dispatched[0].job.sourceScheduleId, schedule.id);
    assert.equal(store.findSchedule(schedule.id).lastRunAt !== null, true);
  } finally {
    await cleanup();
  }
});

test("recovers stale leases back to scheduled jobs", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const job = store.enqueueJob({ queue: "default", type: "report.generate", maxAttempts: 3 });
    store.claimJobs({ workerId: "worker-a", queues: ["default"], limit: 1, leaseMs: 1 });
    store.db.prepare("UPDATE jobs SET lease_expires_at = ? WHERE id = ?").run(addMs(nowIso(), -1000), job.id);

    const recovered = store.recoverStaleLeases();
    assert.equal(recovered.length, 1);
    assert.equal(recovered[0].status, "scheduled");
    assert.equal(recovered[0].leaseOwner, null);
    assert.match(recovered[0].lastError, /Lease expired/);
  } finally {
    await cleanup();
  }
});

test("enforces queue-level concurrency across worker claims", async () => {
  const { store, cleanup } = await tempStore();
  try {
    store.upsertQueue({ name: "default", concurrency: 1 });
    store.enqueueJob({ queue: "default", type: "one", priority: 5 });
    store.enqueueJob({ queue: "default", type: "two", priority: 4 });

    assert.equal(store.claimJobs({ workerId: "worker-a", queues: ["default"], limit: 1 }).length, 1);
    assert.equal(store.claimJobs({ workerId: "worker-b", queues: ["default"], limit: 1 }).length, 0);
  } finally {
    await cleanup();
  }
});

test("cron schedules compute and advance next run", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const schedule = store.createSchedule({
      name: "Cron heartbeat",
      queue: "critical",
      type: "webhook.deliver",
      cronExpr: "*/5 * * * *",
    });
    assert.equal(schedule.scheduleKind, "cron");
    assert.equal(schedule.cronExpr, "*/5 * * * *");

    store.db.prepare("UPDATE schedules SET next_run_at = ? WHERE id = ?").run(addMs(nowIso(), -1000), schedule.id);
    const dispatched = store.dispatchDueSchedules({ schedulerId: "scheduler-a" });
    assert.equal(dispatched.length, 1);
    assert.equal(store.findSchedule(schedule.id).nextRunAt > nowIso(), true);
  } finally {
    await cleanup();
  }
});

test("DAG workflow releases dependent jobs after parents complete", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const workflow = store.createWorkflow({
      name: "Demo DAG",
      jobs: [
        { key: "extract", queue: "bulk", type: "report.generate", payload: { durationMs: 1 } },
        { key: "notify", queue: "critical", type: "webhook.deliver", dependsOn: ["extract"], payload: { durationMs: 1 } },
      ],
    });
    const parent = workflow.jobs[0];
    const child = workflow.jobs[1];
    assert.equal(child.status, "waiting_dependencies");

    store.claimJobs({ workerId: "worker-a", queues: ["bulk"], limit: 1 });
    store.completeJob(parent.id, "worker-a", { ok: true });

    assert.equal(store.findJob(child.id).status, "queued");
  } finally {
    await cleanup();
  }
});

test("rejects payloads that do not match job type schemas", async () => {
  const { store, cleanup } = await tempStore();
  try {
    assert.throws(() => {
      store.enqueueJob({
        queue: "default",
        type: "email.digest",
        payload: { durationMs: "slow" },
      });
    }, /Payload validation failed/);
  } finally {
    await cleanup();
  }
});

test("queue max backlog applies backpressure", async () => {
  const { store, cleanup } = await tempStore();
  try {
    store.upsertQueue({ name: "tiny", concurrency: 1, maxBacklog: 1 });
    store.enqueueJob({ queue: "tiny", type: "generic.job" });
    assert.throws(() => {
      store.enqueueJob({ queue: "tiny", type: "generic.job" });
    }, /max backlog/);
  } finally {
    await cleanup();
  }
});

test("disabled queues pause dispatch until resumed", async () => {
  const { store, cleanup } = await tempStore();
  try {
    store.upsertQueue({ name: "paused", concurrency: 1, enabled: false });
    store.enqueueJob({ queue: "paused", type: "generic.job" });

    assert.equal(store.claimJobs({ workerId: "worker-a", queues: ["paused"], limit: 1 }).length, 0);

    store.toggleQueue("paused", true);
    assert.equal(store.claimJobs({ workerId: "worker-a", queues: ["paused"], limit: 1 }).length, 1);
  } finally {
    await cleanup();
  }
});

test("priority aging lets older jobs overtake newer high-priority jobs", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const oldLow = store.enqueueJob({ queue: "default", type: "old-low", priority: -5 });
    const newHigh = store.enqueueJob({ queue: "default", type: "new-high", priority: 10 });
    store.db.prepare("UPDATE jobs SET created_at = ? WHERE id = ?").run(addMs(nowIso(), -30000), oldLow.id);

    const claimed = store.claimJobs({
      workerId: "worker-a",
      queues: ["default"],
      limit: 1,
      agingIntervalMs: 1000,
    });

    assert.equal(claimed.length, 1);
    assert.equal(claimed[0].id, oldLow.id);
    assert.equal(store.findJob(newHigh.id).status, "queued");
  } finally {
    await cleanup();
  }
});

test("dependency-waiting jobs can be cancelled", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const workflow = store.createWorkflow({
      name: "Cancelable DAG",
      jobs: [
        { key: "parent", queue: "default", type: "generic.parent" },
        { key: "child", queue: "default", type: "generic.child", dependsOn: ["parent"] },
      ],
    });

    const cancelled = store.cancelJob(workflow.jobs[1].id);
    assert.equal(cancelled.status, "cancelled");
  } finally {
    await cleanup();
  }
});

test("bulk replay requeues dead-letter jobs", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const job = store.enqueueJob({ queue: "default", type: "webhook.deliver", maxAttempts: 1 });
    store.claimJobs({ workerId: "worker-a", queues: ["default"], limit: 1 });
    store.failJob(job.id, "worker-a", new Error("bad"));
    assert.equal(store.findJob(job.id).status, "dead_letter");

    const requeued = store.requeueDeadLetters({ queue: "default" });
    assert.equal(requeued.length, 1);
    assert.equal(store.findJob(job.id).status, "queued");
  } finally {
    await cleanup();
  }
});

test("worker capabilities gate job claiming", async () => {
  const { store, cleanup } = await tempStore();
  try {
    store.enqueueJob({
      queue: "default",
      type: "report.generate",
      requiredCapabilities: ["report"],
    });

    assert.equal(store.claimJobs({
      workerId: "worker-email",
      queues: ["default"],
      capabilities: ["email"],
      limit: 1,
    }).length, 0);
    assert.equal(store.claimJobs({
      workerId: "worker-report",
      queues: ["default"],
      capabilities: ["report"],
      limit: 1,
    }).length, 1);
  } finally {
    await cleanup();
  }
});

test("dispatch rate limits defer excess jobs", async () => {
  const { store, cleanup } = await tempStore();
  try {
    store.upsertRateLimit({
      scope: "type",
      target: "webhook.deliver",
      limitCount: 1,
      windowMs: 60000,
    });
    const first = store.enqueueJob({ queue: "default", type: "webhook.deliver" });
    const second = store.enqueueJob({ queue: "default", type: "webhook.deliver" });

    const claimed = store.claimJobs({ workerId: "worker-a", queues: ["default"], limit: 2 });

    assert.equal(claimed.length, 1);
    assert.equal(claimed[0].id, first.id);
    assert.equal(store.findJob(second.id).status, "scheduled");
    assert.match(store.findJob(second.id).blockedReason, /Rate limited/);
  } finally {
    await cleanup();
  }
});

test("timed-out running jobs move to dead letter and record guidance", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const job = store.enqueueJob({
      queue: "default",
      type: "email.digest",
      timeoutMs: 1,
      maxAttempts: 1,
    });
    store.claimJobs({ workerId: "worker-a", queues: ["default"], limit: 1 });
    store.db.prepare("UPDATE jobs SET started_at = ? WHERE id = ?").run(addMs(nowIso(), -5000), job.id);

    const recovered = store.recoverTimedOutJobs();

    assert.equal(recovered.length, 1);
    assert.equal(store.findJob(job.id).status, "dead_letter");
    assert.equal(store.findJob(job.id).failureHint.category, "timeout");
  } finally {
    await cleanup();
  }
});

test("permanent parent failure cancels dependent jobs", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const workflow = store.createWorkflow({
      name: "Failure DAG",
      jobs: [
        { key: "parent", queue: "default", type: "webhook.deliver", maxAttempts: 1 },
        { key: "child", queue: "default", type: "email.digest", dependsOn: ["parent"] },
      ],
    });
    const parent = workflow.jobs[0];
    const child = workflow.jobs[1];

    store.claimJobs({ workerId: "worker-a", queues: ["default"], limit: 1 });
    store.failJob(parent.id, "worker-a", new Error("upstream 503"));

    assert.equal(store.findJob(parent.id).status, "dead_letter");
    assert.equal(store.findJob(child.id).status, "cancelled");
    assert.match(store.findJob(child.id).blockedReason, /failed permanently/);
  } finally {
    await cleanup();
  }
});

test("exports and imports queue snapshots", async () => {
  const first = await tempStore();
  const second = await tempStore();
  try {
    const job = first.store.enqueueJob({ queue: "default", type: "email.digest" });
    const snapshot = first.store.exportSnapshot();

    second.store.importSnapshot(snapshot, { mode: "replace" });

    assert.equal(second.store.findJob(job.id).type, "email.digest");
    assert.equal(second.store.listQueues().length >= 1, true);
  } finally {
    await first.cleanup();
    await second.cleanup();
  }
});

test("tracks idempotency duplicate hits", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const first = store.enqueueJob({
      queue: "default",
      type: "email.digest",
      idempotencyKey: "idem-1",
    });
    const second = store.enqueueJob({
      queue: "default",
      type: "email.digest",
      idempotencyKey: "idem-1",
    });

    assert.equal(second.id, first.id);
    assert.equal(store.getIdempotencyStats().savedDuplicateSubmissions, 1);
  } finally {
    await cleanup();
  }
});

test("seeds demo scenario and failure injection scenarios", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const demo = store.seedDemoScenario();
    const timeout = store.injectFailureScenario("timeout");
    const dependency = store.injectFailureScenario("dependency");

    assert.equal(demo.jobs.length, 3);
    assert.equal(timeout.jobs[0].timeoutMs, 10);
    assert.equal(dependency.jobs[1].status, "waiting_dependencies");
    assert.equal(store.getIdempotencyStats().savedDuplicateSubmissions >= 1, true);
  } finally {
    await cleanup();
  }
});

test("returns throughput and worker pool summaries", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const job = store.enqueueJob({ queue: "default", type: "email.digest" });
    store.heartbeatWorker({
      workerId: "worker-a",
      queues: ["default"],
      capabilities: ["email"],
      concurrency: 3,
      activeJobs: 1,
    });
    store.claimJobs({ workerId: "worker-a", queues: ["default"], capabilities: ["email"], limit: 1 });
    store.completeJob(job.id, "worker-a", { ok: true });

    assert.equal(store.getThroughputSeries().at(-1).completed, 1);
    assert.equal(store.getWorkerPools()[0].capacity, 3);
  } finally {
    await cleanup();
  }
});

test("maintenance can archive terminal jobs and prune events", async () => {
  const { store, cleanup } = await tempStore();
  try {
    const job = store.enqueueJob({ queue: "default", type: "email.digest" });
    store.claimJobs({ workerId: "worker-a", queues: ["default"], limit: 1 });
    store.completeJob(job.id, "worker-a", { ok: true });

    const archive = store.archiveTerminalJobs({ olderThanDays: 0, limit: 10 });
    const prune = store.pruneEvents({ olderThanDays: 1, keepLast: 1 });

    assert.equal(archive.archived, 1);
    assert.throws(() => store.findJob(job.id), /Job not found/);
    assert.equal(typeof prune.pruned, "number");
  } finally {
    await cleanup();
  }
});

async function tempStore() {
  const dir = await mkdtemp(path.join(os.tmpdir(), "jobq-test-"));
  const store = await new JobQueueStore({ dbPath: path.join(dir, "jobq.sqlite") }).init();
  return {
    store,
    cleanup: async () => {
      store.close();
      await rm(dir, { recursive: true, force: true });
    },
  };
}
