#!/usr/bin/env node
import { JobQueueStore } from "../core/store.mjs";
import { sleep } from "../worker/handlers.mjs";
import { JobWorker } from "../worker/workerRuntime.mjs";

const silentLogger = {
  log() {},
  error() {},
};

const jobCount = numberArg("--jobs", 1000);
const workerCount = numberArg("--workers", 4);
const concurrency = numberArg("--concurrency", 4);
const durationMs = numberArg("--duration-ms", 5);
const timeoutMs = numberArg("--timeout-ms", 120000);
const queue = stringArg("--queue", "bulk");
const startedAt = Date.now();
const runKeyPrefix = `load-test-${startedAt}`;
const store = await new JobQueueStore().init();

store.upsertQueue({ name: queue, concurrency: Math.max(concurrency * workerCount, 1), maxBacklog: Math.max(jobCount * 2, 1000) });

for (let index = 0; index < jobCount; index += 1) {
  store.enqueueJob({
    queue,
    type: "cache.warm",
    priority: index % 10,
    idempotencyKey: `${runKeyPrefix}-${index}`,
    payload: { keyPattern: `load:${index}:*`, durationMs },
  });
}

const workers = Array.from({ length: workerCount }, (_, index) => new JobWorker(store, {
  workerId: `load-worker-${index + 1}`,
  queues: [queue],
  concurrency,
  pollMs: 1,
  logger: silentLogger,
}));

while (Date.now() - startedAt < timeoutMs) {
  await Promise.all(workers.map((worker) => worker.tick()));
  const current = getRunStats();
  const completed = current.completed;
  const deadLetter = current.deadLetter;
  if (completed + deadLetter >= jobCount) {
    break;
  }
  await sleep(1);
}

for (const worker of workers) {
  await worker.stop({ timeoutMs: 5000 });
}

const elapsedMs = Date.now() - startedAt;
const stats = getRunStats();
const completed = stats.completed;
const deadLetter = stats.deadLetter;
const throughput = completed > 0 ? Math.round((completed / elapsedMs) * 100000) / 100 : 0;

console.log(JSON.stringify({
  runKeyPrefix,
  jobCount,
  workerCount,
  concurrency,
  completed,
  deadLetter,
  elapsedMs,
  throughputJobsPerSecond: throughput,
  avgQueueWaitMs: stats.avgQueueWaitMs,
  avgEndToEndMs: stats.avgEndToEndMs,
}, null, 2));

store.close();

function numberArg(name, fallback) {
  const index = process.argv.indexOf(name);
  if (index === -1 || process.argv[index + 1] === undefined) {
    return Number(process.env[nameToEnv(name)] || fallback);
  }
  return Number(process.argv[index + 1]);
}

function stringArg(name, fallback) {
  const index = process.argv.indexOf(name);
  if (index === -1 || process.argv[index + 1] === undefined) {
    return process.env[nameToEnv(name)] || fallback;
  }
  return process.argv[index + 1];
}

function nameToEnv(name) {
  return name.replace(/^--/, "LOAD_TEST_").replaceAll("-", "_").toUpperCase();
}

function getRunStats() {
  const pattern = `${runKeyPrefix}-%`;
  const statusRows = store.db.prepare(`
    SELECT status, COUNT(*) AS count
    FROM jobs
    WHERE idempotency_key LIKE ?
    GROUP BY status
  `).all(pattern);
  const latency = store.db.prepare(`
    SELECT
      AVG((julianday(completed_at) - julianday(created_at)) * 86400000) AS avgEndToEndMs,
      AVG((julianday(started_at) - julianday(created_at)) * 86400000) AS avgQueueWaitMs
    FROM jobs
    WHERE idempotency_key LIKE ? AND completed_at IS NOT NULL
  `).get(pattern);
  const byStatus = Object.fromEntries(statusRows.map((row) => [row.status, Number(row.count)]));
  return {
    completed: byStatus.completed || 0,
    deadLetter: byStatus.dead_letter || 0,
    queued: byStatus.queued || 0,
    scheduled: byStatus.scheduled || 0,
    running: byStatus.running || 0,
    avgQueueWaitMs: round(Number(latency?.avgQueueWaitMs || 0), 2),
    avgEndToEndMs: round(Number(latency?.avgEndToEndMs || 0), 2),
  };
}

function round(value, places) {
  const factor = 10 ** places;
  return Math.round(value * factor) / factor;
}
