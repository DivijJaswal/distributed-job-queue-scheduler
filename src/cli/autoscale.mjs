#!/usr/bin/env node
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { JobQueueStore } from "../core/store.mjs";
import { sleep } from "../worker/handlers.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const workerEntry = path.resolve(__dirname, "../worker/worker.mjs");
const store = await new JobQueueStore().init();
const workers = new Map();

const intervalMs = Number(process.env.AUTOSCALE_INTERVAL_MS || 5000);
const minWorkers = Number(process.env.AUTOSCALE_MIN_WORKERS || 1);
const maxWorkers = Number(process.env.AUTOSCALE_MAX_WORKERS || 6);
const targetJobsPerWorker = Number(process.env.AUTOSCALE_TARGET_JOBS_PER_WORKER || 8);
const targetQueueAgeMs = Number(process.env.AUTOSCALE_TARGET_QUEUE_AGE_MS || 30000);

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

console.log(`autoscaler polling every ${intervalMs}ms with min=${minWorkers} max=${maxWorkers}`);

while (true) {
  const recommendation = store.getAutoscaleRecommendation({
    minWorkers,
    maxWorkers,
    targetJobsPerWorker,
    targetQueueAgeMs,
  });
  reconcile(recommendation);
  console.log(`${recommendation.action}: current=${workers.size} desired=${recommendation.desiredWorkers} pending=${recommendation.pendingJobs}`);
  await sleep(intervalMs);
}

function reconcile(recommendation) {
  while (workers.size < recommendation.desiredWorkers) {
    const id = `autoscaled-worker-${Date.now()}-${workers.size + 1}`;
    const child = spawn(process.execPath, [workerEntry], {
      env: {
        ...process.env,
        WORKER_ID: id,
        WORKER_QUEUES: process.env.WORKER_QUEUES || "critical,default,bulk",
        WORKER_CAPABILITIES: process.env.WORKER_CAPABILITIES || "",
      },
      stdio: "inherit",
    });
    workers.set(id, child);
    child.on("exit", () => workers.delete(id));
  }

  while (workers.size > recommendation.desiredWorkers) {
    const [id, child] = workers.entries().next().value;
    child.kill("SIGTERM");
    workers.delete(id);
  }
}

async function shutdown() {
  console.log("stopping autoscaler...");
  for (const child of workers.values()) {
    child.kill("SIGTERM");
  }
  store.close();
  process.exit(0);
}
