#!/usr/bin/env node
import { createApp } from "../server/server.mjs";
import { JobScheduler } from "../worker/schedulerRuntime.mjs";
import { JobWorker } from "../worker/workerRuntime.mjs";

const app = await createApp();
await app.listen();

const scheduler = new JobScheduler(app.store, {
  schedulerId: "scheduler-local",
});
const workerA = new JobWorker(app.store, {
  workerId: "worker-local-a",
  queues: ["critical", "default"],
  capabilities: ["email", "webhook", "cache"],
  concurrency: 2,
});
const workerB = new JobWorker(app.store, {
  workerId: "worker-local-b",
  queues: ["default", "bulk"],
  capabilities: ["report", "cache", "billing"],
  concurrency: 2,
});

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

void scheduler.start();
void workerA.start();
void workerB.start();

console.log("local mode started: server + scheduler + 2 workers");

async function shutdown() {
  console.log("stopping local mode...");
  scheduler.stop();
  await Promise.all([
    workerA.stop(),
    workerB.stop(),
  ]);
  await app.close();
  process.exit(0);
}
