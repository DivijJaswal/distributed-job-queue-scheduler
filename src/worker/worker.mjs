#!/usr/bin/env node
import { JobQueueStore } from "../core/store.mjs";
import { JobWorker } from "./workerRuntime.mjs";

const store = await new JobQueueStore().init();
const worker = new JobWorker(store);

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

await worker.start();

async function shutdown() {
  console.log("stopping worker...");
  await worker.stop({ timeoutMs: Number(process.env.DRAIN_TIMEOUT_MS || 15000) });
  store.close();
  process.exit(0);
}
