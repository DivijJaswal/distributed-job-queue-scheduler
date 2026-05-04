#!/usr/bin/env node
import { JobQueueStore } from "../core/store.mjs";
import { JobScheduler } from "./schedulerRuntime.mjs";

const store = await new JobQueueStore().init();
const scheduler = new JobScheduler(store);

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

await scheduler.start();

function shutdown() {
  console.log("stopping scheduler...");
  scheduler.stop();
  store.close();
  process.exit(0);
}
