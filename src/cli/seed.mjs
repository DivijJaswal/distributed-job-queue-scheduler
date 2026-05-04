#!/usr/bin/env node
import { JobQueueStore } from "../core/store.mjs";
import { addMs, nowIso } from "../core/time.mjs";

const store = await new JobQueueStore().init();

for (const queue of [
  { name: "critical", concurrency: 2, rateLimitCount: 20, rateLimitWindowMs: 60000 },
  { name: "default", concurrency: 4 },
  { name: "bulk", concurrency: 2, retryPolicy: { strategy: "exponential", baseMs: 1500, maxDelayMs: 30000, jitterMs: 500 } },
]) {
  store.upsertQueue(queue);
}

store.upsertRateLimit({
  scope: "type",
  target: "webhook.deliver",
  limitCount: 10,
  windowMs: 60000,
});

const jobs = [
  store.enqueueJob({
    queue: "critical",
    type: "webhook.deliver",
    priority: 10,
    timeoutMs: 3000,
    requiredCapabilities: ["webhook"],
    payload: { endpoint: "https://example.com/incident", durationMs: 450 },
  }),
  store.enqueueJob({
    queue: "default",
    type: "email.digest",
    priority: 3,
    requiredCapabilities: ["email"],
    payload: { recipient: "ops@example.com", template: "daily-ops", durationMs: 500 },
  }),
  store.enqueueJob({
    queue: "bulk",
    type: "report.generate",
    priority: 1,
    requiredCapabilities: ["report"],
    payload: { reportId: "usage-rollup", rows: 1200, durationMs: 800 },
  }),
  store.enqueueJob({
    queue: "default",
    type: "webhook.deliver",
    priority: 4,
    maxAttempts: 3,
    timeoutMs: 2000,
    requiredCapabilities: ["webhook"],
    retryPolicy: { strategy: "fixed", baseMs: 1000, maxDelayMs: 5000, jitterMs: 250 },
    payload: { endpoint: "https://example.com/retry", failUntilAttempt: 1, durationMs: 250 },
  }),
  store.enqueueJob({
    queue: "default",
    type: "cache.warm",
    priority: 2,
    runAt: addMs(nowIso(), 15000),
    requiredCapabilities: ["cache"],
    payload: { keyPattern: "dashboard:*" },
  }),
];

const schedules = [
  store.createSchedule({
    name: "Heartbeat fanout",
    queue: "critical",
    type: "webhook.deliver",
    intervalMs: 30000,
    priority: 7,
    requiredCapabilities: ["webhook"],
    payload: { endpoint: "https://example.com/heartbeat", durationMs: 300 },
  }),
  store.createSchedule({
    name: "Cron report demo",
    queue: "bulk",
    type: "report.generate",
    cronExpr: "*/5 * * * *",
    priority: 1,
    nextRunAt: addMs(nowIso(), 5000),
    requiredCapabilities: ["report"],
    payload: { reportId: "hourly-demo", rows: 500 },
  }),
];

const workflow = store.createWorkflow({
  name: "Demo ETL notification workflow",
  jobs: [
    { key: "extract", queue: "bulk", type: "report.generate", requiredCapabilities: ["report"], payload: { reportId: "etl-extract", rows: 750, durationMs: 500 } },
    { key: "transform", queue: "default", type: "cache.warm", requiredCapabilities: ["cache"], dependsOn: ["extract"], payload: { keyPattern: "etl:daily:*", durationMs: 250 } },
    { key: "notify", queue: "critical", type: "webhook.deliver", requiredCapabilities: ["webhook"], dependsOn: ["transform"], payload: { endpoint: "https://example.com/etl-complete", durationMs: 250 } },
  ],
});

console.log(`Seeded ${jobs.length} jobs, ${schedules.length} schedules, and workflow ${workflow.id}.`);
store.close();
