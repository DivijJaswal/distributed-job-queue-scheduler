#!/usr/bin/env node
import { createRequire } from "node:module";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { createApp } from "../src/server/server.mjs";
import { JobScheduler } from "../src/worker/schedulerRuntime.mjs";
import { JobWorker } from "../src/worker/workerRuntime.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "..");
const artifactsDir = path.join(repoRoot, "artifacts");
const videosDir = path.join(artifactsDir, "videos");
const outputPath = path.join(artifactsDir, "distributed-job-queue-demo.webm");
const pauseMs = Number(process.env.DEMO_STEP_PAUSE_MS || 2000);
const logger = {
  log() {},
  warn() {},
  error() {},
};

await rm(videosDir, { recursive: true, force: true });
await mkdir(videosDir, { recursive: true });
await rm(outputPath, { force: true });

const { chromium } = loadPlaywright();
const tempDir = await mkdtemp(path.join(os.tmpdir(), "jobq-demo-record-"));
const app = await createApp({ dbPath: path.join(tempDir, "jobq.sqlite") });
await app.listen({ host: "127.0.0.1", port: 0 });
const { port } = app.server.address();
const baseUrl = `http://127.0.0.1:${port}`;

const workers = [
  new JobWorker(app.store, {
    workerId: "demo-critical-worker",
    queues: ["critical"],
    capabilities: ["webhook", "email"],
    concurrency: 2,
    pollMs: 250,
    leaseMs: 4000,
    logger,
  }),
  new JobWorker(app.store, {
    workerId: "demo-general-worker",
    queues: ["default", "bulk"],
    capabilities: ["email", "webhook", "report", "cache", "billing"],
    concurrency: 4,
    pollMs: 250,
    leaseMs: 4000,
    logger,
  }),
];
const scheduler = new JobScheduler(app.store, {
  schedulerId: "demo-scheduler",
  pollMs: 500,
  batchSize: 20,
  logger,
});

const workerRuns = workers.map((worker) => worker.start().catch((error) => {
  if (worker.running) {
    console.error(error);
  }
}));
const schedulerRun = scheduler.start().catch((error) => {
  if (scheduler.running) {
    console.error(error);
  }
});

let browser;
let context;

try {
  browser = await chromium.launch({
    headless: true,
    chromiumSandbox: false,
  });
  context = await browser.newContext({
    viewport: { width: 1440, height: 1000 },
    recordVideo: {
      dir: videosDir,
      size: { width: 1440, height: 1000 },
    },
  });
  const page = await context.newPage();

  await page.goto(baseUrl, { waitUntil: "domcontentloaded" });
  await page.waitForSelector("#healthPill", { state: "visible" });
  await page.waitForFunction(() => document.querySelector("#healthPill")?.textContent.includes("ok"));
  await installDemoCaption(page);

  await step(page, "Dashboard opens with health, metrics, throughput, workers, and operations panels.", async () => {
    await page.locator("body").evaluate((node) => node.scrollIntoView({ block: "start" }));
  });

  await clickStep(page, "#seedDemo", "Seed the demo scenario with queues, jobs, one schedule, and one workflow.");
  await waitForJobs(page);

  await clickStep(page, "[data-queue-toggle]", "Pause a queue to show queue enablement controls.");
  await clickStep(page, "[data-queue-toggle]", "Resume the queue.");

  await fillStep(page, "#queueForm [name=name]", "priority", "Create a new priority queue.");
  await fillStep(page, "#queueForm [name=concurrency]", "3", "Set queue concurrency.");
  await fillStep(page, "#queueForm [name=maxBacklog]", "2000", "Set queue backlog guardrail.");
  await fillStep(page, "#queueForm [name=rateLimitCount]", "5", "Add a queue-level dispatch limit.");
  await fillStep(page, "#queueForm [name=rateLimitWindowSeconds]", "30", "Set the queue rate-limit window.");
  await fillStep(page, "#queueForm [name=retryPolicy]", JSON.stringify({
    strategy: "exponential",
    baseMs: 1000,
    maxDelayMs: 20000,
    jitterMs: 300,
  }, null, 2), "Attach a queue retry policy.");
  await clickStep(page, "#queueForm button[type=submit]", "Save the priority queue.");

  await fillStep(page, "#rateLimitForm [name=target]", "billing.reconcile", "Create a type-specific throttle.");
  await fillStep(page, "#rateLimitForm [name=limitCount]", "2", "Limit billing reconciliation dispatches.");
  await fillStep(page, "#rateLimitForm [name=windowSeconds]", "20", "Set the throttle window.");
  await clickStep(page, "#rateLimitForm button[type=submit]", "Save the type rate limit.");

  await selectStep(page, "#jobForm [name=queue]", "priority", "Choose the priority queue for a new job.");
  await selectStep(page, "#jobForm [name=type]", "billing.reconcile", "Choose a billing job type.");
  await fillStep(page, "#jobForm [name=priority]", "9", "Set high job priority.");
  await fillStep(page, "#jobForm [name=maxAttempts]", "4", "Allow retries for this job.");
  await fillStep(page, "#jobForm [name=idempotencyKey]", "demo-billing-reconcile", "Set an idempotency key.");
  await fillStep(page, "#jobForm [name=requiredCapabilities]", "billing", "Require a billing-capable worker.");
  await fillStep(page, "#jobForm [name=payload]", JSON.stringify({
    accountId: "acct-demo-42",
    invoiceCount: 7,
    durationMs: 300,
  }, null, 2), "Provide the job payload.");
  await clickStep(page, "#jobForm button[type=submit]", "Enqueue the billing reconciliation job.");
  await waitForJobs(page);

  await selectStep(page, "#jobForm [name=queue]", "default", "Create a future scheduled job for cancellation.");
  await selectStep(page, "#jobForm [name=type]", "email.digest", "Choose the email job type.");
  await fillStep(page, "#jobForm [name=runAt]", futureLocalDateTime(), "Schedule the job in the future.");
  await fillStep(page, "#jobForm [name=idempotencyKey]", "demo-cancellable-email", "Use a unique idempotency key.");
  await fillStep(page, "#jobForm [name=requiredCapabilities]", "email", "Require email worker capability.");
  await fillStep(page, "#jobForm [name=payload]", JSON.stringify({
    recipient: "ops@example.com",
    template: "manual-cancel-demo",
    durationMs: 250,
  }, null, 2), "Prepare an email payload.");
  await clickStep(page, "#jobForm button[type=submit]", "Enqueue the scheduled job.");
  await clickStep(page, "[data-cancel-job]", "Cancel a queued or scheduled job.");

  await fillStep(page, "#scheduleForm [name=name]", "Two second heartbeat", "Create an interval schedule.");
  await selectStep(page, "#scheduleForm [name=queue]", "critical", "Dispatch scheduled work to the critical queue.");
  await fillStep(page, "#scheduleForm [name=type]", "webhook.deliver", "Set scheduled job type.");
  await fillStep(page, "#scheduleForm [name=intervalSeconds]", "2", "Set a short interval for the demo.");
  await fillStep(page, "#scheduleForm [name=requiredCapabilities]", "webhook", "Require webhook worker capability.");
  await fillStep(page, "#scheduleForm [name=payload]", JSON.stringify({
    endpoint: "https://example.com/demo-heartbeat",
    durationMs: 200,
  }, null, 2), "Provide schedule payload.");
  await clickStep(page, "#scheduleForm button[type=submit]", "Create the schedule.");
  await clickStep(page, "[data-schedule-run]", "Run a schedule immediately.");
  await clickStep(page, "#runSchedulerTick", "Manually dispatch due schedules.");

  await fillStep(page, "#workflowForm [name=name]", "Demo payout workflow", "Create a DAG workflow.");
  await fillStep(page, "#workflowForm [name=jobs]", JSON.stringify([
    {
      key: "extract",
      queue: "bulk",
      type: "report.generate",
      requiredCapabilities: ["report"],
      payload: { reportId: "payout-extract", rows: 1200, durationMs: 250 },
    },
    {
      key: "reconcile",
      queue: "priority",
      type: "billing.reconcile",
      requiredCapabilities: ["billing"],
      dependsOn: ["extract"],
      payload: { accountId: "acct-workflow", invoiceCount: 4, durationMs: 250 },
    },
    {
      key: "notify",
      queue: "critical",
      type: "webhook.deliver",
      requiredCapabilities: ["webhook"],
      dependsOn: ["reconcile"],
      payload: { endpoint: "https://example.com/payout-ready", durationMs: 200 },
    },
  ], null, 2), "Provide workflow job definitions with dependencies.");
  await clickStep(page, "#workflowForm button[type=submit]", "Create the workflow.");

  await injectScenario(page, "retryable", "Inject a retryable failure and watch retry handling.");
  await injectScenario(page, "timeout", "Inject a timeout scenario.");
  await injectScenario(page, "permanent", "Inject a permanent failure that can dead-letter.");
  await injectScenario(page, "dependency", "Inject a dependency failure workflow.");
  await injectScenario(page, "rate_burst", "Inject a rate-limited burst.");

  await clickStep(page, "#refreshAutoscale", "Refresh autoscale recommendation.");
  await fillStep(page, "#jobFilterForm [name=q]", "webhook", "Filter the jobs list by webhook.");
  await clickStep(page, "#jobFilterForm button[type=submit]", "Apply the job search filter.");
  await clickStep(page, "#resetJobFilters", "Reset job filters.");

  await clickStep(page, "[data-job-detail]", "Open a job details drawer with payload, contract, and timeline.");
  await clickStep(page, "#closeJobDrawer", "Close the job details drawer.");
  await clickStep(page, "[data-job-events]", "Show a job timeline toast.");
  await clickOptionalStep(page, "[data-requeue-job]", "Requeue a terminal job.");

  await clickStep(page, "#recoverLeases", "Recover expired leases and timed-out jobs.");
  await clickStep(page, "#replayDeadLetters", "Replay dead-lettered jobs.");
  await clickStep(page, "#exportSnapshot", "Export a JSON snapshot.");
  await clickStep(page, "#importSnapshotForm button[type=submit]", "Import the exported snapshot in merge mode.");

  await clickStep(page, "#vacuumDatabase", "Run SQLite vacuum maintenance.");
  await clickStep(page, "#pruneEvents", "Prune old audit events.");
  await clickStep(page, "#archiveJobs", "Archive old terminal jobs.");

  await step(page, "Demo complete: the dashboard now shows queues, workers, jobs, schedules, workflows, events, recovery, and maintenance.", async () => {
    await page.locator("body").evaluate((node) => node.scrollIntoView({ block: "start" }));
  });

  const video = page.video();
  await context.close();
  context = null;
  if (!video) {
    throw new Error("Playwright did not create a video artifact.");
  }
  await video.saveAs(outputPath);
  console.log(`Demo video saved to ${outputPath}`);
} finally {
  if (context) {
    await context.close().catch(() => {});
  }
  if (browser) {
    await browser.close().catch(() => {});
  }
  scheduler.stop();
  await Promise.all(workers.map((worker) => worker.stop({ timeoutMs: 3000 }).catch(() => {})));
  await Promise.allSettled([...workerRuns, schedulerRun]);
  await app.close().catch(() => {});
  await rm(tempDir, { recursive: true, force: true }).catch(() => {});
}

function loadPlaywright() {
  const errors = [];
  const candidates = [
    { label: "project", requirePath: import.meta.url },
    {
      label: "Codex bundled runtime",
      requirePath: path.join(os.homedir(), ".cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright-loader.cjs"),
    },
  ];

  for (const candidate of candidates) {
    try {
      return createRequire(candidate.requirePath)("playwright");
    } catch (error) {
      errors.push(`${candidate.label}: ${error.message}`);
    }
  }

  throw new Error(`Unable to load Playwright. Tried:\n${errors.join("\n")}`);
}

async function step(page, label, action = async () => {}) {
  await setCaption(page, label);
  await action();
  await page.waitForTimeout(pauseMs);
}

async function clickStep(page, selector, label) {
  await step(page, label, async () => {
    const locator = page.locator(selector).first();
    await locator.waitFor({ state: "visible", timeout: 15000 });
    await locator.evaluate((node) => node.scrollIntoView({ block: "center", inline: "center" }));
    try {
      await locator.click({ timeout: 5000 });
    } catch {
      await locator.click({ force: true });
    }
    await waitForButtonsSettled(page);
  });
}

async function clickOptionalStep(page, selector, label) {
  const locator = page.locator(selector).first();
  if (await locator.count() === 0) {
    await step(page, `${label} skipped because no matching terminal job is visible yet.`);
    return;
  }
  await clickStep(page, selector, label);
}

async function fillStep(page, selector, value, label) {
  await step(page, label, async () => {
    const locator = page.locator(selector).first();
    await locator.waitFor({ state: "visible", timeout: 15000 });
    await locator.evaluate((node) => node.scrollIntoView({ block: "center", inline: "center" }));
    await locator.fill(value);
  });
}

async function selectStep(page, selector, value, label) {
  await step(page, label, async () => {
    const locator = page.locator(selector).first();
    await locator.waitFor({ state: "visible", timeout: 15000 });
    await locator.evaluate((node) => node.scrollIntoView({ block: "center", inline: "center" }));
    await locator.selectOption(value);
  });
}

async function injectScenario(page, scenario, label) {
  await selectStep(page, "#failureForm [name=scenario]", scenario, label);
  await clickStep(page, "#failureForm button[type=submit]", `Submit ${scenario} scenario.`);
  await page.waitForTimeout(1000);
}

async function waitForJobs(page) {
  await page.waitForFunction(() => document.querySelectorAll(".job-row").length > 0, null, { timeout: 15000 });
}

async function waitForButtonsSettled(page) {
  await page.waitForFunction(() => [...document.querySelectorAll("button")].every((button) => !button.disabled), null, {
    timeout: 15000,
  }).catch(() => {});
}

async function installDemoCaption(page) {
  await page.addStyleTag({
    content: `
      #demoCaption {
        position: fixed;
        left: 18px;
        bottom: 18px;
        z-index: 9999;
        max-width: min(920px, calc(100vw - 36px));
        padding: 12px 14px;
        border: 2px solid #f8e16c;
        background: rgba(10, 14, 25, 0.94);
        color: #f8e16c;
        font: 600 15px/1.35 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
        box-shadow: 4px 4px 0 #000;
        pointer-events: none;
      }
    `,
  });
  await page.evaluate(() => {
    const existing = document.querySelector("#demoCaption");
    if (existing) {
      return;
    }
    const caption = document.createElement("div");
    caption.id = "demoCaption";
    document.body.append(caption);
  });
}

async function setCaption(page, text) {
  await page.evaluate((value) => {
    document.querySelector("#demoCaption").textContent = value;
  }, text);
}

function futureLocalDateTime() {
  const date = new Date(Date.now() + 60 * 60 * 1000);
  const pad = (value) => String(value).padStart(2, "0");
  return [
    date.getFullYear(),
    "-",
    pad(date.getMonth() + 1),
    "-",
    pad(date.getDate()),
    "T",
    pad(date.getHours()),
    ":",
    pad(date.getMinutes()),
  ].join("");
}
