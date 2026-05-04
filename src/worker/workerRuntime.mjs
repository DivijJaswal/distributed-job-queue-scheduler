import { executeJob, sleep } from "./handlers.mjs";

export class JobWorker {
  constructor(store, {
    workerId = process.env.WORKER_ID || `worker-${process.pid}`,
    queues = parseList(process.env.WORKER_QUEUES || "critical,default,bulk"),
    capabilities = parseList(process.env.WORKER_CAPABILITIES || "email,webhook,report,cache,billing"),
    concurrency = Number(process.env.WORKER_CONCURRENCY || 2),
    leaseMs = Number(process.env.LEASE_MS || 30000),
    pollMs = Number(process.env.POLL_MS || 1000),
    backoffBaseMs = Number(process.env.BACKOFF_BASE_MS || 2000),
    drainTimeoutMs = Number(process.env.DRAIN_TIMEOUT_MS || 15000),
    logger = console,
  } = {}) {
    this.store = store;
    this.workerId = workerId;
    this.queues = queues;
    this.capabilities = capabilities;
    this.concurrency = Math.max(1, Number(concurrency || 1));
    this.leaseMs = Math.max(1000, Number(leaseMs || 30000));
    this.pollMs = Math.max(250, Number(pollMs || 1000));
    this.backoffBaseMs = Math.max(250, Number(backoffBaseMs || 2000));
    this.drainTimeoutMs = Math.max(0, Number(drainTimeoutMs || 0));
    this.logger = logger;
    this.running = false;
    this.active = new Map();
  }

  async start() {
    this.running = true;
    this.store.heartbeatWorker({
      workerId: this.workerId,
      queues: this.queues,
      capabilities: this.capabilities,
      concurrency: this.concurrency,
      activeJobs: this.active.size,
    });
    this.logger.log(`worker ${this.workerId} listening on ${this.queues.join(",")} with concurrency ${this.concurrency}`);

    while (this.running) {
      await this.tick();
      await sleep(this.pollMs);
    }
  }

  async stop({ timeoutMs = this.drainTimeoutMs } = {}) {
    this.running = false;
    this.store.markWorkerDraining(this.workerId);
    const deadline = Date.now() + Number(timeoutMs || 0);
    while (this.active.size > 0 && (timeoutMs === 0 || Date.now() < deadline)) {
      const waitMs = timeoutMs === 0 ? 100 : Math.max(1, Math.min(100, deadline - Date.now()));
      await sleep(waitMs);
    }
    if (this.active.size > 0) {
      this.store.releaseWorkerLeases(this.workerId, {
        reason: `Worker ${this.workerId} drained out with ${this.active.size} active jobs.`,
      });
    }
    this.store.markWorkerOffline(this.workerId);
  }

  async tick() {
    this.store.heartbeatWorker({
      workerId: this.workerId,
      queues: this.queues,
      capabilities: this.capabilities,
      concurrency: this.concurrency,
      activeJobs: this.active.size,
    });

    const openSlots = this.concurrency - this.active.size;
    if (openSlots <= 0) {
      return [];
    }

    const jobs = this.store.claimJobs({
      workerId: this.workerId,
      queues: this.queues,
      capabilities: this.capabilities,
      limit: openSlots,
      leaseMs: this.leaseMs,
    });
    for (const job of jobs) {
      this.runJob(job);
    }
    return jobs;
  }

  runJob(job) {
    this.store.addEvent({
      jobId: job.id,
      workerId: this.workerId,
      type: "job.started",
      message: `Worker ${this.workerId} started ${job.type}.`,
      metadata: { attempt: job.attempts },
    });
    const leaseRenewal = setInterval(() => {
      try {
        this.store.renewLease(job.id, this.workerId, this.leaseMs);
      } catch (error) {
        this.logger.error(`failed to renew lease for ${job.id}: ${error.message}`);
      }
    }, Math.max(500, Math.floor(this.leaseMs / 2)));

    const task = (async () => {
      try {
        const output = await executeWithTimeout(job);
        try {
          this.store.completeJob(job.id, this.workerId, output);
          this.logger.log(`completed ${job.id} ${job.type}`);
        } catch (error) {
          this.logger.error(`could not complete ${job.id} ${job.type}: ${error.message}`);
        }
      } catch (error) {
        try {
          const updated = this.store.failJob(job.id, this.workerId, error, {
            backoffBaseMs: this.backoffBaseMs,
          });
          this.logger.error(`${updated.status} ${job.id} ${job.type}: ${error.message}`);
        } catch (finalizeError) {
          this.logger.error(`could not fail ${job.id} ${job.type}: ${finalizeError.message}`);
        }
      } finally {
        clearInterval(leaseRenewal);
        this.active.delete(job.id);
      }
    })();

    this.active.set(job.id, task);
    return task;
  }
}

function parseList(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

async function executeWithTimeout(job) {
  const timeoutMs = Number(job.timeoutMs || 0);
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return executeJob(job);
  }

  let timeoutId;
  try {
    return await Promise.race([
      executeJob(job),
      new Promise((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error(`Job timed out after ${timeoutMs}ms.`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timeoutId);
  }
}
