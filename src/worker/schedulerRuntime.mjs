import { sleep } from "./handlers.mjs";

export class JobScheduler {
  constructor(store, {
    schedulerId = process.env.SCHEDULER_ID || `scheduler-${process.pid}`,
    pollMs = Number(process.env.SCHEDULER_POLL_MS || 2000),
    batchSize = Number(process.env.SCHEDULER_BATCH_SIZE || 25),
    logger = console,
  } = {}) {
    this.store = store;
    this.schedulerId = schedulerId;
    this.pollMs = Math.max(500, Number(pollMs || 2000));
    this.batchSize = Math.max(1, Number(batchSize || 25));
    this.logger = logger;
    this.running = false;
  }

  async start() {
    this.running = true;
    this.logger.log(`scheduler ${this.schedulerId} polling every ${this.pollMs}ms`);
    while (this.running) {
      await this.tick();
      await sleep(this.pollMs);
    }
  }

  stop() {
    this.running = false;
  }

  async tick() {
    const dispatched = this.store.dispatchDueSchedules({
      schedulerId: this.schedulerId,
      limit: this.batchSize,
    });
    if (dispatched.length > 0) {
      this.logger.log(`scheduler dispatched ${dispatched.length} jobs`);
    }
    return dispatched;
  }
}
