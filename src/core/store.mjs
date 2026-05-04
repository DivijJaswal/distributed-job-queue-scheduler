import { existsSync, statSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { nextCronRun } from "./cron.mjs";
import { recommendAutoscale } from "./autoscaler.mjs";
import { buildFailureGuidance } from "./failureGuidance.mjs";
import { createId, queueId } from "./ids.mjs";
import { validatePayload } from "./schemas.mjs";
import { addMs, nowIso } from "./time.mjs";

const TERMINAL_STATUSES = new Set(["completed", "cancelled", "dead_letter"]);
const ACTIVE_STATUSES = new Set(["queued", "scheduled", "waiting_dependencies", "running"]);
const SNAPSHOT_TABLES = ["queues", "rate_limits", "jobs", "schedules", "workers", "events", "job_archive"];

export class JobQueueStore {
  constructor({ dbPath = process.env.JOBQ_DB || "data/jobq.sqlite" } = {}) {
    this.dbPath = path.resolve(dbPath);
    this.db = null;
  }

  async init({ seed = true } = {}) {
    await mkdir(path.dirname(this.dbPath), { recursive: true });
    const { DatabaseSync } = await import("node:sqlite");
    this.db = new DatabaseSync(this.dbPath);
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA busy_timeout = 5000");
    this.db.exec("PRAGMA foreign_keys = ON");
    this.migrate();
    if (seed) {
      this.seedDefaults();
    }
    return this;
  }

  close() {
    this.db?.close();
  }

  migrate() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS queues (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        concurrency INTEGER NOT NULL DEFAULT 4,
        max_backlog INTEGER NOT NULL DEFAULT 10000,
        rate_limit_count INTEGER,
        rate_limit_window_ms INTEGER,
        retry_policy TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        queue TEXT NOT NULL,
        type TEXT NOT NULL,
        payload TEXT NOT NULL,
        status TEXT NOT NULL,
        priority INTEGER NOT NULL DEFAULT 0,
        attempts INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 3,
        run_at TEXT NOT NULL,
        lease_owner TEXT,
        lease_expires_at TEXT,
        source_schedule_id TEXT,
        idempotency_key TEXT UNIQUE,
        last_error TEXT,
        output TEXT,
        depends_on TEXT NOT NULL DEFAULT '[]',
        workflow_id TEXT,
        blocked_reason TEXT,
        schema_errors TEXT,
        timeout_ms INTEGER,
        required_capabilities TEXT NOT NULL DEFAULT '[]',
        retry_policy TEXT,
        failure_hint TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        started_at TEXT,
        completed_at TEXT,
        cancelled_at TEXT,
        dead_lettered_at TEXT,
        FOREIGN KEY(queue) REFERENCES queues(name)
      );

      CREATE INDEX IF NOT EXISTS idx_jobs_dispatch
        ON jobs(status, run_at, priority DESC, created_at);
      CREATE INDEX IF NOT EXISTS idx_jobs_queue_status
        ON jobs(queue, status);
      CREATE INDEX IF NOT EXISTS idx_jobs_lease
        ON jobs(status, lease_expires_at);

      CREATE TABLE IF NOT EXISTS schedules (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        queue TEXT NOT NULL,
        type TEXT NOT NULL,
        payload TEXT NOT NULL,
        schedule_kind TEXT NOT NULL DEFAULT 'interval',
        cron_expr TEXT,
        interval_ms INTEGER NOT NULL,
        enabled INTEGER NOT NULL DEFAULT 1,
        priority INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 3,
        timeout_ms INTEGER,
        required_capabilities TEXT NOT NULL DEFAULT '[]',
        retry_policy TEXT,
        next_run_at TEXT NOT NULL,
        last_run_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(queue) REFERENCES queues(name)
      );

      CREATE INDEX IF NOT EXISTS idx_schedules_due
        ON schedules(enabled, next_run_at);

      CREATE TABLE IF NOT EXISTS workers (
        id TEXT PRIMARY KEY,
        queues TEXT NOT NULL,
        capabilities TEXT NOT NULL DEFAULT '[]',
        concurrency INTEGER NOT NULL,
        status TEXT NOT NULL,
        active_jobs INTEGER NOT NULL DEFAULT 0,
        processed INTEGER NOT NULL DEFAULT 0,
        failed INTEGER NOT NULL DEFAULT 0,
        started_at TEXT NOT NULL,
        heartbeat_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        job_id TEXT,
        schedule_id TEXT,
        worker_id TEXT,
        type TEXT NOT NULL,
        message TEXT NOT NULL,
        metadata TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS rate_limits (
        id TEXT PRIMARY KEY,
        scope TEXT NOT NULL,
        target TEXT NOT NULL,
        limit_count INTEGER NOT NULL,
        window_ms INTEGER NOT NULL,
        enabled INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(scope, target)
      );

      CREATE TABLE IF NOT EXISTS job_archive (
        id TEXT PRIMARY KEY,
        job_json TEXT NOT NULL,
        archived_at TEXT NOT NULL
      );
    `);
    this.applyCompatMigrations();
  }

  applyCompatMigrations() {
    this.addColumnIfMissing("queues", "max_backlog", "INTEGER NOT NULL DEFAULT 10000");
    this.addColumnIfMissing("queues", "rate_limit_count", "INTEGER");
    this.addColumnIfMissing("queues", "rate_limit_window_ms", "INTEGER");
    this.addColumnIfMissing("queues", "retry_policy", "TEXT");
    this.addColumnIfMissing("jobs", "depends_on", "TEXT NOT NULL DEFAULT '[]'");
    this.addColumnIfMissing("jobs", "workflow_id", "TEXT");
    this.addColumnIfMissing("jobs", "blocked_reason", "TEXT");
    this.addColumnIfMissing("jobs", "schema_errors", "TEXT");
    this.addColumnIfMissing("jobs", "timeout_ms", "INTEGER");
    this.addColumnIfMissing("jobs", "required_capabilities", "TEXT NOT NULL DEFAULT '[]'");
    this.addColumnIfMissing("jobs", "retry_policy", "TEXT");
    this.addColumnIfMissing("jobs", "failure_hint", "TEXT");
    this.addColumnIfMissing("schedules", "schedule_kind", "TEXT NOT NULL DEFAULT 'interval'");
    this.addColumnIfMissing("schedules", "cron_expr", "TEXT");
    this.addColumnIfMissing("schedules", "timeout_ms", "INTEGER");
    this.addColumnIfMissing("schedules", "required_capabilities", "TEXT NOT NULL DEFAULT '[]'");
    this.addColumnIfMissing("schedules", "retry_policy", "TEXT");
    this.addColumnIfMissing("workers", "capabilities", "TEXT NOT NULL DEFAULT '[]'");
  }

  addColumnIfMissing(table, column, definition) {
    const columns = this.db.prepare(`PRAGMA table_info(${table})`).all();
    if (!columns.some((item) => item.name === column)) {
      this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
    }
  }

  seedDefaults() {
    for (const queue of [
      { name: "critical", concurrency: 2, maxBacklog: 1000 },
      { name: "default", concurrency: 4, maxBacklog: 5000 },
      { name: "bulk", concurrency: 2, maxBacklog: 10000 },
    ]) {
      this.upsertQueue(queue);
    }
  }

  upsertQueue(input = {}) {
    const at = nowIso();
    const name = normalizeName(input.name || "default");
    const existing = this.db.prepare("SELECT * FROM queues WHERE name = ?").get(name);
    const row = {
      id: queueId(name),
      name,
      concurrency: clampInt(input.concurrency ?? existing?.concurrency ?? 4, 1, 100),
      maxBacklog: clampInt(input.maxBacklog ?? input.max_backlog ?? existing?.max_backlog ?? 10000, 1, 1_000_000),
      rateLimitCount: optionalInt(input.rateLimitCount ?? input.rate_limit_count ?? existing?.rate_limit_count, 1, 1_000_000),
      rateLimitWindowMs: optionalInt(input.rateLimitWindowMs ?? input.rate_limit_window_ms ?? existing?.rate_limit_window_ms, 1000, 86_400_000),
      retryPolicy: normalizeRetryPolicy(input.retryPolicy ?? input.retry_policy ?? existing?.retry_policy),
      enabled: boolInt(input.enabled ?? existing?.enabled ?? true),
      createdAt: at,
      updatedAt: at,
    };
    this.db.prepare(`
      INSERT INTO queues (
        id, name, concurrency, max_backlog, rate_limit_count, rate_limit_window_ms,
        retry_policy, enabled, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(name) DO UPDATE SET
        concurrency = excluded.concurrency,
        max_backlog = excluded.max_backlog,
        rate_limit_count = excluded.rate_limit_count,
        rate_limit_window_ms = excluded.rate_limit_window_ms,
        retry_policy = excluded.retry_policy,
        enabled = excluded.enabled,
        updated_at = excluded.updated_at
    `).run(
      row.id,
      row.name,
      row.concurrency,
      row.maxBacklog,
      row.rateLimitCount,
      row.rateLimitWindowMs,
      row.retryPolicy ? JSON.stringify(row.retryPolicy) : null,
      row.enabled,
      row.createdAt,
      row.updatedAt,
    );
    return this.findQueue(row.name);
  }

  findQueue(name) {
    const row = this.db.prepare("SELECT * FROM queues WHERE name = ?").get(normalizeName(name));
    if (!row) {
      throw new NotFoundError(`Queue not found: ${name}`);
    }
    return mapQueue(row);
  }

  listQueues() {
    return this.db.prepare("SELECT * FROM queues ORDER BY name ASC").all().map(mapQueue);
  }

  toggleQueue(name, enabled) {
    const at = nowIso();
    const queueName = normalizeName(name);
    const row = this.db.prepare(`
      UPDATE queues SET enabled = ?, updated_at = ?
      WHERE name = ?
      RETURNING *
    `).get(boolInt(enabled), at, queueName);
    if (!row) {
      throw new NotFoundError(`Queue not found: ${name}`);
    }
    this.addEvent({
      type: enabled ? "queue.resumed" : "queue.paused",
      message: enabled ? `Queue ${queueName} resumed.` : `Queue ${queueName} paused.`,
      metadata: { queue: queueName },
    });
    return mapQueue(row);
  }

  upsertRateLimit(input = {}) {
    const at = nowIso();
    const scope = String(input.scope || "type").trim().toLowerCase();
    if (!["type", "queue"].includes(scope)) {
      throw new Error("Rate limit scope must be type or queue.");
    }
    const target = scope === "queue" ? normalizeName(input.target) : String(input.target || "").trim();
    if (!target) {
      throw new Error("Rate limit target is required.");
    }
    this.db.prepare(`
      INSERT INTO rate_limits (id, scope, target, limit_count, window_ms, enabled, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(scope, target) DO UPDATE SET
        limit_count = excluded.limit_count,
        window_ms = excluded.window_ms,
        enabled = excluded.enabled,
        updated_at = excluded.updated_at
    `).run(
      createId("rl", `${scope}-${target}`),
      scope,
      target,
      clampInt(input.limitCount ?? input.limit_count ?? 60, 1, 1_000_000),
      clampInt(input.windowMs ?? input.window_ms ?? 60000, 1000, 86_400_000),
      boolInt(input.enabled ?? true),
      at,
      at,
    );
    const rateLimit = this.db.prepare("SELECT * FROM rate_limits WHERE scope = ? AND target = ?").get(scope, target);
    this.addEvent({
      type: "rate_limit.upserted",
      message: `Rate limit set for ${scope}:${target}.`,
      metadata: mapRateLimit(rateLimit),
    });
    return mapRateLimit(rateLimit);
  }

  listRateLimits() {
    return this.db.prepare(`
      SELECT * FROM rate_limits
      ORDER BY scope ASC, target ASC
    `).all().map(mapRateLimit);
  }

  enqueueJob(input = {}) {
    const at = nowIso();
    const queue = normalizeName(input.queue || "default");
    this.ensureQueue(queue);
    const queueRow = this.findQueue(queue);
    const backlog = this.countActiveJobs(queue);
    if (backlog >= queueRow.maxBacklog) {
      throw new Error(`Queue "${queue}" is at max backlog ${queueRow.maxBacklog}.`);
    }

    const runAt = input.runAt ? new Date(input.runAt).toISOString() : at;
    const dependsOn = normalizeIdList(input.dependsOn ?? input.depends_on);
    const waitingOn = dependsOn.filter((jobId) => !this.isJobCompleted(jobId));
    const status = waitingOn.length > 0
      ? "waiting_dependencies"
      : new Date(runAt).getTime() > new Date(at).getTime() ? "scheduled" : "queued";
    const type = String(input.type || "generic.job").trim();
    const payload = normalizePayload(input.payload);
    const validation = validatePayload(type, payload);
    if (!validation.ok && input.allowInvalidPayload !== true) {
      throw new Error(`Payload validation failed: ${validation.errors.join(" ")}`);
    }
    const id = input.id || createId("job", `${queue}-${input.type || "job"}`);
    const requiredCapabilities = normalizeIdList(input.requiredCapabilities ?? input.required_capabilities);
    const retryPolicy = normalizeRetryPolicy(input.retryPolicy ?? input.retry_policy);
    const timeoutMs = optionalInt(input.timeoutMs ?? input.timeout_ms, 1, 86_400_000);

    try {
      this.db.prepare(`
        INSERT INTO jobs (
          id, queue, type, payload, status, priority, attempts, max_attempts,
          run_at, source_schedule_id, idempotency_key, depends_on, workflow_id,
          blocked_reason, schema_errors, timeout_ms, required_capabilities,
          retry_policy, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        id,
        queue,
        type,
        JSON.stringify(payload),
        status,
        clampInt(input.priority ?? 0, -1000, 1000),
        clampInt(input.maxAttempts ?? 3, 1, 50),
        runAt,
        input.sourceScheduleId || null,
        emptyToNull(input.idempotencyKey),
        JSON.stringify(dependsOn),
        emptyToNull(input.workflowId),
        waitingOn.length > 0 ? `Waiting on ${waitingOn.length} dependencies.` : null,
        validation.errors.length > 0 ? JSON.stringify(validation.errors) : null,
        timeoutMs,
        JSON.stringify(requiredCapabilities),
        retryPolicy ? JSON.stringify(retryPolicy) : null,
        at,
        at,
      );
    } catch (error) {
      if (String(error.message).includes("UNIQUE") && input.idempotencyKey) {
        const existing = this.db.prepare("SELECT * FROM jobs WHERE idempotency_key = ?").get(input.idempotencyKey);
        this.addEvent({
          jobId: existing.id,
          type: "job.idempotency_hit",
          message: `Duplicate enqueue skipped for idempotency key ${input.idempotencyKey}.`,
          metadata: { idempotencyKey: input.idempotencyKey },
        });
        return mapJob(existing);
      }
      throw error;
    }

    const job = this.findJob(id);
    this.addEvent({
      jobId: id,
      scheduleId: input.sourceScheduleId,
      type: "job.enqueued",
      message: `Job ${job.type} enqueued on ${job.queue}.`,
      metadata: {
        status: job.status,
        priority: job.priority,
        timeoutMs: job.timeoutMs,
        requiredCapabilities: job.requiredCapabilities,
      },
    });
    if (dependsOn.length > 0) {
      this.addEvent({
        jobId: id,
        type: "job.dependencies_registered",
        message: `Job waits on ${dependsOn.length} dependencies.`,
        metadata: { dependsOn },
      });
    }
    return job;
  }

  countActiveJobs(queue) {
    const placeholders = [...ACTIVE_STATUSES].map(() => "?").join(", ");
    const row = this.db.prepare(`
      SELECT COUNT(*) AS count FROM jobs
      WHERE queue = ? AND status IN (${placeholders})
    `).get(normalizeName(queue), ...ACTIVE_STATUSES);
    return Number(row?.count ?? 0);
  }

  isJobCompleted(jobId) {
    const row = this.db.prepare("SELECT status FROM jobs WHERE id = ?").get(jobId);
    return row?.status === "completed";
  }

  findJob(jobId) {
    const row = this.db.prepare("SELECT * FROM jobs WHERE id = ?").get(jobId);
    if (!row) {
      throw new NotFoundError(`Job not found: ${jobId}`);
    }
    return mapJob(row);
  }

  listJobs({ status, queue, type, workerId, workflowId, q, limit = 100 } = {}) {
    const clauses = [];
    const params = [];
    if (status) {
      clauses.push("status = ?");
      params.push(status);
    }
    if (queue) {
      clauses.push("queue = ?");
      params.push(normalizeName(queue));
    }
    if (type) {
      clauses.push("type = ?");
      params.push(String(type));
    }
    if (workerId) {
      clauses.push("lease_owner = ?");
      params.push(String(workerId));
    }
    if (workflowId) {
      clauses.push("workflow_id = ?");
      params.push(String(workflowId));
    }
    if (q) {
      clauses.push(`(
        id LIKE ?
        OR type LIKE ?
        OR idempotency_key LIKE ?
        OR payload LIKE ?
        OR last_error LIKE ?
        OR workflow_id LIKE ?
      )`);
      const pattern = `%${String(q).trim()}%`;
      params.push(pattern, pattern, pattern, pattern, pattern, pattern);
    }
    const where = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    return this.db.prepare(`
      SELECT * FROM jobs
      ${where}
      ORDER BY created_at DESC
      LIMIT ?
    `).all(...params, clampInt(limit, 1, 500)).map(mapJob);
  }

  claimJobs({
    workerId,
    queues = ["default"],
    capabilities = [],
    limit = 1,
    leaseMs = 30000,
    agingIntervalMs = Number(process.env.PRIORITY_AGING_MS || 60000),
  } = {}) {
    const at = nowIso();
    this.recoverStaleLeases({ now: at });
    this.recoverTimedOutJobs({ now: at });
    this.releaseReadyDependencies({ now: at });
    const normalizedQueues = queues.map(normalizeName).filter(Boolean);
    if (normalizedQueues.length === 0) {
      return [];
    }
    const claimed = [];
    const leaseUntil = addMs(at, leaseMs);
    const placeholders = normalizedQueues.map(() => "?").join(", ");
    const workerCapabilities = normalizeIdList(capabilities);
    const candidates = this.db.prepare(`
      SELECT jobs.*
      FROM jobs
      JOIN queues ON queues.name = jobs.queue
      WHERE jobs.status IN ('queued', 'scheduled')
        AND jobs.run_at <= ?
        AND jobs.queue IN (${placeholders})
        AND queues.enabled = 1
        AND (
          SELECT COUNT(*) FROM jobs AS running
          WHERE running.queue = jobs.queue AND running.status = 'running'
        ) < queues.concurrency
      ORDER BY
        (jobs.priority + CAST(((julianday(?) - julianday(jobs.created_at)) * 86400000 / ?) AS INTEGER)) DESC,
        jobs.run_at ASC,
        jobs.created_at ASC
      LIMIT ?
    `).all(
      at,
      ...normalizedQueues,
      at,
      Math.max(1, Number(agingIntervalMs || 60000)),
      clampInt(limit * 20, 1, 500),
    );

    for (const candidate of candidates) {
      if (claimed.length >= clampInt(limit, 1, 100)) {
        break;
      }
      const requiredCapabilities = parseJson(candidate.required_capabilities, []);
      if (!hasRequiredCapabilities(requiredCapabilities, workerCapabilities)) {
        continue;
      }
      const rateLimit = this.checkDispatchRateLimit(candidate, at);
      if (!rateLimit.allowed) {
        this.deferRateLimitedJob(candidate.id, rateLimit);
        continue;
      }
      const result = this.db.prepare(`
        UPDATE jobs SET
          status = 'running',
          attempts = attempts + 1,
          lease_owner = ?,
          lease_expires_at = ?,
          started_at = COALESCE(started_at, ?),
          updated_at = ?
        WHERE id = ?
          AND status IN ('queued', 'scheduled')
          AND run_at <= ?
          AND (
            SELECT COUNT(*) FROM jobs AS running
            WHERE running.queue = jobs.queue AND running.status = 'running'
          ) < (
            SELECT concurrency FROM queues WHERE queues.name = jobs.queue
          )
        RETURNING *
      `).get(workerId, leaseUntil, at, at, candidate.id, at);
      if (result) {
        claimed.push(mapJob(result));
        this.addEvent({
          jobId: result.id,
          workerId,
          type: "job.claimed",
          message: `Worker ${workerId} claimed ${result.type}.`,
          metadata: {
            leaseExpiresAt: leaseUntil,
            attempts: result.attempts,
            workerCapabilities,
          },
        });
      }
    }

    return claimed;
  }

  checkDispatchRateLimit(jobRow, at = nowIso()) {
    const checks = [];
    const queue = this.db.prepare("SELECT * FROM queues WHERE name = ?").get(jobRow.queue);
    if (queue?.rate_limit_count && queue?.rate_limit_window_ms) {
      checks.push({
        scope: "queue",
        target: jobRow.queue,
        limitCount: Number(queue.rate_limit_count),
        windowMs: Number(queue.rate_limit_window_ms),
      });
    }
    const configured = this.db.prepare(`
      SELECT * FROM rate_limits
      WHERE enabled = 1
        AND (
          (scope = 'type' AND target = ?)
          OR (scope = 'queue' AND target = ?)
        )
    `).all(jobRow.type, jobRow.queue);
    for (const item of configured) {
      checks.push({
        scope: item.scope,
        target: item.target,
        limitCount: Number(item.limit_count),
        windowMs: Number(item.window_ms),
      });
    }

    let denial = null;
    for (const check of checks) {
      const windowStart = addMs(at, -check.windowMs);
      const stats = this.getDispatchCount(check.scope, check.target, windowStart);
      if (stats.count >= check.limitCount) {
        const retryAt = addMs(stats.oldestAt || at, check.windowMs);
        if (!denial || new Date(retryAt).getTime() > new Date(denial.retryAt).getTime()) {
          denial = {
            allowed: false,
            retryAt,
            scope: check.scope,
            target: check.target,
            limitCount: check.limitCount,
            windowMs: check.windowMs,
            reason: `Rate limited by ${check.scope}:${check.target} at ${check.limitCount}/${Math.round(check.windowMs / 1000)}s.`,
          };
        }
      }
    }

    return denial ?? { allowed: true };
  }

  getDispatchCount(scope, target, windowStart) {
    const row = this.db.prepare(`
      SELECT COUNT(*) AS count, MIN(events.created_at) AS oldestAt
      FROM events
      JOIN jobs ON jobs.id = events.job_id
      WHERE events.type = 'job.claimed'
        AND events.created_at >= ?
        AND (
          (? = 'queue' AND jobs.queue = ?)
          OR (? = 'type' AND jobs.type = ?)
        )
    `).get(windowStart, scope, target, scope, target);
    return {
      count: Number(row?.count || 0),
      oldestAt: row?.oldestAt || null,
    };
  }

  deferRateLimitedJob(jobId, rateLimit) {
    const at = nowIso();
    const row = this.db.prepare(`
      UPDATE jobs SET
        status = 'scheduled',
        run_at = ?,
        blocked_reason = ?,
        updated_at = ?
      WHERE id = ? AND status IN ('queued', 'scheduled')
      RETURNING *
    `).get(rateLimit.retryAt, rateLimit.reason, at, jobId);
    if (row) {
      this.addEvent({
        jobId,
        type: "job.rate_limited",
        message: rateLimit.reason,
        metadata: rateLimit,
      });
    }
    return row ? mapJob(row) : null;
  }

  renewLease(jobId, workerId, leaseMs = 30000) {
    const at = nowIso();
    const row = this.db.prepare(`
      UPDATE jobs SET lease_expires_at = ?, updated_at = ?
      WHERE id = ? AND lease_owner = ? AND status = 'running'
      RETURNING *
    `).get(addMs(at, leaseMs), at, jobId, workerId);
    if (!row) {
      throw new NotFoundError(`Running job lease not found: ${jobId}`);
    }
    return mapJob(row);
  }

  completeJob(jobId, workerId, output = {}) {
    const at = nowIso();
    const row = this.db.prepare(`
      UPDATE jobs SET
        status = 'completed',
        output = ?,
        lease_owner = NULL,
        lease_expires_at = NULL,
        completed_at = ?,
        updated_at = ?
      WHERE id = ? AND lease_owner = ? AND status = 'running'
      RETURNING *
    `).get(JSON.stringify(output ?? {}), at, at, jobId, workerId);
    if (!row) {
      throw new NotFoundError(`Running job lease not found: ${jobId}`);
    }
    this.incrementWorker(workerId, { processed: 1, activeJobs: -1 });
    this.addEvent({
      jobId,
      workerId,
      type: "job.completed",
      message: `Job ${row.type} completed.`,
      metadata: output ?? {},
    });
    this.releaseReadyDependencies({ now: at, completedJobId: jobId });
    return mapJob(row);
  }

  releaseReadyDependencies({ now = nowIso(), completedJobId = null } = {}) {
    const waiting = this.db.prepare(`
      SELECT * FROM jobs
      WHERE status = 'waiting_dependencies'
      ORDER BY created_at ASC
    `).all();
    const released = [];
    for (const row of waiting) {
      const dependencies = parseJson(row.depends_on, []);
      if (dependencies.length === 0 || dependencies.every((jobId) => this.isJobCompleted(jobId))) {
        const nextStatus = new Date(row.run_at).getTime() > new Date(now).getTime() ? "scheduled" : "queued";
        const updated = this.db.prepare(`
          UPDATE jobs SET status = ?, blocked_reason = NULL, updated_at = ?
          WHERE id = ? AND status = 'waiting_dependencies'
          RETURNING *
        `).get(nextStatus, now, row.id);
        if (updated) {
          this.addEvent({
            jobId: updated.id,
            type: "job.dependencies_released",
            message: completedJobId
              ? `Dependencies satisfied after ${completedJobId} completed.`
              : "Dependencies satisfied.",
            metadata: { dependsOn: dependencies },
          });
          released.push(mapJob(updated));
        }
      }
    }
    return released;
  }

  failJob(jobId, workerId, error, { backoffBaseMs = 2000 } = {}) {
    const at = nowIso();
    const current = this.db.prepare("SELECT * FROM jobs WHERE id = ? AND lease_owner = ? AND status = 'running'").get(jobId, workerId);
    if (!current) {
      throw new NotFoundError(`Running job lease not found: ${jobId}`);
    }
    const lastError = String(error?.message || error || "Job failed");
    const shouldRetry = Number(current.attempts) < Number(current.max_attempts);
    const nextStatus = shouldRetry ? "scheduled" : "dead_letter";
    const policy = this.resolveRetryPolicy(current, { backoffBaseMs });
    const backoffMs = computeRetryDelayMs(policy, Number(current.attempts));
    const runAt = shouldRetry ? addMs(at, backoffMs) : current.run_at;
    const deadLetteredAt = shouldRetry ? null : at;
    const failureHint = buildFailureGuidance(lastError, mapJob(current));
    const row = this.db.prepare(`
      UPDATE jobs SET
        status = ?,
        last_error = ?,
        run_at = ?,
        lease_owner = NULL,
        lease_expires_at = NULL,
        failure_hint = ?,
        dead_lettered_at = ?,
        updated_at = ?
      WHERE id = ?
      RETURNING *
    `).get(nextStatus, lastError, runAt, JSON.stringify(failureHint), deadLetteredAt, at, jobId);
    this.incrementWorker(workerId, { failed: 1, activeJobs: -1 });
    this.addEvent({
      jobId,
      workerId,
      type: shouldRetry ? "job.retry_scheduled" : "job.dead_lettered",
      message: shouldRetry ? `Job retry scheduled after ${backoffMs}ms.` : "Job moved to dead letter.",
      metadata: { error: lastError, attempts: row.attempts, maxAttempts: row.max_attempts, retryPolicy: policy, failureHint },
    });
    if (!shouldRetry) {
      this.cancelDependentJobs(jobId, {
        reason: `Dependency ${jobId} failed permanently: ${lastError}`,
      });
    }
    return mapJob(row);
  }

  resolveRetryPolicy(jobRow, { backoffBaseMs = 2000 } = {}) {
    const jobPolicy = parseJson(jobRow.retry_policy, null);
    if (jobPolicy) {
      return normalizeRetryPolicy(jobPolicy);
    }
    const queue = this.db.prepare("SELECT retry_policy FROM queues WHERE name = ?").get(jobRow.queue);
    const queuePolicy = parseJson(queue?.retry_policy, null);
    return normalizeRetryPolicy(queuePolicy) ?? {
      strategy: "exponential",
      baseMs: Math.max(250, Number(backoffBaseMs || 2000)),
      maxDelayMs: 300000,
      jitterMs: 0,
    };
  }

  cancelJob(jobId) {
    const at = nowIso();
    const row = this.db.prepare(`
      UPDATE jobs SET status = 'cancelled', cancelled_at = ?, updated_at = ?
      WHERE id = ? AND status IN ('queued', 'scheduled', 'waiting_dependencies')
      RETURNING *
    `).get(at, at, jobId);
    if (!row) {
      throw new Error("Only queued, scheduled, or dependency-waiting jobs can be cancelled.");
    }
    this.addEvent({
      jobId,
      type: "job.cancelled",
      message: `Job ${row.type} cancelled.`,
    });
    this.cancelDependentJobs(jobId, {
      reason: `Dependency ${jobId} was cancelled.`,
    });
    return mapJob(row);
  }

  cancelDependentJobs(parentJobId, { reason = "Dependency was cancelled or failed." } = {}) {
    const at = nowIso();
    const candidates = this.db.prepare(`
      SELECT * FROM jobs
      WHERE status IN ('waiting_dependencies', 'queued', 'scheduled')
      ORDER BY created_at ASC
    `).all();
    const cancelled = [];
    for (const candidate of candidates) {
      const dependsOn = parseJson(candidate.depends_on, []);
      if (!dependsOn.includes(parentJobId)) {
        continue;
      }
      const row = this.db.prepare(`
        UPDATE jobs SET
          status = 'cancelled',
          blocked_reason = ?,
          cancelled_at = ?,
          updated_at = ?
        WHERE id = ? AND status IN ('waiting_dependencies', 'queued', 'scheduled')
        RETURNING *
      `).get(reason, at, at, candidate.id);
      if (row) {
        this.addEvent({
          jobId: row.id,
          type: "job.dependency_cancelled",
          message: reason,
          metadata: { parentJobId },
        });
        cancelled.push(mapJob(row));
        cancelled.push(...this.cancelDependentJobs(row.id, {
          reason: `Dependency ${row.id} was cancelled because ${parentJobId} could not complete.`,
        }));
      }
    }
    return cancelled;
  }

  requeueJob(jobId, { runAt = nowIso() } = {}) {
    const at = nowIso();
    const current = this.findJob(jobId);
    if (!TERMINAL_STATUSES.has(current.status) && current.status !== "failed") {
      throw new Error("Only completed, cancelled, failed, or dead-letter jobs can be requeued.");
    }
    const status = new Date(runAt).getTime() > Date.now() ? "scheduled" : "queued";
    const row = this.db.prepare(`
      UPDATE jobs SET
        status = ?,
        run_at = ?,
        attempts = 0,
        lease_owner = NULL,
        lease_expires_at = NULL,
        last_error = NULL,
        blocked_reason = NULL,
        failure_hint = NULL,
        output = NULL,
        started_at = NULL,
        completed_at = NULL,
        cancelled_at = NULL,
        dead_lettered_at = NULL,
        updated_at = ?
      WHERE id = ?
      RETURNING *
    `).get(status, runAt, at, jobId);
    this.addEvent({
      jobId,
      type: "job.requeued",
      message: `Job ${row.type} requeued.`,
    });
    return mapJob(row);
  }

  requeueDeadLetters({ queue, type, limit = 100, maxAttempts } = {}) {
    const clauses = ["status = 'dead_letter'"];
    const params = [];
    if (queue) {
      clauses.push("queue = ?");
      params.push(normalizeName(queue));
    }
    if (type) {
      clauses.push("type = ?");
      params.push(String(type));
    }
    const rows = this.db.prepare(`
      SELECT id FROM jobs
      WHERE ${clauses.join(" AND ")}
      ORDER BY dead_lettered_at DESC
      LIMIT ?
    `).all(...params, clampInt(limit, 1, 1000));

    const requeued = [];
    for (const row of rows) {
      if (maxAttempts !== undefined) {
        this.db.prepare("UPDATE jobs SET max_attempts = ? WHERE id = ?").run(clampInt(maxAttempts, 1, 50), row.id);
      }
      requeued.push(this.requeueJob(row.id));
    }
    this.addEvent({
      type: "dead_letter.bulk_replayed",
      message: `Requeued ${requeued.length} dead-letter jobs.`,
      metadata: { queue, type, limit, maxAttempts },
    });
    return requeued;
  }

  createWorkflow({ name = "Workflow", jobs = [] } = {}) {
    if (!Array.isArray(jobs) || jobs.length === 0) {
      throw new Error("Workflow requires at least one job.");
    }
    const workflowId = createId("wf", name);
    const created = [];
    const aliases = new Map();

    for (const definition of jobs) {
      const dependsOn = normalizeIdList(definition.dependsOn).map((dependency) => aliases.get(dependency) || dependency);
      const job = this.enqueueJob({
        ...definition,
        dependsOn,
        workflowId,
        idempotencyKey: definition.idempotencyKey || `${workflowId}:${definition.key || definition.type || created.length}`,
      });
      created.push(job);
      if (definition.key) {
        aliases.set(definition.key, job.id);
      }
    }

    this.addEvent({
      type: "workflow.created",
      message: `Workflow ${name} created with ${created.length} jobs.`,
      metadata: { workflowId, jobs: created.map((job) => job.id) },
    });
    return {
      id: workflowId,
      name,
      jobs: created,
    };
  }

  createSchedule(input = {}) {
    const at = nowIso();
    const queue = normalizeName(input.queue || "default");
    this.ensureQueue(queue);
    const id = input.id || createId("sched", input.name || input.type || queue);
    const payload = normalizePayload(input.payload);
    const validation = validatePayload(String(input.type || "generic.scheduled").trim(), payload);
    if (!validation.ok && input.allowInvalidPayload !== true) {
      throw new Error(`Payload validation failed: ${validation.errors.join(" ")}`);
    }
    const scheduleKind = input.cronExpr || input.cron || input.scheduleKind === "cron" ? "cron" : "interval";
    const cronExpr = scheduleKind === "cron" ? String(input.cronExpr || input.cron).trim() : null;
    const intervalMs = scheduleKind === "cron"
      ? clampInt(input.intervalMs ?? 60000, 1000, 31_536_000_000)
      : clampInt(input.intervalMs ?? 60000, 1000, 31_536_000_000);
    const nextRunAt = input.nextRunAt
      ? new Date(input.nextRunAt).toISOString()
      : scheduleKind === "cron" ? nextCronRun(cronExpr, new Date(at)) : at;
    const requiredCapabilities = normalizeIdList(input.requiredCapabilities ?? input.required_capabilities);
    const retryPolicy = normalizeRetryPolicy(input.retryPolicy ?? input.retry_policy);
    const timeoutMs = optionalInt(input.timeoutMs ?? input.timeout_ms, 1, 86_400_000);
    this.db.prepare(`
      INSERT INTO schedules (
        id, name, queue, type, payload, schedule_kind, cron_expr, interval_ms, enabled, priority,
        max_attempts, timeout_ms, required_capabilities, retry_policy, next_run_at, created_at, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      String(input.name || input.type || "Recurring job").trim(),
      queue,
      String(input.type || "generic.scheduled").trim(),
      JSON.stringify(payload),
      scheduleKind,
      cronExpr,
      intervalMs,
      boolInt(input.enabled ?? true),
      clampInt(input.priority ?? 0, -1000, 1000),
      clampInt(input.maxAttempts ?? 3, 1, 50),
      timeoutMs,
      JSON.stringify(requiredCapabilities),
      retryPolicy ? JSON.stringify(retryPolicy) : null,
      nextRunAt,
      at,
      at,
    );
    const schedule = this.findSchedule(id);
    this.addEvent({
      scheduleId: id,
      type: "schedule.created",
      message: `Schedule ${schedule.name} created.`,
      metadata: { scheduleKind: schedule.scheduleKind, intervalMs: schedule.intervalMs, cronExpr: schedule.cronExpr },
    });
    return schedule;
  }

  findSchedule(scheduleId) {
    const row = this.db.prepare("SELECT * FROM schedules WHERE id = ?").get(scheduleId);
    if (!row) {
      throw new NotFoundError(`Schedule not found: ${scheduleId}`);
    }
    return mapSchedule(row);
  }

  listSchedules() {
    return this.db.prepare("SELECT * FROM schedules ORDER BY created_at DESC").all().map(mapSchedule);
  }

  toggleSchedule(scheduleId, enabled) {
    const at = nowIso();
    const row = this.db.prepare(`
      UPDATE schedules SET enabled = ?, updated_at = ?
      WHERE id = ?
      RETURNING *
    `).get(boolInt(enabled), at, scheduleId);
    if (!row) {
      throw new NotFoundError(`Schedule not found: ${scheduleId}`);
    }
    this.addEvent({
      scheduleId,
      type: enabled ? "schedule.resumed" : "schedule.paused",
      message: enabled ? "Schedule resumed." : "Schedule paused.",
    });
    return mapSchedule(row);
  }

  runScheduleNow(scheduleId) {
    const schedule = this.findSchedule(scheduleId);
    return this.enqueueJob({
      queue: schedule.queue,
      type: schedule.type,
      payload: {
        ...schedule.payload,
        scheduledBy: schedule.id,
        runReason: "manual",
      },
      priority: schedule.priority,
      maxAttempts: schedule.maxAttempts,
      timeoutMs: schedule.timeoutMs,
      requiredCapabilities: schedule.requiredCapabilities,
      retryPolicy: schedule.retryPolicy,
      sourceScheduleId: schedule.id,
    });
  }

  dispatchDueSchedules({ schedulerId = "scheduler", limit = 25 } = {}) {
    const at = nowIso();
    const rows = this.db.prepare(`
      SELECT * FROM schedules
      WHERE enabled = 1 AND next_run_at <= ?
      ORDER BY next_run_at ASC
      LIMIT ?
    `).all(at, clampInt(limit, 1, 100));
    const dispatched = [];
    for (const schedule of rows) {
      const nextRunAt = schedule.schedule_kind === "cron"
        ? nextCronRun(schedule.cron_expr, new Date(at))
        : addMs(schedule.next_run_at, schedule.interval_ms);
      const updated = this.db.prepare(`
        UPDATE schedules SET last_run_at = ?, next_run_at = ?, updated_at = ?
        WHERE id = ? AND next_run_at = ?
        RETURNING *
      `).get(at, nextRunAt, at, schedule.id, schedule.next_run_at);
      if (!updated) {
        continue;
      }
      const job = this.enqueueJob({
        queue: schedule.queue,
        type: schedule.type,
        payload: {
          ...JSON.parse(schedule.payload),
          scheduledBy: schedule.id,
          scheduledAt: at,
        },
        priority: schedule.priority,
        maxAttempts: schedule.max_attempts,
        timeoutMs: schedule.timeout_ms,
        requiredCapabilities: parseJson(schedule.required_capabilities, []),
        retryPolicy: parseJson(schedule.retry_policy, null),
        sourceScheduleId: schedule.id,
      });
      this.addEvent({
        jobId: job.id,
        scheduleId: schedule.id,
        workerId: schedulerId,
        type: "schedule.dispatched",
        message: `Schedule ${schedule.name} dispatched ${job.type}.`,
      });
      dispatched.push({
        schedule: mapSchedule(updated),
        job,
      });
    }
    return dispatched;
  }

  heartbeatWorker({ workerId, queues = [], capabilities = [], concurrency = 1, activeJobs = 0, status = "online" } = {}) {
    const at = nowIso();
    const id = String(workerId || "worker-local");
    this.db.prepare(`
      INSERT INTO workers (id, queues, capabilities, concurrency, status, active_jobs, processed, failed, started_at, heartbeat_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        queues = excluded.queues,
        capabilities = excluded.capabilities,
        concurrency = excluded.concurrency,
        status = excluded.status,
        active_jobs = excluded.active_jobs,
        heartbeat_at = excluded.heartbeat_at,
        updated_at = excluded.updated_at
    `).run(
      id,
      JSON.stringify(queues),
      JSON.stringify(normalizeIdList(capabilities)),
      clampInt(concurrency, 1, 100),
      status,
      Math.max(0, Number(activeJobs || 0)),
      at,
      at,
      at,
    );
    return this.findWorker(id);
  }

  incrementWorker(workerId, { processed = 0, failed = 0, activeJobs = 0 } = {}) {
    const at = nowIso();
    this.db.prepare(`
      UPDATE workers SET
        processed = MAX(0, processed + ?),
        failed = MAX(0, failed + ?),
        active_jobs = MAX(0, active_jobs + ?),
        heartbeat_at = ?,
        updated_at = ?
      WHERE id = ?
    `).run(Number(processed), Number(failed), Number(activeJobs), at, at, workerId);
  }

  markWorkerOffline(workerId) {
    const at = nowIso();
    this.db.prepare("UPDATE workers SET status = 'offline', active_jobs = 0, updated_at = ? WHERE id = ?").run(at, workerId);
  }

  markWorkerDraining(workerId) {
    const at = nowIso();
    this.db.prepare("UPDATE workers SET status = 'draining', updated_at = ? WHERE id = ?").run(at, workerId);
  }

  releaseWorkerLeases(workerId, { reason = "Worker stopped before job completed." } = {}) {
    const at = nowIso();
    const rows = this.db.prepare(`
      UPDATE jobs SET
        status = 'scheduled',
        run_at = ?,
        lease_owner = NULL,
        lease_expires_at = NULL,
        last_error = ?,
        updated_at = ?
      WHERE status = 'running' AND lease_owner = ?
      RETURNING *
    `).all(at, reason, at, workerId);
    for (const row of rows) {
      this.addEvent({
        jobId: row.id,
        workerId,
        type: "job.lease_released",
        message: reason,
      });
    }
    return rows.map(mapJob);
  }

  findWorker(workerId) {
    const row = this.db.prepare("SELECT * FROM workers WHERE id = ?").get(workerId);
    if (!row) {
      throw new NotFoundError(`Worker not found: ${workerId}`);
    }
    return mapWorker(row);
  }

  listWorkers() {
    return this.db.prepare("SELECT * FROM workers ORDER BY heartbeat_at DESC").all().map(mapWorker);
  }

  recoverStaleLeases({ now = nowIso() } = {}) {
    const stale = this.db.prepare(`
      SELECT * FROM jobs
      WHERE status = 'running'
        AND lease_expires_at IS NOT NULL
        AND lease_expires_at < ?
    `).all(now);
    const recovered = [];
    for (const job of stale) {
      const shouldRetry = Number(job.attempts) < Number(job.max_attempts);
      const status = shouldRetry ? "scheduled" : "dead_letter";
      const error = `Lease expired for worker ${job.lease_owner}`;
      const failureHint = buildFailureGuidance(error, mapJob(job));
      const row = this.db.prepare(`
        UPDATE jobs SET
          status = ?,
          run_at = ?,
          lease_owner = NULL,
          lease_expires_at = NULL,
          last_error = ?,
          failure_hint = ?,
          dead_lettered_at = ?,
          updated_at = ?
        WHERE id = ?
        RETURNING *
      `).get(
        status,
        now,
        error,
        JSON.stringify(failureHint),
        shouldRetry ? null : now,
        now,
        job.id,
      );
      this.addEvent({
        jobId: job.id,
        workerId: job.lease_owner,
        type: shouldRetry ? "job.lease_recovered" : "job.dead_lettered",
        message: shouldRetry ? "Expired lease recovered." : "Expired lease moved job to dead letter.",
        metadata: { failureHint },
      });
      if (!shouldRetry) {
        this.cancelDependentJobs(job.id, {
          reason: `Dependency ${job.id} failed after lease expiry.`,
        });
      }
      recovered.push(mapJob(row));
    }
    return recovered;
  }

  recoverTimedOutJobs({ now = nowIso() } = {}) {
    const timedOut = this.db.prepare(`
      SELECT * FROM jobs
      WHERE status = 'running'
        AND timeout_ms IS NOT NULL
        AND started_at IS NOT NULL
        AND ((julianday(?) - julianday(started_at)) * 86400000) >= timeout_ms
    `).all(now);
    const recovered = [];
    for (const job of timedOut) {
      const shouldRetry = Number(job.attempts) < Number(job.max_attempts);
      const status = shouldRetry ? "scheduled" : "dead_letter";
      const policy = this.resolveRetryPolicy(job);
      const delayMs = shouldRetry ? computeRetryDelayMs(policy, Number(job.attempts)) : 0;
      const error = `Job timed out after ${job.timeout_ms}ms.`;
      const failureHint = buildFailureGuidance(error, mapJob(job));
      const row = this.db.prepare(`
        UPDATE jobs SET
          status = ?,
          run_at = ?,
          lease_owner = NULL,
          lease_expires_at = NULL,
          last_error = ?,
          failure_hint = ?,
          dead_lettered_at = ?,
          updated_at = ?
        WHERE id = ? AND status = 'running'
        RETURNING *
      `).get(
        status,
        shouldRetry ? addMs(now, delayMs) : job.run_at,
        error,
        JSON.stringify(failureHint),
        shouldRetry ? null : now,
        now,
        job.id,
      );
      if (row) {
        this.addEvent({
          jobId: job.id,
          workerId: job.lease_owner,
          type: shouldRetry ? "job.timeout_recovered" : "job.dead_lettered",
          message: shouldRetry ? `Timed out job scheduled for retry after ${delayMs}ms.` : "Timed out job moved to dead letter.",
          metadata: { timeoutMs: job.timeout_ms, retryPolicy: policy, failureHint },
        });
        if (!shouldRetry) {
          this.cancelDependentJobs(job.id, {
            reason: `Dependency ${job.id} timed out permanently.`,
          });
        }
        recovered.push(mapJob(row));
      }
    }
    return recovered;
  }

  getMetrics() {
    const jobsByStatus = this.db.prepare(`
      SELECT status, COUNT(*) AS count FROM jobs GROUP BY status
    `).all();
    const queues = this.db.prepare(`
      SELECT
        queues.name,
        queues.concurrency,
        queues.max_backlog,
        queues.enabled,
        COUNT(jobs.id) AS total,
        SUM(CASE WHEN jobs.status = 'queued' THEN 1 ELSE 0 END) AS queued,
        SUM(CASE WHEN jobs.status = 'scheduled' THEN 1 ELSE 0 END) AS scheduled,
        SUM(CASE WHEN jobs.status = 'running' THEN 1 ELSE 0 END) AS running,
        SUM(CASE WHEN jobs.status = 'completed' THEN 1 ELSE 0 END) AS completed,
        SUM(CASE WHEN jobs.status = 'dead_letter' THEN 1 ELSE 0 END) AS deadLetter
      FROM queues
      LEFT JOIN jobs ON jobs.queue = queues.name
      GROUP BY queues.name
      ORDER BY queues.name ASC
    `).all();
    const latency = this.db.prepare(`
      SELECT
        AVG((julianday(completed_at) - julianday(created_at)) * 86400000) AS avgEndToEndMs,
        AVG((julianday(started_at) - julianday(created_at)) * 86400000) AS avgQueueWaitMs
      FROM jobs
      WHERE completed_at IS NOT NULL
    `).get();
    const backlogAge = this.db.prepare(`
      SELECT MIN(created_at) AS oldestPendingAt
      FROM jobs
      WHERE status IN ('queued', 'scheduled', 'waiting_dependencies')
    `).get();
    const oldestPendingAgeMs = backlogAge?.oldestPendingAt
      ? Math.max(0, new Date(nowIso()).getTime() - new Date(backlogAge.oldestPendingAt).getTime())
      : 0;

    return {
      generatedAt: nowIso(),
      jobsByStatus: Object.fromEntries(jobsByStatus.map((row) => [row.status, Number(row.count)])),
      queues: queues.map((row) => ({
        name: row.name,
        concurrency: row.concurrency,
        maxBacklog: row.max_backlog,
        enabled: Boolean(row.enabled),
        total: Number(row.total || 0),
        queued: Number(row.queued || 0),
        scheduled: Number(row.scheduled || 0),
        running: Number(row.running || 0),
        completed: Number(row.completed || 0),
        deadLetter: Number(row.deadLetter || 0),
      })),
      latency: {
        avgEndToEndMs: round(Number(latency?.avgEndToEndMs || 0), 2),
        avgQueueWaitMs: round(Number(latency?.avgQueueWaitMs || 0), 2),
        oldestPendingAgeMs: round(oldestPendingAgeMs, 2),
      },
      workers: this.listWorkers(),
      schedules: this.listSchedules(),
      rateLimits: this.listRateLimits(),
    };
  }

  getPrometheusMetrics() {
    const metrics = this.getMetrics();
    const lines = [
      "# HELP jobq_jobs_total Number of jobs by status.",
      "# TYPE jobq_jobs_total gauge",
      ...Object.entries(metrics.jobsByStatus).map(([status, count]) => `jobq_jobs_total{status="${escapeMetricLabel(status)}"} ${count}`),
      "# HELP jobq_queue_jobs Number of jobs per queue and status.",
      "# TYPE jobq_queue_jobs gauge",
      ...metrics.queues.flatMap((queue) => [
        `jobq_queue_jobs{queue="${escapeMetricLabel(queue.name)}",status="queued"} ${queue.queued}`,
        `jobq_queue_jobs{queue="${escapeMetricLabel(queue.name)}",status="scheduled"} ${queue.scheduled}`,
        `jobq_queue_jobs{queue="${escapeMetricLabel(queue.name)}",status="running"} ${queue.running}`,
        `jobq_queue_jobs{queue="${escapeMetricLabel(queue.name)}",status="completed"} ${queue.completed}`,
        `jobq_queue_jobs{queue="${escapeMetricLabel(queue.name)}",status="dead_letter"} ${queue.deadLetter}`,
      ]),
      "# HELP jobq_workers_online Number of online workers.",
      "# TYPE jobq_workers_online gauge",
      `jobq_workers_online ${metrics.workers.filter((worker) => worker.status === "online").length}`,
      "# HELP jobq_queue_wait_average_ms Average queue wait time for completed jobs.",
      "# TYPE jobq_queue_wait_average_ms gauge",
      `jobq_queue_wait_average_ms ${metrics.latency.avgQueueWaitMs}`,
      "# HELP jobq_end_to_end_average_ms Average end-to-end completion time for completed jobs.",
      "# TYPE jobq_end_to_end_average_ms gauge",
      `jobq_end_to_end_average_ms ${metrics.latency.avgEndToEndMs}`,
      "# HELP jobq_oldest_pending_age_ms Age of the oldest queued, scheduled, or dependency-waiting job.",
      "# TYPE jobq_oldest_pending_age_ms gauge",
      `jobq_oldest_pending_age_ms ${metrics.latency.oldestPendingAgeMs}`,
      "# HELP jobq_rate_limit_configured Configured enabled dispatch rate limits.",
      "# TYPE jobq_rate_limit_configured gauge",
      ...metrics.rateLimits.map((limit) => (
        `jobq_rate_limit_configured{scope="${escapeMetricLabel(limit.scope)}",target="${escapeMetricLabel(limit.target)}"} ${limit.enabled ? 1 : 0}`
      )),
    ];
    return `${lines.join("\n")}\n`;
  }

  getState() {
    return {
      queues: this.listQueues(),
      jobs: this.listJobs({ limit: 250 }),
      schedules: this.listSchedules(),
      workers: this.listWorkers(),
      metrics: this.getMetrics(),
      rateLimits: this.listRateLimits(),
      autoscale: this.getAutoscaleRecommendation(),
      throughput: this.getThroughputSeries(),
      workerPools: this.getWorkerPools(),
      idempotency: this.getIdempotencyStats(),
      events: this.listEvents({ limit: 50 }),
    };
  }

  getAutoscaleRecommendation(options = {}) {
    return recommendAutoscale({
      metrics: this.getMetrics(),
      workers: this.listWorkers(),
      options,
    });
  }

  listEvents({ limit = 50 } = {}) {
    return this.db.prepare(`
      SELECT * FROM events
      ORDER BY created_at DESC
      LIMIT ?
    `).all(clampInt(limit, 1, 500)).map(mapEvent);
  }

  listJobEvents(jobId, { limit = 100 } = {}) {
    return this.db.prepare(`
      SELECT * FROM events
      WHERE job_id = ?
      ORDER BY created_at ASC
      LIMIT ?
    `).all(jobId, clampInt(limit, 1, 500)).map(mapEvent);
  }

  getThroughputSeries({ bucketMs = 60000, limit = 20 } = {}) {
    const rows = this.db.prepare(`
      SELECT status, completed_at, dead_lettered_at, created_at
      FROM jobs
      WHERE completed_at IS NOT NULL OR dead_lettered_at IS NOT NULL
      ORDER BY COALESCE(completed_at, dead_lettered_at, created_at) DESC
      LIMIT ?
    `).all(clampInt(limit * 100, 1, 5000));
    const buckets = new Map();
    for (const row of rows) {
      const at = new Date(row.completed_at || row.dead_lettered_at || row.created_at).getTime();
      const bucket = Math.floor(at / bucketMs) * bucketMs;
      if (!buckets.has(bucket)) {
        buckets.set(bucket, { bucketStart: new Date(bucket).toISOString(), completed: 0, deadLetter: 0 });
      }
      const item = buckets.get(bucket);
      if (row.status === "completed") {
        item.completed += 1;
      }
      if (row.status === "dead_letter") {
        item.deadLetter += 1;
      }
    }
    return [...buckets.values()]
      .sort((a, b) => new Date(a.bucketStart).getTime() - new Date(b.bucketStart).getTime())
      .slice(-clampInt(limit, 1, 100));
  }

  getWorkerPools() {
    const workers = this.listWorkers();
    const pools = new Map();
    for (const worker of workers) {
      const capabilities = worker.capabilities.length ? worker.capabilities : ["any"];
      for (const queue of worker.queues) {
        for (const capability of capabilities) {
          const key = `${queue}:${capability}`;
          if (!pools.has(key)) {
            pools.set(key, {
              queue,
              capability,
              workers: 0,
              onlineWorkers: 0,
              capacity: 0,
              activeJobs: 0,
              processed: 0,
              failed: 0,
            });
          }
          const pool = pools.get(key);
          pool.workers += 1;
          pool.onlineWorkers += worker.status === "online" ? 1 : 0;
          pool.capacity += worker.concurrency;
          pool.activeJobs += worker.activeJobs;
          pool.processed += worker.processed;
          pool.failed += worker.failed;
        }
      }
    }
    return [...pools.values()].sort((a, b) => `${a.queue}:${a.capability}`.localeCompare(`${b.queue}:${b.capability}`));
  }

  getIdempotencyStats() {
    const hits = this.db.prepare(`
      SELECT COUNT(*) AS count FROM events WHERE type = 'job.idempotency_hit'
    `).get();
    const uniqueKeys = this.db.prepare(`
      SELECT COUNT(*) AS count FROM jobs WHERE idempotency_key IS NOT NULL
    `).get();
    const recentHits = this.db.prepare(`
      SELECT * FROM events
      WHERE type = 'job.idempotency_hit'
      ORDER BY created_at DESC
      LIMIT 10
    `).all().map(mapEvent);
    return {
      savedDuplicateSubmissions: Number(hits?.count || 0),
      trackedIdempotencyKeys: Number(uniqueKeys?.count || 0),
      recentHits,
    };
  }

  addEvent({ jobId = null, scheduleId = null, workerId = null, type, message, metadata = {} }) {
    const at = nowIso();
    this.db.prepare(`
      INSERT INTO events (id, job_id, schedule_id, worker_id, type, message, metadata, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      createId("evt", type),
      jobId,
      scheduleId,
      workerId,
      String(type || "event"),
      String(message || ""),
      JSON.stringify(metadata ?? {}),
      at,
    );
  }

  exportSnapshot() {
    return {
      version: 1,
      exportedAt: nowIso(),
      tables: Object.fromEntries(SNAPSHOT_TABLES.map((table) => [
        table,
        this.db.prepare(`SELECT * FROM ${table}`).all(),
      ])),
    };
  }

  importSnapshot(snapshot, { mode = "merge" } = {}) {
    if (!snapshot?.tables || typeof snapshot.tables !== "object") {
      throw new Error("Snapshot must contain a tables object.");
    }
    const at = nowIso();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      if (mode === "replace") {
        for (const table of [...SNAPSHOT_TABLES].reverse()) {
          this.db.prepare(`DELETE FROM ${table}`).run();
        }
      }
      for (const table of SNAPSHOT_TABLES) {
        const rows = Array.isArray(snapshot.tables[table]) ? snapshot.tables[table] : [];
        const columns = this.db.prepare(`PRAGMA table_info(${table})`).all().map((column) => column.name);
        for (const row of rows) {
          const selectedColumns = columns.filter((column) => Object.hasOwn(row, column));
          if (selectedColumns.length === 0) {
            continue;
          }
          const placeholders = selectedColumns.map(() => "?").join(", ");
          this.db.prepare(`
            INSERT OR REPLACE INTO ${table} (${selectedColumns.join(", ")})
            VALUES (${placeholders})
          `).run(...selectedColumns.map((column) => row[column]));
        }
      }
      this.addEvent({
        type: "snapshot.imported",
        message: `Snapshot imported in ${mode} mode.`,
        metadata: { mode, importedAt: at },
      });
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
    return this.getState();
  }

  seedDemoScenario() {
    const marker = createId("demo", "scenario");
    this.upsertQueue({
      name: "critical",
      concurrency: 2,
      maxBacklog: 1000,
      rateLimitCount: 20,
      rateLimitWindowMs: 60000,
      retryPolicy: { strategy: "exponential", baseMs: 1500, maxDelayMs: 30000, jitterMs: 500 },
    });
    this.upsertQueue({
      name: "default",
      concurrency: 4,
      maxBacklog: 5000,
      retryPolicy: { strategy: "fixed", baseMs: 1000, maxDelayMs: 5000, jitterMs: 250 },
    });
    this.upsertQueue({ name: "bulk", concurrency: 2, maxBacklog: 10000 });
    this.upsertRateLimit({ scope: "type", target: "webhook.deliver", limitCount: 8, windowMs: 60000 });

    const jobs = [
      this.enqueueJob({
        queue: "critical",
        type: "webhook.deliver",
        priority: 10,
        timeoutMs: 5000,
        requiredCapabilities: ["webhook"],
        idempotencyKey: `${marker}:incident-webhook`,
        payload: { endpoint: "https://example.com/incident", durationMs: 300 },
      }),
      this.enqueueJob({
        queue: "default",
        type: "webhook.deliver",
        priority: 7,
        maxAttempts: 3,
        requiredCapabilities: ["webhook"],
        idempotencyKey: `${marker}:retryable-webhook`,
        payload: { endpoint: "https://example.com/retry", failUntilAttempt: 1, durationMs: 200 },
      }),
      this.enqueueJob({
        queue: "default",
        type: "email.digest",
        priority: 2,
        requiredCapabilities: ["email"],
        idempotencyKey: `${marker}:daily-email`,
        payload: { recipient: "ops@example.com", template: "daily-ops", durationMs: 250 },
      }),
    ];
    this.enqueueJob({
      queue: "default",
      type: "email.digest",
      idempotencyKey: `${marker}:daily-email`,
      payload: { recipient: "ops@example.com", durationMs: 250 },
    });

    const schedule = this.createSchedule({
      name: "Demo cron heartbeat",
      queue: "critical",
      type: "webhook.deliver",
      cronExpr: "*/5 * * * *",
      requiredCapabilities: ["webhook"],
      payload: { endpoint: "https://example.com/heartbeat", durationMs: 200 },
    });
    const workflow = this.createWorkflow({
      name: "Demo reporting DAG",
      jobs: [
        { key: "extract", queue: "bulk", type: "report.generate", requiredCapabilities: ["report"], payload: { reportId: "demo-extract", rows: 500, durationMs: 250 } },
        { key: "warm", queue: "default", type: "cache.warm", requiredCapabilities: ["cache"], dependsOn: ["extract"], payload: { keyPattern: "demo:report:*", durationMs: 200 } },
        { key: "notify", queue: "critical", type: "webhook.deliver", requiredCapabilities: ["webhook"], dependsOn: ["warm"], payload: { endpoint: "https://example.com/report-ready", durationMs: 200 } },
      ],
    });
    this.addEvent({
      type: "demo.seeded",
      message: "Demo scenario seeded.",
      metadata: { marker, jobs: jobs.map((job) => job.id), schedule: schedule.id, workflow: workflow.id },
    });
    return { marker, jobs, schedule, workflow };
  }

  injectFailureScenario(kind = "retryable") {
    const scenario = String(kind || "retryable");
    if (scenario === "timeout") {
      return {
        scenario,
        jobs: [this.enqueueJob({
          queue: "critical",
          type: "webhook.deliver",
          maxAttempts: 1,
          timeoutMs: 10,
          requiredCapabilities: ["webhook"],
          payload: { endpoint: "https://example.com/slow", durationMs: 1000 },
        })],
      };
    }
    if (scenario === "permanent") {
      return {
        scenario,
        jobs: [this.enqueueJob({
          queue: "default",
          type: "webhook.deliver",
          maxAttempts: 1,
          requiredCapabilities: ["webhook"],
          payload: { endpoint: "https://example.com/fail", failAlways: true, error: "Injected permanent upstream 503", durationMs: 100 },
        })],
      };
    }
    if (scenario === "rate_burst") {
      this.upsertRateLimit({ scope: "type", target: "webhook.deliver", limitCount: 1, windowMs: 60000 });
      const jobs = Array.from({ length: 5 }, (_, index) => this.enqueueJob({
        queue: "critical",
        type: "webhook.deliver",
        priority: 5 - index,
        requiredCapabilities: ["webhook"],
        payload: { endpoint: `https://example.com/burst/${index}`, durationMs: 100 },
      }));
      return { scenario, jobs };
    }
    if (scenario === "dependency") {
      const workflow = this.createWorkflow({
        name: "Injected dependency failure",
        jobs: [
          { key: "parent", queue: "default", type: "webhook.deliver", maxAttempts: 1, requiredCapabilities: ["webhook"], payload: { failAlways: true, error: "Injected parent failure", durationMs: 100 } },
          { key: "child", queue: "critical", type: "email.digest", requiredCapabilities: ["email"], dependsOn: ["parent"], payload: { recipient: "ops@example.com", durationMs: 100 } },
        ],
      });
      return { scenario, workflow, jobs: workflow.jobs };
    }
    return {
      scenario: "retryable",
      jobs: [this.enqueueJob({
        queue: "default",
        type: "webhook.deliver",
        maxAttempts: 3,
        requiredCapabilities: ["webhook"],
        payload: { endpoint: "https://example.com/retry", failUntilAttempt: 1, error: "Injected retryable upstream error", durationMs: 100 },
      })],
    };
  }

  vacuumDatabase() {
    const beforeBytes = dbFileSize(this.dbPath);
    this.db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
    this.db.exec("VACUUM");
    const afterBytes = dbFileSize(this.dbPath);
    this.addEvent({
      type: "maintenance.vacuumed",
      message: "Database vacuum completed.",
      metadata: { beforeBytes, afterBytes },
    });
    return { beforeBytes, afterBytes, reclaimedBytes: Math.max(0, beforeBytes - afterBytes) };
  }

  pruneEvents({ olderThanDays = 7, keepLast = 200 } = {}) {
    const cutoff = addMs(nowIso(), -clampInt(olderThanDays, 1, 3650) * 24 * 60 * 60 * 1000);
    const result = this.db.prepare(`
      DELETE FROM events
      WHERE created_at < ?
        AND id NOT IN (
          SELECT id FROM events ORDER BY created_at DESC LIMIT ?
        )
    `).run(cutoff, clampInt(keepLast, 0, 10000));
    this.addEvent({
      type: "maintenance.events_pruned",
      message: `Pruned ${result.changes} old events.`,
      metadata: { olderThanDays, keepLast, cutoff },
    });
    return { pruned: result.changes, cutoff };
  }

  archiveTerminalJobs({ olderThanDays = 7, limit = 500 } = {}) {
    const at = nowIso();
    const cutoff = addMs(at, -clampInt(olderThanDays, 0, 3650) * 24 * 60 * 60 * 1000);
    const rows = this.db.prepare(`
      SELECT * FROM jobs
      WHERE status IN ('completed', 'cancelled', 'dead_letter')
        AND COALESCE(completed_at, cancelled_at, dead_lettered_at, updated_at) <= ?
      ORDER BY updated_at ASC
      LIMIT ?
    `).all(cutoff, clampInt(limit, 1, 10000));
    this.db.exec("BEGIN IMMEDIATE");
    try {
      for (const row of rows) {
        this.db.prepare(`
          INSERT OR REPLACE INTO job_archive (id, job_json, archived_at)
          VALUES (?, ?, ?)
        `).run(row.id, JSON.stringify(mapJob(row)), at);
        this.db.prepare("DELETE FROM jobs WHERE id = ?").run(row.id);
      }
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
    this.addEvent({
      type: "maintenance.jobs_archived",
      message: `Archived ${rows.length} terminal jobs.`,
      metadata: { olderThanDays, limit, cutoff },
    });
    return { archived: rows.length, cutoff };
  }

  ensureQueue(queue) {
    if (!this.db.prepare("SELECT name FROM queues WHERE name = ?").get(queue)) {
      this.upsertQueue({ name: queue });
    }
  }
}

export class NotFoundError extends Error {
  constructor(message) {
    super(message);
    this.name = "NotFoundError";
    this.statusCode = 404;
  }
}

function mapQueue(row) {
  return {
    id: row.id,
    name: row.name,
    concurrency: row.concurrency,
    maxBacklog: row.max_backlog,
    rateLimitCount: row.rate_limit_count,
    rateLimitWindowMs: row.rate_limit_window_ms,
    retryPolicy: parseJson(row.retry_policy, null),
    enabled: Boolean(row.enabled),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapJob(row) {
  return {
    id: row.id,
    queue: row.queue,
    type: row.type,
    payload: parseJson(row.payload, {}),
    status: row.status,
    priority: row.priority,
    attempts: row.attempts,
    maxAttempts: row.max_attempts,
    runAt: row.run_at,
    leaseOwner: row.lease_owner,
    leaseExpiresAt: row.lease_expires_at,
    sourceScheduleId: row.source_schedule_id,
    idempotencyKey: row.idempotency_key,
    lastError: row.last_error,
    output: parseJson(row.output, null),
    dependsOn: parseJson(row.depends_on, []),
    workflowId: row.workflow_id,
    blockedReason: row.blocked_reason,
    schemaErrors: parseJson(row.schema_errors, []),
    timeoutMs: row.timeout_ms,
    requiredCapabilities: parseJson(row.required_capabilities, []),
    retryPolicy: parseJson(row.retry_policy, null),
    failureHint: parseJson(row.failure_hint, null),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    cancelledAt: row.cancelled_at,
    deadLetteredAt: row.dead_lettered_at,
  };
}

function mapSchedule(row) {
  return {
    id: row.id,
    name: row.name,
    queue: row.queue,
    type: row.type,
    payload: parseJson(row.payload, {}),
    scheduleKind: row.schedule_kind,
    cronExpr: row.cron_expr,
    intervalMs: row.interval_ms,
    enabled: Boolean(row.enabled),
    priority: row.priority,
    maxAttempts: row.max_attempts,
    timeoutMs: row.timeout_ms,
    requiredCapabilities: parseJson(row.required_capabilities, []),
    retryPolicy: parseJson(row.retry_policy, null),
    nextRunAt: row.next_run_at,
    lastRunAt: row.last_run_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapWorker(row) {
  return {
    id: row.id,
    queues: parseJson(row.queues, []),
    capabilities: parseJson(row.capabilities, []),
    concurrency: row.concurrency,
    status: row.status,
    activeJobs: row.active_jobs,
    processed: row.processed,
    failed: row.failed,
    startedAt: row.started_at,
    heartbeatAt: row.heartbeat_at,
    updatedAt: row.updated_at,
  };
}

function mapEvent(row) {
  return {
    id: row.id,
    jobId: row.job_id,
    scheduleId: row.schedule_id,
    workerId: row.worker_id,
    type: row.type,
    message: row.message,
    metadata: parseJson(row.metadata, {}),
    createdAt: row.created_at,
  };
}

function mapRateLimit(row) {
  return {
    id: row.id,
    scope: row.scope,
    target: row.target,
    limitCount: row.limit_count,
    windowMs: row.window_ms,
    enabled: Boolean(row.enabled),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function parseJson(value, fallback) {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function normalizePayload(value) {
  if (value === undefined || value === null || value === "") {
    return {};
  }
  if (typeof value === "string") {
    return parseJson(value, { value });
  }
  return value;
}

function normalizeName(value) {
  return String(value || "default").trim().toLowerCase().replace(/[^a-z0-9_.-]+/g, "-") || "default";
}

function normalizeIdList(value) {
  if (value === undefined || value === null || value === "") {
    return [];
  }
  if (Array.isArray(value)) {
    return value.map((item) => String(item).trim()).filter(Boolean);
  }
  return String(value).split(",").map((item) => item.trim()).filter(Boolean);
}

function hasRequiredCapabilities(required, available) {
  const availableSet = new Set(normalizeIdList(available).map((item) => item.toLowerCase()));
  return normalizeIdList(required).every((item) => availableSet.has(item.toLowerCase()));
}

function boolInt(value) {
  return value === false || value === "false" || value === 0 || value === "0" ? 0 : 1;
}

function emptyToNull(value) {
  const normalized = String(value ?? "").trim();
  return normalized ? normalized : null;
}

function clampInt(value, min, max) {
  const number = Number.parseInt(value, 10);
  if (!Number.isFinite(number)) {
    return min;
  }
  return Math.min(max, Math.max(min, number));
}

function optionalInt(value, min, max) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  return clampInt(value, min, max);
}

function normalizeRetryPolicy(value) {
  const policy = typeof value === "string" ? parseJson(value, null) : value;
  if (!policy || typeof policy !== "object" || Array.isArray(policy)) {
    return null;
  }
  const strategy = ["fixed", "exponential"].includes(policy.strategy) ? policy.strategy : "exponential";
  return {
    strategy,
    baseMs: clampInt(policy.baseMs ?? policy.base_ms ?? 2000, 250, 3_600_000),
    maxDelayMs: clampInt(policy.maxDelayMs ?? policy.max_delay_ms ?? 300000, 250, 86_400_000),
    jitterMs: clampInt(policy.jitterMs ?? policy.jitter_ms ?? 0, 0, 3_600_000),
  };
}

function computeRetryDelayMs(policy, attempts) {
  const normalized = normalizeRetryPolicy(policy) ?? {
    strategy: "exponential",
    baseMs: 2000,
    maxDelayMs: 300000,
    jitterMs: 0,
  };
  const base = normalized.strategy === "fixed"
    ? normalized.baseMs
    : normalized.baseMs * (2 ** Math.max(0, Number(attempts) - 1));
  const jitter = normalized.jitterMs > 0 ? Math.floor(Math.random() * normalized.jitterMs) : 0;
  return Math.min(normalized.maxDelayMs, base + jitter);
}

function round(value, places) {
  const factor = 10 ** places;
  return Math.round(value * factor) / factor;
}

function escapeMetricLabel(value) {
  return String(value ?? "").replaceAll("\\", "\\\\").replaceAll("\"", "\\\"");
}

function dbFileSize(dbPath) {
  try {
    return statSync(dbPath).size;
  } catch {
    return 0;
  }
}

export function defaultDbPath() {
  return path.resolve(process.env.JOBQ_DB || "data/jobq.sqlite");
}

export function databaseExists(dbPath = defaultDbPath()) {
  return existsSync(dbPath);
}
