export function buildOpenApiSpec() {
  return {
    openapi: "3.0.3",
    info: {
      title: "Distributed Job Queue + Scheduler API",
      version: "0.1.0",
      description: "Local-first APIs for queues, jobs, schedules, workers, workflows, rate limits, backup/import, autoscaling recommendations, and metrics.",
    },
    paths: {
      "/health": { get: { summary: "Service health", responses: ok() } },
      "/api/state": { get: { summary: "Dashboard state", responses: ok() } },
      "/api/jobs": {
        get: { summary: "List jobs with filters", responses: ok() },
        post: { summary: "Enqueue a job", responses: created() },
      },
      "/api/jobs/{jobId}/events": { get: { summary: "Job timeline", responses: ok() } },
      "/api/jobs/{jobId}/cancel": { post: { summary: "Cancel a queued, scheduled, or dependency-waiting job", responses: ok() } },
      "/api/jobs/{jobId}/requeue": { post: { summary: "Requeue a terminal job", responses: ok() } },
      "/api/queues": { post: { summary: "Create or update a queue", responses: created() } },
      "/api/queues/{queueName}/toggle": { post: { summary: "Pause or resume queue dispatch", responses: ok() } },
      "/api/rate-limits": {
        get: { summary: "List configured rate limits", responses: ok() },
        post: { summary: "Create or update a type/queue dispatch rate limit", responses: created() },
      },
      "/api/schedules": { post: { summary: "Create an interval or cron schedule", responses: created() } },
      "/api/schedules/{scheduleId}/toggle": { post: { summary: "Pause or resume a schedule", responses: ok() } },
      "/api/schedules/{scheduleId}/run-now": { post: { summary: "Dispatch a schedule immediately", responses: created() } },
      "/api/workflows": { post: { summary: "Create a DAG workflow", responses: created() } },
      "/api/dead-letter/replay": { post: { summary: "Bulk replay dead-letter jobs", responses: ok() } },
      "/api/recover-leases": { post: { summary: "Recover stale leases and timed-out jobs", responses: ok() } },
      "/api/autoscale/recommendation": { get: { summary: "Compute local worker autoscaling recommendation", responses: ok() } },
      "/api/charts/throughput": { get: { summary: "Completed/dead-letter throughput buckets", responses: ok() } },
      "/api/worker-pools": { get: { summary: "Worker capacity grouped by queue and capability", responses: ok() } },
      "/api/idempotency": { get: { summary: "Duplicate idempotency hit statistics", responses: ok() } },
      "/api/demo/seed": { post: { summary: "Seed a full dashboard demo scenario", responses: created() } },
      "/api/failures/inject": { post: { summary: "Inject timeout, retryable, permanent, rate-limit, or dependency failure scenarios", responses: created() } },
      "/api/maintenance/vacuum": { post: { summary: "Run SQLite checkpoint and vacuum", responses: ok() } },
      "/api/maintenance/prune-events": { post: { summary: "Prune old audit events while keeping recent entries", responses: ok() } },
      "/api/maintenance/archive-jobs": { post: { summary: "Archive old terminal jobs into job_archive", responses: ok() } },
      "/api/export": { get: { summary: "Export a JSON snapshot of queue state", responses: ok() } },
      "/api/import": { post: { summary: "Import a JSON snapshot in merge or replace mode", responses: ok() } },
      "/api/metrics": { get: { summary: "JSON metrics", responses: ok() } },
      "/metrics": { get: { summary: "Prometheus text metrics", responses: okText() } },
      "/openapi.json": { get: { summary: "OpenAPI specification", responses: ok() } },
    },
  };
}

function ok() {
  return {
    200: {
      description: "OK",
      content: { "application/json": { schema: { type: "object" } } },
    },
  };
}

function created() {
  return {
    201: {
      description: "Created",
      content: { "application/json": { schema: { type: "object" } } },
    },
  };
}

function okText() {
  return {
    200: {
      description: "OK",
      content: { "text/plain": { schema: { type: "string" } } },
    },
  };
}
