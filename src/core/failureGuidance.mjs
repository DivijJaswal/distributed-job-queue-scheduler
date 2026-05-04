const RULES = [
  {
    pattern: /timed out|timeout/i,
    category: "timeout",
    cause: "The job exceeded its configured execution deadline.",
    action: "Increase timeoutMs only if the job is expected to be long-running; otherwise inspect downstream latency and split the job into smaller units.",
  },
  {
    pattern: /lease expired|drained out|worker stopped/i,
    category: "worker_lifecycle",
    cause: "The worker lost ownership before the job reached a terminal state.",
    action: "Check worker shutdowns, heartbeat cadence, leaseMs, and whether the worker process is being terminated during active work.",
  },
  {
    pattern: /payload validation|schema|invalid json/i,
    category: "schema",
    cause: "The submitted payload does not match the expected job contract.",
    action: "Fix the payload shape or update the schema for this job type before replaying.",
  },
  {
    pattern: /429|rate limit|too many requests/i,
    category: "rate_limit",
    cause: "The target system or queue policy throttled dispatch.",
    action: "Reduce dispatch rate, widen the rate-limit window, or replay after the current window expires.",
  },
  {
    pattern: /econn|network|upstream|webhook|503|502|504/i,
    category: "dependency",
    cause: "An external dependency was unavailable or returned an error.",
    action: "Validate the target endpoint, retry after dependency recovery, and consider a more conservative retry policy with jitter.",
  },
  {
    pattern: /simulated|failAlways|failUntilAttempt/i,
    category: "demo_failure",
    cause: "The demo payload intentionally triggered a failure path.",
    action: "Remove failAlways/failUntilAttempt from the payload or requeue to demonstrate recovery behavior.",
  },
];

export function buildFailureGuidance(error, job = {}) {
  const message = String(error?.message || error || "");
  const rule = RULES.find((item) => item.pattern.test(message)) ?? {
    category: "unknown",
    cause: "The worker reported an unclassified error.",
    action: "Review the job timeline, payload, worker logs, and retry policy before replaying from dead letter.",
  };

  return {
    category: rule.category,
    likelyCause: rule.cause,
    suggestedAction: rule.action,
    jobType: job.type,
    queue: job.queue,
    generatedAt: new Date().toISOString(),
  };
}
