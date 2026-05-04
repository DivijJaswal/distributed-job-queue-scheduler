export async function executeJob(job) {
  const payload = job.payload ?? {};
  const durationMs = Number(payload.durationMs ?? durationForType(job.type));
  await sleep(durationMs);

  if (payload.failAlways) {
    throw new Error(payload.error || `Simulated permanent failure for ${job.type}`);
  }

  if (payload.failUntilAttempt && Number(job.attempts) <= Number(payload.failUntilAttempt)) {
    throw new Error(payload.error || `Simulated retryable failure on attempt ${job.attempts}`);
  }

  switch (job.type) {
    case "email.digest":
      return {
        action: "email_sent",
        recipient: payload.recipient || "user@example.com",
        template: payload.template || "daily-digest",
      };
    case "report.generate":
      return {
        action: "report_generated",
        reportId: payload.reportId || `report-${job.id.slice(-6)}`,
        rowsProcessed: Number(payload.rows ?? 250),
      };
    case "webhook.deliver":
      return {
        action: "webhook_delivered",
        endpoint: payload.endpoint || "https://example.com/webhook",
        statusCode: 200,
      };
    case "billing.reconcile":
      return {
        action: "billing_reconciled",
        accountId: payload.accountId || "acct-demo",
        invoiceCount: Number(payload.invoiceCount ?? 12),
      };
    case "cache.warm":
      return {
        action: "cache_warmed",
        keyPattern: payload.keyPattern || "dashboard:*",
      };
    default:
      return {
        action: "generic_completed",
        type: job.type,
      };
  }
}

function durationForType(type) {
  switch (type) {
    case "report.generate":
      return 900;
    case "billing.reconcile":
      return 700;
    case "webhook.deliver":
      return 500;
    case "cache.warm":
      return 350;
    default:
      return 450;
  }
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(ms || 0))));
}
