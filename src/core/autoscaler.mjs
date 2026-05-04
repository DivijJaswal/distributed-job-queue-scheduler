export function recommendAutoscale({ metrics, workers = [], options = {} }) {
  const minWorkers = clamp(options.minWorkers ?? 1, 0, 100);
  const maxWorkers = clamp(options.maxWorkers ?? 8, minWorkers, 100);
  const targetJobsPerWorker = clamp(options.targetJobsPerWorker ?? 8, 1, 1000);
  const targetQueueAgeMs = clamp(options.targetQueueAgeMs ?? 30000, 1000, 86_400_000);
  const onlineWorkers = workers.filter((worker) => worker.status === "online");
  const activeWorkers = workers.filter((worker) => ["online", "draining"].includes(worker.status));
  const queueStats = metrics?.queues ?? [];
  const pendingJobs = queueStats.reduce((sum, queue) => sum + queue.queued + queue.scheduled, 0);
  const runningJobs = queueStats.reduce((sum, queue) => sum + queue.running, 0);
  const deadLetters = queueStats.reduce((sum, queue) => sum + queue.deadLetter, 0);
  const oldestPendingAgeMs = Number(metrics?.latency?.oldestPendingAgeMs || 0);

  let desiredWorkers = Math.ceil((pendingJobs + runningJobs) / targetJobsPerWorker);
  if (oldestPendingAgeMs > targetQueueAgeMs) {
    desiredWorkers += Math.ceil(oldestPendingAgeMs / targetQueueAgeMs) - 1;
  }
  if (pendingJobs > 0 && desiredWorkers === 0) {
    desiredWorkers = 1;
  }
  desiredWorkers = clamp(desiredWorkers, minWorkers, maxWorkers);

  const currentWorkers = activeWorkers.length;
  const delta = desiredWorkers - currentWorkers;
  const action = delta > 0 ? "scale_up" : delta < 0 ? "scale_down" : "steady";
  const reasons = [];
  reasons.push(`${pendingJobs} pending jobs and ${runningJobs} running jobs.`);
  reasons.push(`${onlineWorkers.length} online workers, ${activeWorkers.length} active worker records.`);
  if (oldestPendingAgeMs > 0) {
    reasons.push(`Oldest pending job age is ${Math.round(oldestPendingAgeMs)}ms.`);
  }
  if (deadLetters > 0) {
    reasons.push(`${deadLetters} jobs are in dead letter; autoscaling will not replay them automatically.`);
  }

  return {
    action,
    currentWorkers,
    desiredWorkers,
    delta,
    pendingJobs,
    runningJobs,
    oldestPendingAgeMs,
    targetJobsPerWorker,
    targetQueueAgeMs,
    minWorkers,
    maxWorkers,
    reasons,
    generatedAt: new Date().toISOString(),
  };
}

function clamp(value, min, max) {
  const number = Number.parseInt(value, 10);
  if (!Number.isFinite(number)) {
    return min;
  }
  return Math.max(min, Math.min(max, number));
}
