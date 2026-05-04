let state = null;
let refreshTimer = null;
let jobFilters = {};

const elements = {
  healthPill: document.querySelector("#healthPill"),
  seedDemo: document.querySelector("#seedDemo"),
  refreshState: document.querySelector("#refreshState"),
  throughputChart: document.querySelector("#throughputChart"),
  metricQueued: document.querySelector("#metricQueued"),
  metricRunning: document.querySelector("#metricRunning"),
  metricCompleted: document.querySelector("#metricCompleted"),
  metricDeadLetter: document.querySelector("#metricDeadLetter"),
  metricWorkers: document.querySelector("#metricWorkers"),
  queueForm: document.querySelector("#queueForm"),
  queuesList: document.querySelector("#queuesList"),
  rateLimitForm: document.querySelector("#rateLimitForm"),
  rateLimitsList: document.querySelector("#rateLimitsList"),
  jobForm: document.querySelector("#jobForm"),
  jobQueue: document.querySelector("#jobQueue"),
  jobFilterForm: document.querySelector("#jobFilterForm"),
  resetJobFilters: document.querySelector("#resetJobFilters"),
  scheduleForm: document.querySelector("#scheduleForm"),
  scheduleQueue: document.querySelector("#scheduleQueue"),
  schedulesList: document.querySelector("#schedulesList"),
  workflowForm: document.querySelector("#workflowForm"),
  failureForm: document.querySelector("#failureForm"),
  runSchedulerTick: document.querySelector("#runSchedulerTick"),
  recoverLeases: document.querySelector("#recoverLeases"),
  replayDeadLetters: document.querySelector("#replayDeadLetters"),
  jobsList: document.querySelector("#jobsList"),
  workersList: document.querySelector("#workersList"),
  workerPoolsList: document.querySelector("#workerPoolsList"),
  idempotencyPanel: document.querySelector("#idempotencyPanel"),
  autoscalePanel: document.querySelector("#autoscalePanel"),
  refreshAutoscale: document.querySelector("#refreshAutoscale"),
  queueHealth: document.querySelector("#queueHealth"),
  eventsList: document.querySelector("#eventsList"),
  exportSnapshot: document.querySelector("#exportSnapshot"),
  importSnapshotForm: document.querySelector("#importSnapshotForm"),
  importSnapshotText: document.querySelector("#importSnapshotText"),
  vacuumDatabase: document.querySelector("#vacuumDatabase"),
  pruneEvents: document.querySelector("#pruneEvents"),
  archiveJobs: document.querySelector("#archiveJobs"),
  drawerBackdrop: document.querySelector("#drawerBackdrop"),
  jobDrawer: document.querySelector("#jobDrawer"),
  jobDrawerTitle: document.querySelector("#jobDrawerTitle"),
  jobDrawerBody: document.querySelector("#jobDrawerBody"),
  closeJobDrawer: document.querySelector("#closeJobDrawer"),
  toast: document.querySelector("#toast"),
};

const defaultJobPayload = {
  recipient: "ops@example.com",
  durationMs: 500,
};

const defaultSchedulePayload = {
  endpoint: "https://example.com/heartbeat",
  durationMs: 300,
};

const defaultWorkflowJobs = [
  {
    key: "extract",
    queue: "bulk",
    type: "report.generate",
    payload: { reportId: "daily-extract", rows: 1000, durationMs: 600 },
  },
  {
    key: "transform",
    queue: "default",
    type: "cache.warm",
    dependsOn: ["extract"],
    payload: { keyPattern: "etl:daily:*", durationMs: 300 },
  },
  {
    key: "notify",
    queue: "critical",
    type: "webhook.deliver",
    dependsOn: ["transform"],
    payload: { endpoint: "https://example.com/etl-complete", durationMs: 250 },
  },
];

boot();

async function boot() {
  bindEvents();
  seedForms();
  await checkHealth();
  await loadState();
  connectLiveUpdates();
  refreshTimer = setInterval(loadState, 10000);
}

function bindEvents() {
  elements.seedDemo.addEventListener("click", async () => {
    await withBusy(elements.seedDemo, "Seeding", async () => {
      const response = await api("/api/demo/seed", { method: "POST" });
      toast(`Demo seeded: ${response.demo.jobs.length} jobs, 1 schedule, 1 workflow.`);
      await loadState();
    });
  });

  elements.refreshState.addEventListener("click", async () => {
    await withBusy(elements.refreshState, "Refreshing", loadState);
  });

  elements.queueForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    await withBusy(event.submitter, "Saving", async () => {
      const form = new FormData(elements.queueForm);
      await api("/api/queues", {
        method: "POST",
        body: {
          name: form.get("name"),
          concurrency: Number(form.get("concurrency") || 4),
          maxBacklog: Number(form.get("maxBacklog") || 10000),
          rateLimitCount: nullableNumber(form.get("rateLimitCount")),
          rateLimitWindowMs: nullableNumber(form.get("rateLimitWindowSeconds")) ? Number(form.get("rateLimitWindowSeconds")) * 1000 : null,
          retryPolicy: parseOptionalJsonField(form.get("retryPolicy"), "Queue retry policy JSON"),
          enabled: form.get("enabled") === "on",
        },
      });
      toast("Queue saved.");
      await loadState();
    });
  });

  elements.rateLimitForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    await withBusy(event.submitter, "Saving", async () => {
      const form = new FormData(elements.rateLimitForm);
      await api("/api/rate-limits", {
        method: "POST",
        body: {
          scope: "type",
          target: form.get("target"),
          limitCount: Number(form.get("limitCount") || 10),
          windowMs: Number(form.get("windowSeconds") || 60) * 1000,
          enabled: form.get("enabled") === "on",
        },
      });
      toast("Rate limit saved.");
      await loadState();
    });
  });

  elements.jobForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    await withBusy(event.submitter, "Enqueueing", async () => {
      const form = new FormData(elements.jobForm);
      await api("/api/jobs", {
        method: "POST",
        body: {
          queue: form.get("queue"),
          type: form.get("type"),
          priority: Number(form.get("priority") || 0),
          maxAttempts: Number(form.get("maxAttempts") || 3),
          runAt: localDateTimeToIso(form.get("runAt")),
          idempotencyKey: nullable(form.get("idempotencyKey")),
          dependsOn: splitList(form.get("dependsOn")),
          timeoutMs: nullableNumber(form.get("timeoutMs")),
          requiredCapabilities: splitList(form.get("requiredCapabilities")),
          retryPolicy: parseOptionalJsonField(form.get("retryPolicy"), "Retry policy JSON"),
          payload: parseJsonField(form.get("payload"), "Payload JSON"),
        },
      });
      toast("Job enqueued.");
      await loadState();
    });
  });

  elements.scheduleForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    await withBusy(event.submitter, "Creating", async () => {
      const form = new FormData(elements.scheduleForm);
      await api("/api/schedules", {
        method: "POST",
        body: {
          name: form.get("name"),
          queue: form.get("queue"),
          type: form.get("type"),
          scheduleKind: form.get("scheduleKind"),
          cronExpr: nullable(form.get("cronExpr")),
          intervalMs: Number(form.get("intervalSeconds") || 30) * 1000,
          priority: Number(form.get("priority") || 0),
          maxAttempts: Number(form.get("maxAttempts") || 3),
          timeoutMs: nullableNumber(form.get("timeoutMs")),
          requiredCapabilities: splitList(form.get("requiredCapabilities")),
          retryPolicy: parseOptionalJsonField(form.get("retryPolicy"), "Schedule retry policy JSON"),
          payload: parseJsonField(form.get("payload"), "Payload JSON"),
        },
      });
      elements.scheduleForm.reset();
      seedForms();
      toast("Schedule created.");
      await loadState();
    });
  });

  elements.workflowForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    await withBusy(event.submitter, "Creating", async () => {
      const form = new FormData(elements.workflowForm);
      const response = await api("/api/workflows", {
        method: "POST",
        body: {
          name: form.get("name"),
          jobs: parseJsonField(form.get("jobs"), "Jobs JSON"),
        },
      });
      toast(`Workflow created with ${response.workflow.jobs.length} jobs.`);
      await loadState();
    });
  });

  elements.failureForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    await withBusy(event.submitter, "Injecting", async () => {
      const form = new FormData(elements.failureForm);
      const response = await api("/api/failures/inject", {
        method: "POST",
        body: { scenario: form.get("scenario") },
      });
      toast(`Injected ${response.scenario.scenario} scenario.`);
      await loadState();
    });
  });

  elements.runSchedulerTick.addEventListener("click", async () => {
    await withBusy(elements.runSchedulerTick, "Dispatching", async () => {
      const response = await api("/api/scheduler/tick", { method: "POST" });
      toast(`Scheduler dispatched ${response.dispatched.length} jobs.`);
      await loadState();
    });
  });

  elements.recoverLeases.addEventListener("click", async () => {
    await withBusy(elements.recoverLeases, "Recovering", async () => {
      const response = await api("/api/recover-leases", { method: "POST" });
      toast(`Recovered ${response.recovered.length} expired leases.`);
      await loadState();
    });
  });

  elements.replayDeadLetters.addEventListener("click", async () => {
    await withBusy(elements.replayDeadLetters, "Replaying", async () => {
      const response = await api("/api/dead-letter/replay", {
        method: "POST",
        body: { limit: 100 },
      });
      toast(`Requeued ${response.requeued.length} dead-letter jobs.`);
      await loadState();
    });
  });

  elements.jobFilterForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(elements.jobFilterForm);
    jobFilters = {
      q: nullable(form.get("q")),
      status: nullable(form.get("status")),
      queue: nullable(form.get("queue")),
      type: nullable(form.get("type")),
    };
    renderJobs();
  });

  elements.resetJobFilters.addEventListener("click", () => {
    elements.jobFilterForm.reset();
    jobFilters = {};
    renderJobs();
  });

  elements.refreshAutoscale.addEventListener("click", async () => {
    await withBusy(elements.refreshAutoscale, "Refreshing", async () => {
      state.autoscale = await api("/api/autoscale/recommendation");
      renderAutoscale();
    });
  });

  elements.exportSnapshot.addEventListener("click", async () => {
    await withBusy(elements.exportSnapshot, "Exporting", async () => {
      const snapshot = await api("/api/export");
      elements.importSnapshotText.value = JSON.stringify(snapshot, null, 2);
      await navigator.clipboard?.writeText(elements.importSnapshotText.value).catch(() => {});
      toast("Snapshot exported to the import box.");
    });
  });

  elements.importSnapshotForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    await withBusy(event.submitter, "Importing", async () => {
      const snapshot = parseJsonField(elements.importSnapshotText.value, "Snapshot JSON");
      await api("/api/import", {
        method: "POST",
        body: { snapshot, mode: "merge" },
      });
      toast("Snapshot imported.");
      await loadState();
    });
  });

  elements.vacuumDatabase.addEventListener("click", async () => {
    await withBusy(elements.vacuumDatabase, "Vacuuming", async () => {
      const response = await api("/api/maintenance/vacuum", { method: "POST" });
      toast(`Vacuum complete. Reclaimed ${response.result.reclaimedBytes} bytes.`);
      await loadState();
    });
  });

  elements.pruneEvents.addEventListener("click", async () => {
    await withBusy(elements.pruneEvents, "Pruning", async () => {
      const response = await api("/api/maintenance/prune-events", {
        method: "POST",
        body: { olderThanDays: 7, keepLast: 200 },
      });
      toast(`Pruned ${response.result.pruned} events.`);
      await loadState();
    });
  });

  elements.archiveJobs.addEventListener("click", async () => {
    await withBusy(elements.archiveJobs, "Archiving", async () => {
      const response = await api("/api/maintenance/archive-jobs", {
        method: "POST",
        body: { olderThanDays: 7, limit: 500 },
      });
      toast(`Archived ${response.result.archived} terminal jobs.`);
      await loadState();
    });
  });

  elements.closeJobDrawer.addEventListener("click", closeJobDrawer);
  elements.drawerBackdrop.addEventListener("click", closeJobDrawer);
}

function seedForms() {
  if (!elements.jobForm.elements.payload.value.trim()) {
    elements.jobForm.elements.payload.value = JSON.stringify(defaultJobPayload, null, 2);
  }
  if (!elements.scheduleForm.elements.payload.value.trim()) {
    elements.scheduleForm.elements.payload.value = JSON.stringify(defaultSchedulePayload, null, 2);
  }
  if (!elements.workflowForm.elements.jobs.value.trim()) {
    elements.workflowForm.elements.jobs.value = JSON.stringify(defaultWorkflowJobs, null, 2);
  }
}

function connectLiveUpdates() {
  if (!window.EventSource) {
    return;
  }
  const source = new EventSource("/api/events/stream");
  source.addEventListener("state", (event) => {
    state = JSON.parse(event.data);
    render();
  });
  source.addEventListener("error", () => {
    source.close();
  });
}

async function checkHealth() {
  try {
    const health = await api("/health");
    elements.healthPill.textContent = `${health.service}: ${health.status}`;
    elements.healthPill.classList.remove("pill--muted");
  } catch {
    elements.healthPill.textContent = "Server unavailable";
    elements.healthPill.classList.add("pill--muted");
  }
}

async function loadState() {
  state = await api("/api/state");
  render();
}

function render() {
  renderMetrics();
  renderThroughputChart();
  renderSelectors();
  renderQueues();
  renderRateLimits();
  renderQueueHealth();
  renderSchedules();
  renderWorkers();
  renderWorkerPools();
  renderIdempotency();
  renderAutoscale();
  renderJobs();
  renderEvents();
}

function renderMetrics() {
  const jobs = state.metrics.jobsByStatus || {};
  elements.metricQueued.textContent = jobs.queued || 0;
  elements.metricRunning.textContent = jobs.running || 0;
  elements.metricCompleted.textContent = jobs.completed || 0;
  elements.metricDeadLetter.textContent = jobs.dead_letter || 0;
  elements.metricWorkers.textContent = state.workers.length;
}

function renderThroughputChart() {
  const series = state.throughput || [];
  if (series.length === 0) {
    elements.throughputChart.innerHTML = `<p class="empty-state">Throughput appears after jobs complete or dead-letter.</p>`;
    return;
  }
  const width = 720;
  const height = 180;
  const padding = 24;
  const maxValue = Math.max(1, ...series.map((item) => item.completed + item.deadLetter));
  const barGap = 6;
  const barWidth = Math.max(10, (width - padding * 2) / series.length - barGap);
  const bars = series.map((item, index) => {
    const x = padding + index * (barWidth + barGap);
    const completedHeight = Math.round((item.completed / maxValue) * (height - padding * 2));
    const deadHeight = Math.round((item.deadLetter / maxValue) * (height - padding * 2));
    const completedY = height - padding - completedHeight;
    const deadY = completedY - deadHeight;
    return `
      <rect x="${x}" y="${completedY}" width="${barWidth}" height="${completedHeight}" rx="3" class="chart-completed"></rect>
      <rect x="${x}" y="${deadY}" width="${barWidth}" height="${deadHeight}" rx="3" class="chart-dead"></rect>
      <text x="${x + barWidth / 2}" y="${height - 6}" text-anchor="middle">${formatTime(item.bucketStart)}</text>
    `;
  }).join("");
  elements.throughputChart.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Completed and dead-letter throughput">
      <line x1="${padding}" y1="${height - padding}" x2="${width - padding}" y2="${height - padding}" class="chart-axis"></line>
      ${bars}
    </svg>
    <div class="chart-legend"><span class="legend-completed"></span>Completed <span class="legend-dead"></span>Dead letter</div>
  `;
}

function renderSelectors() {
  const options = state.queues.map((queue) => `<option value="${escapeAttr(queue.name)}">${escapeHtml(queue.name)}</option>`).join("");
  elements.jobQueue.innerHTML = options;
  elements.scheduleQueue.innerHTML = options;
  const selectedFilterQueue = elements.jobFilterForm.elements.queue.value;
  elements.jobFilterForm.elements.queue.innerHTML = `<option value="">all</option>${options}`;
  elements.jobFilterForm.elements.queue.value = selectedFilterQueue;
}

function renderQueues() {
  elements.queuesList.innerHTML = state.queues.map((queue) => `
    <article class="list-item">
      <div class="result-row">
        <h3>${escapeHtml(queue.name)}</h3>
        <span class="status ${queue.enabled ? "" : "status--muted"}">${queue.enabled ? "enabled" : "paused"}</span>
      </div>
      <p>Concurrency ${queue.concurrency} | Max backlog ${queue.maxBacklog}</p>
      ${queue.rateLimitCount && queue.rateLimitWindowMs ? `<p>Queue rate ${queue.rateLimitCount}/${Math.round(queue.rateLimitWindowMs / 1000)}s</p>` : ""}
      ${queue.retryPolicy ? `<small>Retry ${escapeHtml(queue.retryPolicy.strategy)} base ${queue.retryPolicy.baseMs}ms max ${queue.retryPolicy.maxDelayMs}ms</small>` : ""}
      <small>${queue.enabled ? "Accepting work" : "Paused"} | Created ${formatDate(queue.createdAt)}</small>
      <div class="job-row__actions">
        <button type="button" class="button" data-queue-toggle="${escapeAttr(queue.name)}" data-enabled="${queue.enabled ? "false" : "true"}">${queue.enabled ? "Pause Queue" : "Resume Queue"}</button>
      </div>
    </article>
  `).join("") || `<p class="empty-state">No queues configured.</p>`;

  elements.queuesList.querySelectorAll("[data-queue-toggle]").forEach((button) => {
    button.addEventListener("click", async () => {
      await withBusy(button, "Updating", async () => {
        await api(`/api/queues/${encodeURIComponent(button.dataset.queueToggle)}/toggle`, {
          method: "POST",
          body: { enabled: button.dataset.enabled === "true" },
        });
        toast("Queue updated.");
        await loadState();
      });
    });
  });
}

function renderRateLimits() {
  elements.rateLimitsList.innerHTML = (state.rateLimits || []).map((limit) => `
    <article class="list-item">
      <div class="result-row">
        <h3>${escapeHtml(limit.scope)}:${escapeHtml(limit.target)}</h3>
        <span class="status ${limit.enabled ? "" : "status--muted"}">${limit.enabled ? "enabled" : "disabled"}</span>
      </div>
      <p>${limit.limitCount} dispatches every ${Math.round(limit.windowMs / 1000)}s</p>
    </article>
  `).join("") || `<p class="empty-state">No type-specific rate limits.</p>`;
}

function renderQueueHealth() {
  elements.queueHealth.innerHTML = state.metrics.queues.map((queue) => `
    <article class="list-item">
      <div class="result-row">
        <h3>${escapeHtml(queue.name)}</h3>
        <span class="status ${queue.deadLetter ? "status--danger" : queue.running ? "status--info" : ""}">${queue.total} total</span>
      </div>
      <p>Queued ${queue.queued} | Scheduled ${queue.scheduled} | Running ${queue.running}</p>
      <small>Completed ${queue.completed} | Dead letter ${queue.deadLetter}</small>
    </article>
  `).join("") || `<p class="empty-state">Queue health appears after jobs are created.</p>`;
}

function renderSchedules() {
  elements.schedulesList.innerHTML = state.schedules.map((schedule) => `
    <article class="list-item">
      <div class="result-row">
        <h3>${escapeHtml(schedule.name)}</h3>
        <span class="status ${schedule.enabled ? "" : "status--muted"}">${schedule.enabled ? "active" : "paused"}</span>
      </div>
      <p>${escapeHtml(schedule.type)} on ${escapeHtml(schedule.queue)} ${schedule.scheduleKind === "cron" ? `cron ${escapeHtml(schedule.cronExpr)}` : `every ${Math.round(schedule.intervalMs / 1000)}s`}</p>
      <small>Next run ${formatDate(schedule.nextRunAt)} | Last run ${schedule.lastRunAt ? formatDate(schedule.lastRunAt) : "never"}</small>
      <div class="job-row__actions">
        <button type="button" class="button" data-schedule-toggle="${escapeAttr(schedule.id)}" data-enabled="${schedule.enabled ? "false" : "true"}">${schedule.enabled ? "Pause" : "Resume"}</button>
        <button type="button" class="button" data-schedule-run="${escapeAttr(schedule.id)}">Run Now</button>
      </div>
    </article>
  `).join("") || `<p class="empty-state">No schedules yet.</p>`;

  elements.schedulesList.querySelectorAll("[data-schedule-toggle]").forEach((button) => {
    button.addEventListener("click", async () => {
      await withBusy(button, "Updating", async () => {
        await api(`/api/schedules/${encodeURIComponent(button.dataset.scheduleToggle)}/toggle`, {
          method: "POST",
          body: { enabled: button.dataset.enabled === "true" },
        });
        toast("Schedule updated.");
        await loadState();
      });
    });
  });

  elements.schedulesList.querySelectorAll("[data-schedule-run]").forEach((button) => {
    button.addEventListener("click", async () => {
      await withBusy(button, "Enqueueing", async () => {
        await api(`/api/schedules/${encodeURIComponent(button.dataset.scheduleRun)}/run-now`, { method: "POST" });
        toast("Scheduled job enqueued.");
        await loadState();
      });
    });
  });
}

function renderWorkers() {
  elements.workersList.innerHTML = state.workers.map((worker) => `
    <article class="list-item">
      <div class="result-row">
        <h3>${escapeHtml(worker.id)}</h3>
        <span class="status ${worker.status === "online" ? "" : "status--muted"}">${escapeHtml(worker.status)}</span>
      </div>
      <p>${worker.activeJobs} active | ${worker.processed} processed | ${worker.failed} failed</p>
      <small>Queues ${worker.queues.map(escapeHtml).join(", ")} | capabilities ${(worker.capabilities || []).map(escapeHtml).join(", ") || "any"} | heartbeat ${formatDate(worker.heartbeatAt)}</small>
    </article>
  `).join("") || `<p class="empty-state">Start a worker with <code>yarn worker</code>.</p>`;
}

function renderWorkerPools() {
  elements.workerPoolsList.innerHTML = (state.workerPools || []).map((pool) => `
    <article class="list-item">
      <div class="result-row">
        <h3>${escapeHtml(pool.queue)} / ${escapeHtml(pool.capability)}</h3>
        <span class="status ${pool.activeJobs >= pool.capacity && pool.capacity > 0 ? "status--warn" : ""}">${pool.activeJobs}/${pool.capacity}</span>
      </div>
      <p>${pool.onlineWorkers}/${pool.workers} online workers | ${pool.processed} processed | ${pool.failed} failed</p>
    </article>
  `).join("") || `<p class="empty-state">Worker pools appear after workers heartbeat.</p>`;
}

function renderIdempotency() {
  const stats = state.idempotency || {};
  elements.idempotencyPanel.innerHTML = `
    <article class="list-item">
      <div class="result-row">
        <h3>${stats.savedDuplicateSubmissions || 0} duplicate submissions saved</h3>
        <span class="status">${stats.trackedIdempotencyKeys || 0} keys</span>
      </div>
      <p>${(stats.recentHits || []).map((event) => escapeHtml(event.metadata?.idempotencyKey || event.jobId)).join(", ") || "No duplicate submissions observed."}</p>
    </article>
  `;
}

function renderAutoscale() {
  const rec = state.autoscale;
  if (!rec) {
    elements.autoscalePanel.innerHTML = `<p class="empty-state">No recommendation yet.</p>`;
    return;
  }
  elements.autoscalePanel.innerHTML = `
    <article class="list-item">
      <div class="result-row">
        <h3>${escapeHtml(rec.action)}</h3>
        <span class="status ${rec.delta > 0 ? "status--info" : rec.delta < 0 ? "status--warn" : ""}">${rec.currentWorkers} -> ${rec.desiredWorkers}</span>
      </div>
      <p>${rec.pendingJobs} pending | ${rec.runningJobs} running | oldest ${Math.round(rec.oldestPendingAgeMs)}ms</p>
      <small>${(rec.reasons || []).map(escapeHtml).join(" | ")}</small>
    </article>
  `;
}

function filteredJobs() {
  const q = String(jobFilters.q || "").toLowerCase();
  return state.jobs.filter((job) => {
    if (jobFilters.status && job.status !== jobFilters.status) {
      return false;
    }
    if (jobFilters.queue && job.queue !== jobFilters.queue) {
      return false;
    }
    if (jobFilters.type && job.type !== jobFilters.type) {
      return false;
    }
    if (q) {
      const haystack = [
        job.id,
        job.type,
        job.queue,
        job.workflowId,
        job.idempotencyKey,
        job.lastError,
        JSON.stringify(job.payload),
      ].filter(Boolean).join(" ").toLowerCase();
      return haystack.includes(q);
    }
    return true;
  });
}

function renderJobs() {
  const jobs = filteredJobs();
  elements.jobsList.innerHTML = jobs.map((job) => `
    <article class="job-row">
      <div class="job-row__meta">
        <div>
          <h3>${escapeHtml(job.type)}</h3>
          <p>${escapeHtml(job.queue)} | priority ${job.priority} | attempts ${job.attempts}/${job.maxAttempts}</p>
        </div>
        <span class="status ${statusClass(job.status)}">${escapeHtml(job.status)}</span>
      </div>
      <small>Run at ${formatDate(job.runAt)}${job.leaseOwner ? ` | lease ${escapeHtml(job.leaseOwner)} until ${formatDate(job.leaseExpiresAt)}` : ""}${job.workflowId ? ` | workflow ${escapeHtml(job.workflowId)}` : ""}${job.timeoutMs ? ` | timeout ${job.timeoutMs}ms` : ""}</small>
      ${job.blockedReason ? `<p class="status status--warn">${escapeHtml(job.blockedReason)}</p>` : ""}
      ${job.dependsOn?.length ? `<p>Depends on ${job.dependsOn.map(escapeHtml).join(", ")}</p>` : ""}
      ${job.requiredCapabilities?.length ? `<p>Requires ${job.requiredCapabilities.map(escapeHtml).join(", ")}</p>` : ""}
      ${job.retryPolicy ? `<p>Retry ${escapeHtml(job.retryPolicy.strategy)} base ${job.retryPolicy.baseMs}ms max ${job.retryPolicy.maxDelayMs}ms jitter ${job.retryPolicy.jitterMs}ms</p>` : ""}
      ${job.schemaErrors?.length ? `<p class="status status--danger">${job.schemaErrors.map(escapeHtml).join(" ")}</p>` : ""}
      ${job.lastError ? `<p class="status status--danger">${escapeHtml(job.lastError)}</p>` : ""}
      ${job.failureHint ? `<p class="status status--info">${escapeHtml(job.failureHint.likelyCause)} ${escapeHtml(job.failureHint.suggestedAction)}</p>` : ""}
      <pre>${escapeHtml(JSON.stringify(job.payload, null, 2))}</pre>
      <div class="job-row__actions">
        <button type="button" class="button" data-job-detail="${escapeAttr(job.id)}">Details</button>
        ${["queued", "scheduled", "waiting_dependencies"].includes(job.status) ? `<button type="button" class="button" data-cancel-job="${escapeAttr(job.id)}">Cancel</button>` : ""}
        ${["completed", "cancelled", "dead_letter"].includes(job.status) ? `<button type="button" class="button" data-requeue-job="${escapeAttr(job.id)}">Requeue</button>` : ""}
        <button type="button" class="button" data-job-events="${escapeAttr(job.id)}">Timeline</button>
      </div>
    </article>
  `).join("") || `<p class="empty-state">No jobs yet. Enqueue one or run <code>yarn seed</code>.</p>`;

  elements.jobsList.querySelectorAll("[data-cancel-job]").forEach((button) => {
    button.addEventListener("click", async () => {
      await withBusy(button, "Cancelling", async () => {
        await api(`/api/jobs/${encodeURIComponent(button.dataset.cancelJob)}/cancel`, { method: "POST" });
        toast("Job cancelled.");
        await loadState();
      });
    });
  });

  elements.jobsList.querySelectorAll("[data-job-detail]").forEach((button) => {
    button.addEventListener("click", async () => {
      await openJobDrawer(button.dataset.jobDetail);
    });
  });

  elements.jobsList.querySelectorAll("[data-requeue-job]").forEach((button) => {
    button.addEventListener("click", async () => {
      await withBusy(button, "Requeueing", async () => {
        await api(`/api/jobs/${encodeURIComponent(button.dataset.requeueJob)}/requeue`, { method: "POST" });
        toast("Job requeued.");
        await loadState();
      });
    });
  });

  elements.jobsList.querySelectorAll("[data-job-events]").forEach((button) => {
    button.addEventListener("click", async () => {
      await withBusy(button, "Loading", async () => {
        const response = await api(`/api/jobs/${encodeURIComponent(button.dataset.jobEvents)}/events`);
        const text = response.events.map((event) => `${formatTime(event.createdAt)} ${event.type}: ${event.message}`).join("\n") || "No events for this job.";
        toast(text);
      });
    });
  });
}

function renderEvents() {
  elements.eventsList.innerHTML = state.events.map((event) => `
    <article class="timeline-item">
      <div class="result-row">
        <h3>${escapeHtml(event.type)}</h3>
        <span class="status status--muted">${formatTime(event.createdAt)}</span>
      </div>
      <p>${escapeHtml(event.message)}</p>
      <small>${[event.jobId, event.scheduleId, event.workerId].filter(Boolean).map(escapeHtml).join(" | ")}</small>
    </article>
  `).join("") || `<p class="empty-state">Events appear as jobs move through the queue.</p>`;
}

async function openJobDrawer(jobId) {
  const job = state.jobs.find((item) => item.id === jobId);
  if (!job) {
    toast("Job no longer appears in the current state.", { error: true });
    return;
  }
  const response = await api(`/api/jobs/${encodeURIComponent(jobId)}/events`);
  elements.jobDrawerTitle.textContent = job.type;
  elements.jobDrawerBody.innerHTML = `
    <article class="list-item">
      <div class="result-row">
        <h3>${escapeHtml(job.id)}</h3>
        <span class="status ${statusClass(job.status)}">${escapeHtml(job.status)}</span>
      </div>
      <p>${escapeHtml(job.queue)} | priority ${job.priority} | attempts ${job.attempts}/${job.maxAttempts}</p>
      <small>Run at ${formatDate(job.runAt)} | Created ${formatDate(job.createdAt)}</small>
    </article>
    ${job.failureHint ? `
      <article class="list-item">
        <h3>Failure Guidance</h3>
        <p>${escapeHtml(job.failureHint.likelyCause)}</p>
        <small>${escapeHtml(job.failureHint.suggestedAction)}</small>
      </article>
    ` : ""}
    <article class="list-item">
      <h3>Execution Contract</h3>
      <p>Timeout ${job.timeoutMs || "none"} | Required ${(job.requiredCapabilities || []).join(", ") || "none"}</p>
      <small>Retry ${job.retryPolicy ? `${job.retryPolicy.strategy} base ${job.retryPolicy.baseMs}ms max ${job.retryPolicy.maxDelayMs}ms` : "queue/default"}</small>
    </article>
    <article class="list-item">
      <h3>Payload</h3>
      <pre>${escapeHtml(JSON.stringify(job.payload, null, 2))}</pre>
    </article>
    ${job.output ? `
      <article class="list-item">
        <h3>Output</h3>
        <pre>${escapeHtml(JSON.stringify(job.output, null, 2))}</pre>
      </article>
    ` : ""}
    <article class="list-item">
      <h3>Timeline</h3>
      <div class="timeline">
        ${response.events.map((event) => `
          <div class="timeline-item">
            <div class="result-row">
              <h3>${escapeHtml(event.type)}</h3>
              <span class="status status--muted">${formatTime(event.createdAt)}</span>
            </div>
            <p>${escapeHtml(event.message)}</p>
          </div>
        `).join("") || `<p class="empty-state">No timeline events for this job.</p>`}
      </div>
    </article>
  `;
  elements.drawerBackdrop.hidden = false;
  elements.jobDrawer.hidden = false;
}

function closeJobDrawer() {
  elements.drawerBackdrop.hidden = true;
  elements.jobDrawer.hidden = true;
}

async function api(path, { method = "GET", body } = {}) {
  const response = await fetch(path, {
    method,
    headers: body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.message || "Request failed.");
  }
  return payload;
}

async function withBusy(button, label, task) {
  const original = button?.textContent;
  if (button) {
    button.disabled = true;
    button.textContent = label;
  }
  try {
    await task();
  } catch (error) {
    toast(error.message, { error: true });
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = original;
    }
  }
}

function toast(message, { error = false } = {}) {
  elements.toast.textContent = message;
  elements.toast.classList.toggle("is-error", error);
  elements.toast.classList.add("is-visible");
  window.clearTimeout(toast.timer);
  toast.timer = window.setTimeout(() => {
    elements.toast.classList.remove("is-visible");
  }, 2800);
}

function parseJsonField(value, label) {
  try {
    return value?.trim() ? JSON.parse(value) : {};
  } catch (error) {
    throw new Error(`${label} is invalid JSON: ${error.message}`);
  }
}

function parseOptionalJsonField(value, label) {
  if (!value || !String(value).trim()) {
    return null;
  }
  return parseJsonField(value, label);
}

function localDateTimeToIso(value) {
  if (!value) {
    return null;
  }
  return new Date(value).toISOString();
}

function nullable(value) {
  const normalized = String(value ?? "").trim();
  return normalized || null;
}

function nullableNumber(value) {
  const normalized = String(value ?? "").trim();
  return normalized ? Number(normalized) : null;
}

function splitList(value) {
  return String(value ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function formatDate(value) {
  if (!value) {
    return "-";
  }
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function formatTime(value) {
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function statusClass(status) {
  if (status === "running") {
    return "status--info";
  }
  if (status === "scheduled" || status === "waiting_dependencies") {
    return "status--warn";
  }
  if (status === "dead_letter" || status === "cancelled") {
    return "status--danger";
  }
  if (status === "completed") {
    return "";
  }
  return "status--muted";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#039;");
}

function escapeAttr(value) {
  return escapeHtml(value);
}

window.addEventListener("beforeunload", () => {
  if (refreshTimer) {
    clearInterval(refreshTimer);
  }
});
