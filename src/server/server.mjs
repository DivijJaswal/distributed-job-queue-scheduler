#!/usr/bin/env node
import { createReadStream, existsSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { JobQueueStore, NotFoundError } from "../core/store.mjs";
import { buildOpenApiSpec } from "./openapi.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "..", "..");
const publicDir = path.join(repoRoot, "public");
const dataDir = path.join(repoRoot, "data");

export async function createApp({
  store = null,
  dbPath = process.env.JOBQ_DB || path.join(dataDir, "jobq.sqlite"),
} = {}) {
  await mkdir(dataDir, { recursive: true });
  const queueStore = store ?? await new JobQueueStore({ dbPath }).init();
  const sseClients = new Set();

  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url, `http://${req.headers.host ?? "localhost"}`);
      const route = routeParts(url.pathname);

      if (req.method === "GET" && url.pathname === "/health") {
        return sendJson(res, 200, {
          status: "ok",
          service: "distributed-job-queue-scheduler",
          dbPath: path.relative(repoRoot, queueStore.dbPath),
        });
      }

      if (req.method === "GET" && url.pathname === "/openapi.json") {
        return sendJson(res, 200, buildOpenApiSpec());
      }

      if (req.method === "GET" && url.pathname === "/api/state") {
        return sendJson(res, 200, queueStore.getState());
      }

      if (req.method === "GET" && url.pathname === "/api/metrics") {
        return sendJson(res, 200, queueStore.getMetrics());
      }

      if (req.method === "GET" && url.pathname === "/api/autoscale/recommendation") {
        return sendJson(res, 200, queueStore.getAutoscaleRecommendation({
          minWorkers: url.searchParams.get("minWorkers"),
          maxWorkers: url.searchParams.get("maxWorkers"),
          targetJobsPerWorker: url.searchParams.get("targetJobsPerWorker"),
          targetQueueAgeMs: url.searchParams.get("targetQueueAgeMs"),
        }));
      }

      if (req.method === "GET" && url.pathname === "/api/charts/throughput") {
        return sendJson(res, 200, {
          series: queueStore.getThroughputSeries({
            bucketMs: Number(url.searchParams.get("bucketMs") || 60000),
            limit: Number(url.searchParams.get("limit") || 20),
          }),
        });
      }

      if (req.method === "GET" && url.pathname === "/api/worker-pools") {
        return sendJson(res, 200, {
          workerPools: queueStore.getWorkerPools(),
        });
      }

      if (req.method === "GET" && url.pathname === "/api/idempotency") {
        return sendJson(res, 200, queueStore.getIdempotencyStats());
      }

      if (req.method === "GET" && url.pathname === "/metrics") {
        return sendText(res, 200, queueStore.getPrometheusMetrics(), "text/plain; version=0.0.4; charset=utf-8");
      }

      if (req.method === "GET" && url.pathname === "/api/events/stream") {
        return streamState(res, queueStore, sseClients);
      }

      if (req.method === "GET" && url.pathname === "/api/jobs") {
        return sendJson(res, 200, {
          jobs: queueStore.listJobs({
            status: url.searchParams.get("status"),
            queue: url.searchParams.get("queue"),
            type: url.searchParams.get("type"),
            workerId: url.searchParams.get("workerId"),
            workflowId: url.searchParams.get("workflowId"),
            q: url.searchParams.get("q"),
            limit: Number(url.searchParams.get("limit") || 100),
          }),
        });
      }

      if (req.method === "GET" && url.pathname === "/api/rate-limits") {
        return sendJson(res, 200, {
          rateLimits: queueStore.listRateLimits(),
        });
      }

      if (req.method === "POST" && url.pathname === "/api/rate-limits") {
        const rateLimit = queueStore.upsertRateLimit(await readJson(req));
        return sendChanged(res, sseClients, { rateLimit, state: queueStore.getState() }, 201);
      }

      if (req.method === "POST" && url.pathname === "/api/queues") {
        const queue = queueStore.upsertQueue(await readJson(req));
        return sendChanged(res, sseClients, { queue, state: queueStore.getState() }, 201);
      }

      if (route[0] === "api" && route[1] === "queues" && route[2]) {
        if (req.method === "POST" && route[3] === "toggle") {
          const body = await readJson(req);
          const queue = queueStore.toggleQueue(route[2], body.enabled);
          return sendChanged(res, sseClients, { queue, state: queueStore.getState() });
        }
      }

      if (req.method === "POST" && url.pathname === "/api/jobs") {
        const job = queueStore.enqueueJob(await readJson(req));
        return sendChanged(res, sseClients, { job, state: queueStore.getState() }, 201);
      }

      if (route[0] === "api" && route[1] === "jobs" && route[2]) {
        if (req.method === "GET" && route[3] === "events") {
          return sendJson(res, 200, {
            events: queueStore.listJobEvents(route[2]),
          });
        }

        if (req.method === "POST" && route[3] === "cancel") {
          const job = queueStore.cancelJob(route[2]);
          return sendChanged(res, sseClients, { job, state: queueStore.getState() });
        }

        if (req.method === "POST" && route[3] === "requeue") {
          const body = await readJson(req);
          const job = queueStore.requeueJob(route[2], { runAt: body.runAt });
          return sendChanged(res, sseClients, { job, state: queueStore.getState() });
        }
      }

      if (req.method === "POST" && url.pathname === "/api/dead-letter/replay") {
        const requeued = queueStore.requeueDeadLetters(await readJson(req));
        return sendChanged(res, sseClients, { requeued, state: queueStore.getState() });
      }

      if (req.method === "POST" && url.pathname === "/api/workflows") {
        const workflow = queueStore.createWorkflow(await readJson(req));
        return sendChanged(res, sseClients, { workflow, state: queueStore.getState() }, 201);
      }

      if (req.method === "POST" && url.pathname === "/api/demo/seed") {
        const demo = queueStore.seedDemoScenario();
        return sendChanged(res, sseClients, { demo, state: queueStore.getState() }, 201);
      }

      if (req.method === "POST" && url.pathname === "/api/failures/inject") {
        const body = await readJson(req);
        const scenario = queueStore.injectFailureScenario(body.scenario);
        return sendChanged(res, sseClients, { scenario, state: queueStore.getState() }, 201);
      }

      if (req.method === "POST" && url.pathname === "/api/schedules") {
        const schedule = queueStore.createSchedule(await readJson(req));
        return sendChanged(res, sseClients, { schedule, state: queueStore.getState() }, 201);
      }

      if (route[0] === "api" && route[1] === "schedules" && route[2]) {
        if (req.method === "POST" && route[3] === "toggle") {
          const body = await readJson(req);
          const schedule = queueStore.toggleSchedule(route[2], body.enabled);
          return sendChanged(res, sseClients, { schedule, state: queueStore.getState() });
        }

        if (req.method === "POST" && route[3] === "run-now") {
          const job = queueStore.runScheduleNow(route[2]);
          return sendChanged(res, sseClients, { job, state: queueStore.getState() }, 201);
        }
      }

      if (req.method === "POST" && url.pathname === "/api/scheduler/tick") {
        const dispatched = queueStore.dispatchDueSchedules({
          schedulerId: "api-manual",
          limit: 50,
        });
        return sendChanged(res, sseClients, { dispatched, state: queueStore.getState() });
      }

      if (req.method === "POST" && url.pathname === "/api/recover-leases") {
        const recovered = [
          ...queueStore.recoverStaleLeases(),
          ...queueStore.recoverTimedOutJobs(),
        ];
        return sendChanged(res, sseClients, { recovered, state: queueStore.getState() });
      }

      if (req.method === "GET" && url.pathname === "/api/export") {
        return sendJson(res, 200, queueStore.exportSnapshot());
      }

      if (req.method === "POST" && url.pathname === "/api/import") {
        const body = await readJson(req);
        const state = queueStore.importSnapshot(body.snapshot ?? body, {
          mode: body.mode || "merge",
        });
        return sendChanged(res, sseClients, { state });
      }

      if (req.method === "POST" && url.pathname === "/api/maintenance/vacuum") {
        const result = queueStore.vacuumDatabase();
        return sendChanged(res, sseClients, { result, state: queueStore.getState() });
      }

      if (req.method === "POST" && url.pathname === "/api/maintenance/prune-events") {
        const result = queueStore.pruneEvents(await readJson(req));
        return sendChanged(res, sseClients, { result, state: queueStore.getState() });
      }

      if (req.method === "POST" && url.pathname === "/api/maintenance/archive-jobs") {
        const result = queueStore.archiveTerminalJobs(await readJson(req));
        return sendChanged(res, sseClients, { result, state: queueStore.getState() });
      }

      return serveStatic(url.pathname, res);
    } catch (error) {
      return sendError(res, error);
    }
  });

  return {
    server,
    store: queueStore,
    listen({ host = process.env.HOST || "127.0.0.1", port = Number(process.env.PORT || 4230) } = {}) {
      return new Promise((resolve) => {
        server.listen(port, host, () => {
          console.log(`distributed-job-queue-scheduler listening on http://${host}:${port}`);
          resolve({ host, port });
        });
      });
    },
    close() {
      return new Promise((resolve, reject) => {
        server.close((error) => {
          for (const client of sseClients) {
            clearInterval(client.interval);
            client.res.end();
          }
          queueStore.close();
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        });
      });
    },
  };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const app = await createApp();
  await app.listen();
}

function serveStatic(urlPath, res) {
  const safePath = urlPath === "/" ? "/index.html" : decodeURIComponent(urlPath);
  const filePath = path.resolve(publicDir, `.${safePath}`);
  if (!filePath.startsWith(publicDir) || !existsSync(filePath)) {
    return sendJson(res, 404, {
      error: "not_found",
      message: "Route not found.",
    });
  }
  const stream = createReadStream(filePath);
  stream.once("open", () => {
    res.writeHead(200, {
      "content-type": contentType(filePath),
    });
    stream.pipe(res);
  });
  stream.on("error", (error) => {
    if (!res.headersSent) {
      return sendError(res, error);
    }
    res.destroy(error);
  });
}

function routeParts(pathname) {
  return pathname.split("/").filter(Boolean).map(decodeURIComponent);
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
  });
  res.end(`${JSON.stringify(payload)}\n`);
}

function sendText(res, statusCode, payload, contentTypeValue = "text/plain; charset=utf-8") {
  res.writeHead(statusCode, {
    "content-type": contentTypeValue,
    "cache-control": "no-store",
  });
  res.end(payload);
}

function sendChanged(res, clients, payload, statusCode = 200) {
  broadcastState(clients, payload.state);
  return sendJson(res, statusCode, payload);
}

function streamState(res, store, clients) {
  res.writeHead(200, {
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-store",
    connection: "keep-alive",
  });
  const client = {
    res,
    interval: setInterval(() => sendSse(res, "state", store.getState()), 2000),
  };
  clients.add(client);
  sendSse(res, "state", store.getState());
  res.on("close", () => {
    clearInterval(client.interval);
    clients.delete(client);
  });
}

function broadcastState(clients, state) {
  for (const client of clients) {
    sendSse(client.res, "state", state);
  }
}

function sendSse(res, event, payload) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function sendError(res, error) {
  const statusCode = error.statusCode || (error instanceof NotFoundError ? 404 : error.name === "SyntaxError" ? 400 : 500);
  sendJson(res, statusCode, {
    error: error.name || "Error",
    message: error.message || "Unexpected server error.",
  });
}

async function readJson(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }
  const raw = Buffer.concat(chunks).toString("utf8").trim();
  return raw ? JSON.parse(raw) : {};
}

function contentType(filePath) {
  switch (path.extname(filePath).toLowerCase()) {
    case ".html":
      return "text/html; charset=utf-8";
    case ".css":
      return "text/css; charset=utf-8";
    case ".js":
      return "text/javascript; charset=utf-8";
    case ".json":
      return "application/json; charset=utf-8";
    case ".svg":
      return "image/svg+xml";
    default:
      return "application/octet-stream";
  }
}
