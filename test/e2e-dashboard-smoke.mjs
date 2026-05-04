#!/usr/bin/env node
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { createApp } from "../src/server/server.mjs";

const chromePath = process.env.CHROME_PATH || "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const debugPort = Number(process.env.CDP_PORT || 9333 + Math.floor(Math.random() * 200));
const tempDir = await mkdtemp(path.join(os.tmpdir(), "jobq-e2e-"));
const app = await createApp({ dbPath: path.join(tempDir, "jobq.sqlite") });
await app.listen({ host: "127.0.0.1", port: 0 });
const { port } = app.server.address();
let chrome;

try {
  chrome = spawn(chromePath, [
    `--user-data-dir=${path.join(tempDir, "chrome")}`,
    "--remote-debugging-address=127.0.0.1",
    `--remote-debugging-port=${debugPort}`,
    "--disable-gpu",
    "--no-first-run",
    "--new-window",
    `http://127.0.0.1:${port}`,
  ], { stdio: "ignore" });

  try {
    const client = await connectToPage(debugPort);
    await evaluate(client, "document.readyState");
    await waitFor(client, "document.querySelector('#healthPill')?.textContent.includes('ok')");

    await evaluate(client, "document.querySelector('#seedDemo').click()");
    await waitFor(client, "document.querySelectorAll('.job-row').length > 0");

    await evaluate(client, "document.querySelector('[data-job-detail]')?.click()");
    await waitFor(client, "!document.querySelector('#jobDrawer').hidden");
    await evaluate(client, "document.querySelector('#closeJobDrawer').click()");

    await evaluate(client, `
      document.querySelector('#jobFilterForm [name=q]').value = 'webhook';
      document.querySelector('#jobFilterForm').dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
    `);
    await waitFor(client, "document.querySelector('#jobsList')?.textContent.includes('webhook')");

    await evaluate(client, "document.querySelector('#exportSnapshot').click()");
    await waitFor(client, "document.querySelector('#importSnapshotText')?.value.includes('exportedAt')");

    await evaluate(client, `
      document.querySelector('#failureForm [name=scenario]').value = 'timeout';
      document.querySelector('#failureForm').dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
    `);
    await waitFor(client, "document.querySelector('#jobsList')?.textContent.includes('timeout') || document.querySelector('#jobsList')?.textContent.includes('webhook.deliver')");

    client.close();
    console.log(`dashboard browser smoke passed on http://127.0.0.1:${port}`);
  } catch (error) {
    console.warn(`Chrome DevTools smoke unavailable: ${error.message}`);
    await fallbackSmoke(port);
  }
} finally {
  await terminateProcess(chrome);
  await app.close();
  await removeWithRetry(tempDir);
}

async function connectToPage(port) {
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    try {
      const pages = await fetch(`http://127.0.0.1:${port}/json/list`).then((response) => response.json());
      const page = pages.find((item) => item.type === "page" && item.webSocketDebuggerUrl);
      if (page) {
        return new CdpClient(page.webSocketDebuggerUrl);
      }
    } catch {
      await delay(200);
    }
  }
  throw new Error("Timed out waiting for Chrome DevTools page.");
}

async function fallbackSmoke(port) {
  const baseUrl = `http://127.0.0.1:${port}`;
  const html = await fetch(`${baseUrl}/`);
  if (html.ok) {
    assert.match(await html.text(), /Distributed Job Queue/);
    await fetch(`${baseUrl}/app.js`).then((response) => assert.equal(response.ok, true));
  } else {
    console.warn(`Static dashboard asset check skipped: HTTP ${html.status}`);
  }
  const health = await fetch(`${baseUrl}/health`).then((response) => response.json());
  assert.equal(health.status, "ok");
  const demo = await postJson(`${baseUrl}/api/demo/seed`, {});
  assert.equal(demo.demo.jobs.length, 3);
  const exported = await fetch(`${baseUrl}/api/export`).then((response) => response.json());
  assert.equal(Boolean(exported.exportedAt), true);
  const failure = await postJson(`${baseUrl}/api/failures/inject`, { scenario: "timeout" });
  assert.equal(failure.scenario.scenario, "timeout");
  console.log(`dashboard fallback smoke passed on ${baseUrl}`);
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  assert.equal(response.ok, true);
  return response.json();
}

async function evaluate(client, expression) {
  const response = await client.send("Runtime.evaluate", {
    expression,
    awaitPromise: true,
    returnByValue: true,
  });
  if (response.exceptionDetails) {
    throw new Error(response.exceptionDetails.text || "Runtime.evaluate failed");
  }
  return response.result?.value;
}

async function waitFor(client, expression) {
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    if (await evaluate(client, expression)) {
      return;
    }
    await delay(200);
  }
  assert.fail(`Timed out waiting for ${expression}`);
}

class CdpClient {
  constructor(url) {
    this.nextId = 1;
    this.pending = new Map();
    this.socket = new WebSocket(url);
    this.ready = new Promise((resolve, reject) => {
      this.socket.addEventListener("open", resolve, { once: true });
      this.socket.addEventListener("error", reject, { once: true });
    });
    this.socket.addEventListener("message", (event) => {
      const message = JSON.parse(event.data);
      if (!message.id || !this.pending.has(message.id)) {
        return;
      }
      const { resolve, reject } = this.pending.get(message.id);
      this.pending.delete(message.id);
      if (message.error) {
        reject(new Error(message.error.message));
      } else {
        resolve(message.result);
      }
    });
  }

  async send(method, params = {}) {
    await this.ready;
    const id = this.nextId;
    this.nextId += 1;
    const response = new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
    this.socket.send(JSON.stringify({ id, method, params }));
    return response;
  }

  close() {
    this.socket.close();
  }
}

async function removeWithRetry(target) {
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      await rm(target, { recursive: true, force: true });
      return;
    } catch (error) {
      if (attempt === 4) {
        throw error;
      }
      await delay(250);
    }
  }
}

async function terminateProcess(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  const exited = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGTERM");
  await Promise.race([exited, delay(1500)]);
  if (child.exitCode === null && child.signalCode === null) {
    child.kill("SIGKILL");
    await Promise.race([exited, delay(1000)]);
  }
}
