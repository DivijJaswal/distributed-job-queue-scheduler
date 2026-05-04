#!/usr/bin/env node
import { JobQueueStore } from "../core/store.mjs";

const args = parseArgs(process.argv.slice(2));
const store = await new JobQueueStore().init();
const job = store.enqueueJob({
  queue: args.queue || "default",
  type: args.type || "generic.job",
  payload: parsePayload(args.payload),
  priority: Number(args.priority || 0),
  maxAttempts: Number(args.maxAttempts || 3),
  runAt: args.runAt,
  idempotencyKey: args.idempotencyKey,
});

console.log(JSON.stringify(job, null, 2));
store.close();

function parseArgs(argv) {
  const result = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (!arg.startsWith("--")) {
      continue;
    }
    const key = arg.slice(2).replace(/-([a-z])/g, (_, char) => char.toUpperCase());
    const next = argv[index + 1];
    if (!next || next.startsWith("--")) {
      result[key] = true;
    } else {
      result[key] = next;
      index += 1;
    }
  }
  return result;
}

function parsePayload(value) {
  if (!value) {
    return {};
  }
  try {
    return JSON.parse(value);
  } catch {
    return { value };
  }
}
