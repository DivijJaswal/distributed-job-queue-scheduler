import crypto from "node:crypto";

export function createId(prefix, label = "") {
  const slug = String(label || prefix)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "")
    .slice(0, 36) || prefix;
  return `${prefix}-${slug}-${crypto.randomUUID().slice(0, 8)}`;
}

export function queueId(name) {
  return String(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "") || "default";
}
