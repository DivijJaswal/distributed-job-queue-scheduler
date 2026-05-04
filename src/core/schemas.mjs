const JOB_SCHEMAS = {
  "email.digest": {
    optional: {
      recipient: "string",
      template: "string",
      durationMs: "number",
      failAlways: "boolean",
      failUntilAttempt: "number",
    },
  },
  "webhook.deliver": {
    optional: {
      endpoint: "string",
      durationMs: "number",
      failAlways: "boolean",
      failUntilAttempt: "number",
    },
  },
  "report.generate": {
    optional: {
      reportId: "string",
      rows: "number",
      durationMs: "number",
      failAlways: "boolean",
      failUntilAttempt: "number",
    },
  },
  "billing.reconcile": {
    optional: {
      accountId: "string",
      invoiceCount: "number",
      durationMs: "number",
      failAlways: "boolean",
      failUntilAttempt: "number",
    },
  },
  "cache.warm": {
    optional: {
      keyPattern: "string",
      durationMs: "number",
      failAlways: "boolean",
      failUntilAttempt: "number",
    },
  },
};

export function validatePayload(type, payload) {
  const schema = JOB_SCHEMAS[type];
  if (!schema) {
    return {
      ok: true,
      errors: [],
    };
  }

  const errors = [];
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    errors.push("Payload must be a JSON object.");
  } else {
    for (const [key, value] of Object.entries(payload)) {
      const expected = schema.optional[key];
      if (!expected) {
        continue;
      }
      if (!matchesType(value, expected)) {
        errors.push(`Payload field "${key}" must be ${expected}.`);
      }
    }
  }

  return {
    ok: errors.length === 0,
    errors,
  };
}

export function knownJobTypes() {
  return Object.keys(JOB_SCHEMAS);
}

function matchesType(value, expected) {
  if (value === undefined || value === null) {
    return true;
  }
  return typeof value === expected && (expected !== "number" || Number.isFinite(value));
}
