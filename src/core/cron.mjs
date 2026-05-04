const FIELD_RANGES = [
  [0, 59],
  [0, 23],
  [1, 31],
  [1, 12],
  [0, 6],
];

export function nextCronRun(expression, from = new Date()) {
  const fields = parseCronExpression(expression);
  const cursor = new Date(from);
  cursor.setSeconds(0, 0);
  cursor.setMinutes(cursor.getMinutes() + 1);

  const maxChecks = 366 * 24 * 60;
  for (let index = 0; index < maxChecks; index += 1) {
    if (matchesCron(cursor, fields)) {
      return cursor.toISOString();
    }
    cursor.setMinutes(cursor.getMinutes() + 1);
  }

  throw new Error(`No cron run found within one year for "${expression}".`);
}

export function parseCronExpression(expression) {
  const parts = String(expression || "").trim().split(/\s+/);
  if (parts.length !== 5) {
    throw new Error("Cron expression must contain 5 fields: minute hour day month weekday.");
  }
  return parts.map((part, index) => parseField(part, FIELD_RANGES[index]));
}

function matchesCron(date, fields) {
  return fields[0].has(date.getMinutes())
    && fields[1].has(date.getHours())
    && fields[2].has(date.getDate())
    && fields[3].has(date.getMonth() + 1)
    && fields[4].has(date.getDay());
}

function parseField(raw, [min, max]) {
  const values = new Set();
  for (const segment of String(raw).split(",")) {
    const trimmed = segment.trim();
    if (!trimmed) {
      continue;
    }
    const [rangePart, stepPart] = trimmed.split("/");
    const step = stepPart === undefined ? 1 : Number.parseInt(stepPart, 10);
    if (!Number.isInteger(step) || step < 1) {
      throw new Error(`Invalid cron step "${segment}".`);
    }

    let start;
    let end;
    if (rangePart === "*") {
      start = min;
      end = max;
    } else if (rangePart.includes("-")) {
      const [rawStart, rawEnd] = rangePart.split("-");
      start = Number.parseInt(rawStart, 10);
      end = Number.parseInt(rawEnd, 10);
    } else {
      start = Number.parseInt(rangePart, 10);
      end = start;
    }

    if (!Number.isInteger(start) || !Number.isInteger(end) || start < min || end > max || start > end) {
      throw new Error(`Invalid cron field "${segment}".`);
    }

    for (let value = start; value <= end; value += step) {
      values.add(value);
    }
  }

  if (values.size === 0) {
    throw new Error("Cron field cannot be empty.");
  }
  return values;
}
