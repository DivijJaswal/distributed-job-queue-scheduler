export function nowIso() {
  return new Date().toISOString();
}

export function addMs(dateInput, ms) {
  return new Date(new Date(dateInput).getTime() + Number(ms)).toISOString();
}

export function epochMs(dateInput) {
  return new Date(dateInput).getTime();
}

export function isDue(dateInput, reference = nowIso()) {
  return epochMs(dateInput) <= epochMs(reference);
}
