export function normalizeHost(req) {
  const raw =
    req.headers[":authority"] ||
    req.headers["x-forwarded-host"] ||
    req.headers["host"] ||
    "";
  let h = String(Array.isArray(raw) ? raw[0] : raw)
    .trim()
    .toLowerCase();
  if (h.endsWith(".")) h = h.slice(0, -1);
  if (h.includes(":") && !h.startsWith("[")) h = h.split(":")[0];
  return h;
}
