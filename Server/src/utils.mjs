import crypto from "crypto";
import { PassThrough, Readable } from "stream";

export const isJsonContentType = (ct) =>
  /^application\/(json|.*\+json)/i.test(ct || "");
export const isTextLike = (ct) =>
  /^text\//i.test(ct || "") ||
  /^application\/(json|xml|x-www-form-urlencoded|javascript|.*\+json|.*\+xml)/i.test(
    ct || ""
  );

export function chooseEventId() {
  return (
    crypto.randomUUID?.() ||
    `${Date.now()}-${crypto.randomBytes(8).toString("hex")}`
  );
}

export function ensureBodyStream(req) {
  if (req.body && typeof req.body.pipe === "function") return req.body;
  if (req.raw && typeof req.raw.pipe === "function") return req.raw;
  return Readable.from([]);
}

export async function readToBufferWithLimit(stream, limit) {
  let counted = 0;
  const hasher = crypto.createHash("sha256");
  const limiter = new PassThrough();
  const chunks = [];

  limiter.on("data", (chunk) => {
    counted += chunk.length;
    hasher.update(chunk);
    if (counted > limit) limiter.destroy(new Error("payload_too_large"));
  });

  await new Promise((resolve, reject) => {
    stream
      .on("error", reject)
      .pipe(limiter)
      .on("error", reject)
      .on("data", (c) => chunks.push(c))
      .on("end", resolve);
  });

  return {
    buf: Buffer.concat(chunks),
    size: counted,
    hash: `sha256:${hasher.digest("hex")}`,
  };
}

export function readCorrelationIdFromHeaders(req) {
  const v = req.headers["x-correlation-id"];
  const cid = Array.isArray(v) ? v[0] : v;
  if (!cid) return null;
  const s = String(cid).trim();
  if (
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      s
    ) ||
    /^[0-9a-f]{32}$/i.test(s)
  )
    return s.toLowerCase();
  return null;
}

export function redactHeaders(h, redactions = []) {
  if (!h) return h;
  const out = { ...h };
  const redactList = [
    "authorization",
    "proxy-authorization",
    "x-api-key",
    "x-auth-token",
    ...redactions,
  ].filter(Boolean);
  for (const k of redactList) if (out[k]) out[k] = "[redacted]";
  return out;
}
