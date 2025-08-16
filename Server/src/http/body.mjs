import { PassThrough, Readable } from "stream";

export function isJsonContentType(ct) {
  return /^application\/(json|.*\+json)/i.test(ct || "");
}
export function isTextLike(ct) {
  return (
    /^text\//i.test(ct || "") ||
    /^application\/(json|xml|x-www-form-urlencoded|javascript|.*\+json|.*\+xml)/i.test(
      ct || ""
    )
  );
}
export function ensureBodyStream(req) {
  if (req.body && typeof req.body.pipe === "function") return req.body;
  if (req.raw && typeof req.raw.pipe === "function") return req.raw;
  return Readable.from([]);
}
export async function readToBufferWithLimit(stream, limit) {
  let counted = 0;
  const limiter = new PassThrough();
  const chunks = [];
  limiter.on("data", (chunk) => {
    counted += chunk.length;
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
  return Buffer.concat(chunks);
}
