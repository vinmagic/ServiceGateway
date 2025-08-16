import fs from "fs";
import path from "path";
import yaml from "js-yaml";

let streamsRef = { cfg: [] };

function compileStreams(
  rawList,
  assertFn = (x, m) => {
    if (!x) throw new Error(m);
  }
) {
  return rawList.map((s, i) => {
    const label = s?.id || `streams[${i}]`;
    assertFn(s?.match?.path, `${label}.match.path is required`);
    const _rePath = new RegExp(s.match.path);
    const _methods = s.match.methods
      ? s.match.methods.map((m) => String(m).toUpperCase())
      : null;
    return { ...s, _rePath, _methods };
  });
}

export function findStream(method, urlPath) {
  const M = String(method).toUpperCase();
  return streamsRef.cfg.find((s) => {
    if (s._methods && !s._methods.includes(M)) return false;
    return s._rePath.test(urlPath);
  });
}

export async function initStreams(logger, streamsDir = "./streams.d") {
  const out = [];
  try {
    const entries = fs.readdirSync(streamsDir, { withFileTypes: true });
    for (const ent of entries) {
      if (!ent.isFile()) continue;
      const lower = ent.name.toLowerCase();
      if (!lower.endsWith(".yml") && !lower.endsWith(".yaml")) continue;
      const doc = yaml.load(
        fs.readFileSync(path.join(streamsDir, ent.name), "utf8")
      );
      if (!doc) continue;
      if (Array.isArray(doc)) out.push(...doc);
      else if (typeof doc === "object") out.push(doc);
    }
  } catch {}
  streamsRef = { cfg: compileStreams(out) };
  logger?.info?.({ count: streamsRef.cfg.length }, "streams_loaded");

  // sort by priority (high first, default=0)
  streamsRef.cfg.sort((b, a) => (b.priority || 0) - (a.priority || 0));

  return streamsRef;
}
