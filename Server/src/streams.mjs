import fs from "fs";
import path from "path";
import yaml from "js-yaml";

export function loadYamlFile(fp) {
  try {
    return yaml.load(fs.readFileSync(fp, "utf8"));
  } catch (e) {
    console.warn("streams_yaml_load_failed", { file: fp, err: e?.message });
    return null;
  }
}

export function loadStreamsFromDir(dir) {
  const out = [];
  if (!dir) return out;
  let entries = [];
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return out;
  }
  for (const ent of entries) {
    if (!ent.isFile()) continue;
    const lower = ent.name.toLowerCase();
    if (!lower.endsWith(".yml") && !lower.endsWith(".yaml")) continue;
    const doc = loadYamlFile(path.join(dir, ent.name));
    if (!doc) continue;
    if (Array.isArray(doc)) out.push(...doc);
    else if (doc && typeof doc === "object") out.push(doc);
  }
  return out;
}

export function compileStreams(rawList) {
  return rawList.map((s, i) => {
    const label = s?.id || `streams[${i}]`;
    if (!s?.match?.path) throw new Error(`${label}.match.path is required`);
    let _rePath,
      _reHost = null;
    try {
      _rePath = new RegExp(s.match.path);
      if (s.match.host) _reHost = new RegExp(s.match.host);
    } catch (e) {
      throw new Error(`Invalid regex in ${label}: ${String(e?.message || e)}`);
    }
    return {
      ...s,
      _rePath,
      _reHost,
      _methods: s.match.methods
        ? s.match.methods.map((m) => String(m).toUpperCase())
        : null,
    };
  });
}

export function sortByPriority(compiled) {
  compiled.sort((a, b) => (b.priority || 0) - (a.priority || 0));
  return compiled;
}

export function normalizeHost(req) {
  const raw =
    req.headers[":authority"] ||
    (Array.isArray(req.headers["x-forwarded-host"])
      ? req.headers["x-forwarded-host"][0]
      : req.headers["x-forwarded-host"]) ||
    (Array.isArray(req.headers["host"])
      ? req.headers["host"][0]
      : req.headers["host"]) ||
    "";
  let h = raw.trim().toLowerCase();
  if (h.startsWith("[") && h.includes("]")) {
    const i = h.indexOf("]");
    h = h.slice(0, i + 1);
  } else if (h.includes(":")) {
    h = h.split(":")[0];
  }
  if (h.endsWith(".")) h = h.slice(0, -1);
  return h;
}

export function findStream(streams, method, urlPath, host) {
  const M = String(method).toUpperCase();
  return streams.find((s) => {
    if (s._methods && !s._methods.includes(M)) return false;
    if (s._reHost && !s._reHost.test(host || "")) return false;
    return s._rePath.test(urlPath);
  });
}
