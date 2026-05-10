import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline';
import { readCsv } from './csv.js';

const BUCKETS = 256;
const CACHE_LIMIT = 32;
const loadedBuckets = new Map();

function bucketOf(customerId) {
  return customerId.slice(0, 2).toLowerCase();
}

function bucketFile(dir, key) {
  return path.join(dir, `${key}.jsonl`);
}

async function buildIndex(csvPath, dir) {
  console.log('[userIndex] building bucket index from', csvPath);
  await fsp.mkdir(dir, { recursive: true });
  const streams = new Map();
  const started = Date.now();
  let rows = 0;

  await readCsv(csvPath, (row) => {
    const id = row.customer_id;
    const pred = row.prediction_str;
    if (!id || !pred) return;
    const key = bucketOf(id);
    let s = streams.get(key);
    if (!s) {
      s = fs.createWriteStream(bucketFile(dir, key), { encoding: 'utf-8' });
      streams.set(key, s);
    }
    s.write(`${id}\t${pred}\n`);
    rows++;
  });

  await Promise.all(
    [...streams.values()].map(
      (s) => new Promise((resolve) => s.end(resolve))
    )
  );
  await fsp.writeFile(
    path.join(dir, '_meta.json'),
    JSON.stringify({ rows, builtAt: new Date().toISOString() })
  );
  console.log(
    `[userIndex] built ${streams.size} buckets, ${rows} rows in ${((Date.now() - started) / 1000).toFixed(1)}s`
  );
}

async function loadBucket(dir, key) {
  if (loadedBuckets.has(key)) {
    const m = loadedBuckets.get(key);
    loadedBuckets.delete(key);
    loadedBuckets.set(key, m);
    return m;
  }
  const file = bucketFile(dir, key);
  const map = new Map();
  try {
    const stream = fs.createReadStream(file, { encoding: 'utf-8' });
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    for await (const line of rl) {
      if (!line) continue;
      const tab = line.indexOf('\t');
      if (tab < 0) continue;
      map.set(line.slice(0, tab), line.slice(tab + 1));
    }
  } catch (err) {
    if (err.code !== 'ENOENT') throw err;
  }
  loadedBuckets.set(key, map);
  while (loadedBuckets.size > CACHE_LIMIT) {
    const firstKey = loadedBuckets.keys().next().value;
    loadedBuckets.delete(firstKey);
  }
  return map;
}

export async function initUserIndex({ csvPath, cacheDir }) {
  const metaFile = path.join(cacheDir, '_meta.json');
  const hasIndex = await fsp
    .stat(metaFile)
    .then(() => true)
    .catch(() => false);
  if (!hasIndex) {
    await buildIndex(csvPath, cacheDir);
  } else {
    const meta = JSON.parse(await fsp.readFile(metaFile, 'utf-8'));
    console.log(
      `[userIndex] reusing bucket index (${meta.rows} rows, built ${meta.builtAt})`
    );
  }

  return {
    async get(customerId) {
      if (!customerId) return null;
      const key = bucketOf(customerId);
      const bucket = await loadBucket(cacheDir, key);
      const raw = bucket.get(customerId);
      if (!raw) return null;
      return raw.split(',').map((s) => s.trim()).filter(Boolean);
    },
  };
}
