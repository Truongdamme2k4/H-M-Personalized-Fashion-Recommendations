import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { readCsv } from './csv.js';
import { connectMongo, getDb } from './mongo.js';
import { imageUrl } from './imageUrl.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.resolve(__dirname, '..', '..', process.env.DATA_DIR || '../');

const TRENDING_TTL_MS = 60_000;

const store = {
  ready: false,
  articles: new Map(),
  trendingCache: { items: [], expires: 0 },
};

async function seedArticles(coll) {
  const count = await coll.estimatedDocumentCount();
  if (count > 0) {
    console.log(`[seed] articles already has ${count} docs — skipping`);
    return;
  }
  const file = path.join(DATA_DIR, 'article_metadata.csv');
  console.log(`[seed] articles from ${file}`);
  const docs = [];
  await readCsv(file, (row) => {
    if (!row.article_id) return;
    docs.push({
      _id: row.article_id,
      name: row.prod_name,
      type: row.product_type_name,
      price: row.display_price ? Number(row.display_price) : null,
      imageFolder: row.image_folder,
    });
  });
  const BATCH = 5000;
  let inserted = 0;
  for (let i = 0; i < docs.length; i += BATCH) {
    const slice = docs.slice(i, i + BATCH);
    await coll.insertMany(slice, { ordered: false });
    inserted += slice.length;
  }
  console.log(`[seed] articles inserted=${inserted}`);
}

async function seedFromIdMapJson(coll, filename, label) {
  const count = await coll.estimatedDocumentCount();
  if (count > 0) {
    console.log(`[seed] ${label} already has ${count} docs — skipping`);
    return;
  }
  const raw = await fs.readFile(path.join(DATA_DIR, filename), 'utf-8');
  const obj = JSON.parse(raw);
  const ids = Object.keys(obj);
  console.log(`[seed] ${label} from ${filename} (${ids.length} keys)`);
  const BATCH = 5000;
  let inserted = 0;
  for (let i = 0; i < ids.length; i += BATCH) {
    const slice = ids.slice(i, i + BATCH).map((id) => ({ _id: id, items: obj[id] || [] }));
    if (slice.length === 0) continue;
    await coll.insertMany(slice, { ordered: false });
    inserted += slice.length;
  }
  console.log(`[seed] ${label} inserted=${inserted}`);
}

async function seedGlobalTrending(db) {
  const coll = db.collection('global_trending');
  const existing = await coll.findOne({ _id: 'global' });
  if (existing) {
    console.log(`[seed] global_trending already populated (${(existing.items || []).length} items)`);
    return;
  }
  const file = path.join(DATA_DIR, 'global_trending.json');
  try {
    const raw = await fs.readFile(file, 'utf-8');
    const obj = JSON.parse(raw);
    const items = obj.trending_items || [];
    await coll.updateOne(
      { _id: 'global' },
      { $set: { items, updated_at: new Date(), source: 'seed' } },
      { upsert: true }
    );
    console.log(`[seed] global_trending inserted=${items.length}`);
  } catch (err) {
    if (err.code !== 'ENOENT') throw err;
    console.log('[seed] global_trending.json not found — skipping (pipeline will populate)');
  }
}

async function loadArticlesToMemory(coll) {
  const cursor = coll.find({}, { projection: { _id: 1, name: 1, type: 1, price: 1, imageFolder: 1 } });
  let n = 0;
  for await (const doc of cursor) {
    store.articles.set(doc._id, {
      id: doc._id,
      name: doc.name,
      type: doc.type,
      price: doc.price ?? null,
      imageFolder: doc.imageFolder,
      imageUrl: imageUrl(doc._id, doc.imageFolder, doc.type),
    });
    n++;
  }
  console.log(`[mongo] loaded ${n} articles into memory`);
}

export async function initStore() {
  if (store.ready) return store;
  const db = await connectMongo();

  await Promise.all([
    seedArticles(db.collection('articles')),
    seedFromIdMapJson(db.collection('similar_products'), 'similar_products.json', 'similar_products'),
    seedFromIdMapJson(db.collection('cart_recommendations'), 'cart_recommendations.json', 'cart_recommendations'),
    seedGlobalTrending(db),
  ]);

  await loadArticlesToMemory(db.collection('articles'));

  store.ready = true;
  return store;
}

export function getArticle(id) {
  return store.articles.get(id) || null;
}

export function hydrate(ids) {
  return ids.map((id) => store.articles.get(id) || { id, missing: true });
}

export async function getUserRecs(customerId) {
  if (!customerId) return null;
  const doc = await getDb().collection('user_recommendations').findOne(
    { _id: customerId },
    { projection: { items: 1 } }
  );
  if (!doc) return null;
  return doc.items || null;
}

export async function getTrending(limit = 12) {
  const now = Date.now();
  if (store.trendingCache.expires > now) {
    return store.trendingCache.items.slice(0, limit);
  }
  const doc = await getDb().collection('global_trending').findOne({ _id: 'global' });
  const items = doc?.items || [];
  store.trendingCache = { items, expires: now + TRENDING_TTL_MS };
  return items.slice(0, limit);
}

export async function getCartRecs(id) {
  const doc = await getDb().collection('cart_recommendations').findOne(
    { _id: id },
    { projection: { items: 1 } }
  );
  return doc?.items || [];
}

export async function getSimilar(id) {
  const doc = await getDb().collection('similar_products').findOne(
    { _id: id },
    { projection: { items: 1 } }
  );
  if (!doc) return undefined;
  return doc.items || [];
}
