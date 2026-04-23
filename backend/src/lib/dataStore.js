import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { readCsv } from './csv.js';
import { initUserIndex } from './userIndex.js';
import { imageUrl } from './imageUrl.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.resolve(__dirname, '..', '..', process.env.DATA_DIR || '../');
const CACHE_DIR = path.resolve(__dirname, '..', '..', 'data_cache', 'users');
const IMAGE_BASE = process.env.IMAGE_BASE || '/images';

const store = {
  ready: false,
  articles: new Map(),
  userIndex: null,
  cartRecs: {},
  similar: {},
  trending: [],
};

async function loadArticles() {
  const file = path.join(DATA_DIR, 'article_metadata.csv');
  await readCsv(file, (row) => {
    if (!row.article_id) return;
    store.articles.set(row.article_id, {
      id: row.article_id,
      name: row.prod_name,
      type: row.product_type_name,
      price: row.display_price ? Number(row.display_price) : null,
      imageFolder: row.image_folder,
      imageUrl: imageUrl(row.article_id, row.image_folder, row.product_type_name),
    });
  });
}

async function loadJson(filename) {
  const raw = await fs.readFile(path.join(DATA_DIR, filename), 'utf-8');
  return JSON.parse(raw);
}

export async function initStore() {
  if (store.ready) return store;
  const cartJson = await loadJson('cart_recommendations.json');
  const similarJson = await loadJson('similar_products.json');
  const trendingJson = await loadJson('global_trending.json');
  store.cartRecs = cartJson;
  store.similar = similarJson;
  store.trending = trendingJson.trending_items || [];
  await loadArticles();
  store.userIndex = await initUserIndex({
    csvPath: path.join(DATA_DIR, 'final_recommendations.csv'),
    cacheDir: CACHE_DIR,
  });
  store.ready = true;
  console.log(
    `[data] articles=${store.articles.size} ` +
    `cart_keys=${Object.keys(cartJson).length} similar_keys=${Object.keys(similarJson).length} ` +
    `trending=${store.trending.length}`
  );
  return store;
}

export function getArticle(id) {
  return store.articles.get(id) || null;
}

export function hydrate(ids) {
  return ids.map((id) => store.articles.get(id) || { id, missing: true });
}

export async function getUserRecs(customerId) {
  if (!store.userIndex) return null;
  return store.userIndex.get(customerId);
}

export function getTrending(limit = 12) {
  return store.trending.slice(0, limit);
}

export function getCartRecs(id) {
  return store.cartRecs[id] || [];
}

export function getSimilar(id) {
  return store.similar[id];
}
