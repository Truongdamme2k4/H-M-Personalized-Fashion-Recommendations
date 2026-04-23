import { Router } from 'express';
import {
  hydrate,
  getUserRecs,
  getTrending,
  getCartRecs,
  getArticle,
} from '../lib/dataStore.js';

const router = Router();

function withReason(items, reason) {
  return items.map((it) => ({ ...it, reason }));
}

router.get('/home/:customerId', async (req, res, next) => {
  try {
    const { customerId } = req.params;
    const personalized = await getUserRecs(customerId);
    if (personalized) {
      return res.json({
        source: 'personalized',
        customerId,
        items: withReason(
          hydrate(personalized),
          'AI gợi ý dựa trên lịch sử mua hàng của bạn'
        ),
      });
    }
    return res.json({
      source: 'trending',
      customerId,
      items: withReason(
        hydrate(getTrending(12)),
        'Top bán chạy toàn hệ thống'
      ),
    });
  } catch (err) {
    next(err);
  }
});

router.get('/trending', (_req, res) => {
  res.json({
    items: withReason(hydrate(getTrending(12)), 'Top bán chạy toàn hệ thống'),
  });
});

router.post('/cart', (req, res) => {
  const { items } = req.body ?? {};
  if (!Array.isArray(items) || items.length === 0) {
    return res.status(400).json({ error: 'items must be a non-empty array' });
  }
  const inCart = new Set(items);
  const cartSize = items.length;
  // recId -> { count, sources: [sourceId,...], bestRank }
  const stats = new Map();
  for (const id of items) {
    const recs = getCartRecs(id);
    recs.forEach((rid, i) => {
      if (inCart.has(rid)) return;
      let entry = stats.get(rid);
      if (!entry) {
        entry = { count: 0, sources: [], bestRank: i + 1 };
        stats.set(rid, entry);
      }
      entry.count += 1;
      entry.sources.push(id);
      if (i + 1 < entry.bestRank) entry.bestRank = i + 1;
    });
  }
  // Stronger signal first: more cart items co-purchase, then better rank
  const sorted = [...stats.entries()].sort(
    (a, b) => b[1].count - a[1].count || a[1].bestRank - b[1].bestRank
  );
  const top = sorted.slice(0, 12);
  const recommendations = top.map(([rid, info]) => {
    const product = hydrate([rid])[0];
    let reason;
    if (info.count > 1) {
      reason = `Liên quan ${info.count}/${cartSize} món trong giỏ`;
    } else {
      const src = getArticle(info.sources[0]);
      const srcLabel = src?.name || info.sources[0];
      reason = `Thường mua cùng "${srcLabel}"`;
    }
    return {
      ...product,
      reason,
      score: info.count,
      cartSize,
      bestRank: info.bestRank,
    };
  });
  res.json({ cart: items, recommendations });
});

export default router;
