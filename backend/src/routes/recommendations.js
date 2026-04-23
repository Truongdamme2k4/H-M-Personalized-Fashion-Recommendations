import { Router } from 'express';
import {
  hydrate,
  getUserRecs,
  getTrending,
  getCartRecs,
} from '../lib/dataStore.js';

const router = Router();

// Homepage: personalized for existing customers, trending fallback for new ones
router.get('/home/:customerId', async (req, res, next) => {
  try {
    const { customerId } = req.params;
    const personalized = await getUserRecs(customerId);
    if (personalized) {
      return res.json({
        source: 'personalized',
        customerId,
        items: hydrate(personalized),
      });
    }
    return res.json({
      source: 'trending',
      customerId,
      items: hydrate(getTrending(12)),
    });
  } catch (err) {
    next(err);
  }
});

router.get('/trending', (_req, res) => {
  res.json({ items: hydrate(getTrending(12)) });
});

// Cart cross-sell: merge up to 6-per-item, dedupe, exclude cart contents
router.post('/cart', (req, res) => {
  const { items } = req.body ?? {};
  if (!Array.isArray(items) || items.length === 0) {
    return res.status(400).json({ error: 'items must be a non-empty array' });
  }
  const inCart = new Set(items);
  const seen = new Set();
  const merged = [];
  for (const id of items) {
    for (const rid of getCartRecs(id)) {
      if (inCart.has(rid) || seen.has(rid)) continue;
      seen.add(rid);
      merged.push(rid);
    }
  }
  res.json({
    cart: items,
    recommendations: hydrate(merged.slice(0, 12)),
  });
});

export default router;
