import { Router } from 'express';
import { getArticle, getSimilar, hydrate } from '../lib/dataStore.js';

const router = Router();

router.get('/:id', (req, res) => {
  const article = getArticle(req.params.id);
  if (!article) return res.status(404).json({ error: 'Product not found' });
  res.json(article);
});

router.get('/:id/similar', (req, res) => {
  const { id } = req.params;
  const similar = getSimilar(id);
  if (similar === undefined) {
    return res.status(404).json({ error: 'Product not found' });
  }
  const src = getArticle(id);
  const srcLabel = src?.name || id;
  const srcType = src?.type;
  const reason = srcType
    ? `Tương tự "${srcLabel}" · cùng ${srcType}`
    : `Tương tự "${srcLabel}"`;
  res.json({
    id,
    items: hydrate(similar.slice(0, 6)).map((p) => ({ ...p, reason })),
  });
});

export default router;
