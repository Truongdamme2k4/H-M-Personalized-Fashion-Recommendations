import { Router } from 'express';
import { getArticle, getSimilar, hydrate } from '../lib/dataStore.js';

const router = Router();

router.get('/:id', (req, res) => {
  const article = getArticle(req.params.id);
  if (!article) return res.status(404).json({ error: 'Product not found' });
  res.json(article);
});

router.get('/:id/similar', (req, res) => {
  const similar = getSimilar(req.params.id);
  if (similar === undefined) {
    return res.status(404).json({ error: 'Product not found' });
  }
  res.json({
    id: req.params.id,
    items: hydrate(similar.slice(0, 6)),
  });
});

export default router;
