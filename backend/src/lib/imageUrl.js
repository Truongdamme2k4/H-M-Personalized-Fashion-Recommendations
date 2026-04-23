import { unsplashUrl } from './unsplash.js';

const TEMPLATE = process.env.IMAGE_TEMPLATE || '';

export function imageUrl(articleId, folder, type) {
  if (TEMPLATE && folder) {
    return TEMPLATE.replace('{folder}', folder).replace('{id}', articleId);
  }
  return unsplashUrl(articleId, type);
}
