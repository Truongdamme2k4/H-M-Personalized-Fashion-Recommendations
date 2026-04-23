const PALETTE = [
  ['1d4e3a', 'ffffff'],
  ['2b6cb0', 'ffffff'],
  ['744210', 'ffffff'],
  ['702459', 'ffffff'],
  ['22543d', 'ffffff'],
  ['742a2a', 'ffffff'],
  ['285e61', 'ffffff'],
  ['4c1d95', 'ffffff'],
  ['1a365d', 'ffffff'],
  ['78350f', 'ffffff'],
  ['831843', 'ffffff'],
  ['064e3b', 'ffffff'],
  ['0c4a6e', 'ffffff'],
  ['365314', 'ffffff'],
  ['581c87', 'ffffff'],
  ['7c2d12', 'ffffff'],
  ['134e4a', 'ffffff'],
  ['9a3412', 'ffffff'],
];

function hash(s) {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  return h;
}

export function unsplashUrl(articleId, type) {
  const label = type && type.trim() ? type : 'Item';
  const [bg, fg] = PALETTE[hash(label) % PALETTE.length];
  const text = encodeURIComponent(label);
  return `https://placehold.co/600x800/${bg}/${fg}?font=montserrat&text=${text}`;
}
