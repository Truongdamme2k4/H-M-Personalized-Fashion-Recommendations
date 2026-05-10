import type {
  CartResponse,
  HomeResponse,
  Product,
  SimilarResponse,
} from './types'

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }))
    throw new Error(err.error || `HTTP ${res.status}`)
  }
  return res.json()
}

export const api = {
  home: (customerId: string) =>
    fetchJson<HomeResponse>(
      `/api/recommendations/home/${encodeURIComponent(customerId)}`
    ),
  product: (id: string) => fetchJson<Product>(`/api/products/${id}`),
  similar: (id: string) =>
    fetchJson<SimilarResponse>(`/api/products/${id}/similar`),
  cartRecs: (items: string[]) =>
    fetchJson<CartResponse>('/api/recommendations/cart', {
      method: 'POST',
      body: JSON.stringify({ items }),
    }),
}
