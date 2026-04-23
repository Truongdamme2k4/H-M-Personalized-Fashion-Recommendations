export interface Product {
  id: string
  name?: string
  type?: string
  price?: number | null
  imageFolder?: string
  imageUrl?: string
  missing?: boolean
}

export interface HomeResponse {
  source: 'personalized' | 'trending'
  customerId: string
  items: Product[]
}

export interface TrendingResponse {
  items: Product[]
}

export interface CartResponse {
  cart: string[]
  recommendations: Product[]
}

export interface SimilarResponse {
  id: string
  items: Product[]
}
