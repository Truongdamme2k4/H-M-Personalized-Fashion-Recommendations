import { SimpleGrid, Text } from '@mantine/core'
import type { Product } from '../types'
import { ProductCard } from './ProductCard'

interface Props {
  items: Product[]
  onOpen?: (p: Product) => void
  onAdd?: (p: Product) => void
  compact?: boolean
  emptyText?: string
}

export function ProductGrid({
  items,
  onOpen,
  onAdd,
  compact,
  emptyText,
}: Props) {
  if (items.length === 0) {
    return (
      <Text c="dimmed" ta="center" py="xl">
        {emptyText ?? 'No products'}
      </Text>
    )
  }
  return (
    <SimpleGrid
      cols={{
        base: 2,
        xs: compact ? 3 : 2,
        sm: compact ? 4 : 3,
        md: compact ? 5 : 4,
        lg: compact ? 6 : 4,
      }}
      spacing={compact ? 'sm' : 'lg'}
      verticalSpacing={compact ? 'md' : 'xl'}
    >
      {items.map((p) => (
        <ProductCard
          key={p.id}
          product={p}
          onOpen={onOpen}
          onAdd={onAdd}
          compact={compact}
        />
      ))}
    </SimpleGrid>
  )
}
