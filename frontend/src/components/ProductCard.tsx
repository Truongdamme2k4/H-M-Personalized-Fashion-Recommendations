import {
  Card,
  Text,
  Group,
  Button,
  AspectRatio,
  Box,
  Stack,
  ActionIcon,
  Tooltip,
} from '@mantine/core'
import {
  IconHeart,
  IconShoppingBagPlus,
  IconSparkles,
} from '@tabler/icons-react'
import type { Product } from '../types'

interface Props {
  product: Product
  onOpen?: (p: Product) => void
  onAdd?: (p: Product) => void
  compact?: boolean
}

export function ProductCard({ product, onOpen, onAdd, compact }: Props) {
  return (
    <Card
      padding={0}
      className="product-card fade-in"
      style={{
        background: '#fff',
        border: '1px solid #000',
        cursor: 'pointer',
      }}
      onClick={() => onOpen?.(product)}
    >
      <AspectRatio ratio={3 / 4}>
        <Box
          className="product-image"
          style={{
            background: 'var(--yellow-soft)',
            position: 'relative',
            borderBottom: '1px solid #000',
          }}
        >
          {product.imageUrl && (
            <img
              src={product.imageUrl}
              alt={product.name ?? product.id}
              style={{
                width: '100%',
                height: '100%',
                objectFit: 'cover',
                display: 'block',
              }}
              onError={(e) => {
                const img = e.currentTarget as HTMLImageElement
                const fallback = `https://picsum.photos/seed/${product.id}/600/800`
                if (img.src !== fallback) {
                  img.src = fallback
                } else {
                  img.style.display = 'none'
                }
              }}
            />
          )}
          <ActionIcon
            className="wishlist-btn"
            variant="filled"
            color="white"
            size="lg"
            style={{
              position: 'absolute',
              top: 10,
              right: 10,
              background: '#fff',
              border: '1px solid #000',
              color: '#000',
            }}
            onClick={(e) => e.stopPropagation()}
            aria-label="Wishlist"
          >
            <IconHeart size={16} stroke={1.5} />
          </ActionIcon>
          {onAdd && (
            <Box
              className="quick-add"
              style={{ position: 'absolute', left: 10, right: 10, bottom: 10 }}
            >
              <Button
                fullWidth
                size={compact ? 'xs' : 'sm'}
                color="brand.9"
                leftSection={<IconShoppingBagPlus size={14} />}
                onClick={(e) => {
                  e.stopPropagation()
                  onAdd(product)
                }}
              >
                Quick add
              </Button>
            </Box>
          )}
        </Box>
      </AspectRatio>

      <Stack gap={2} p={compact ? 'xs' : 'sm'} pt={compact ? 8 : 12}>
        {product.reason && (
          <Tooltip label={product.reason} multiline w={260} withArrow>
            <Group
              gap={4}
              wrap="nowrap"
              style={{
                background: 'var(--yellow-soft)',
                border: '1px solid #000',
                padding: '2px 6px',
                marginBottom: 4,
                width: 'fit-content',
                maxWidth: '100%',
              }}
            >
              <IconSparkles size={10} stroke={2} />
              <Text
                size="10px"
                lineClamp={1}
                style={{
                  color: '#000',
                  fontWeight: 600,
                  letterSpacing: 0.3,
                }}
              >
                {product.reason}
              </Text>
            </Group>
          </Tooltip>
        )}
        <Text
          size="xs"
          tt="uppercase"
          style={{ letterSpacing: 1, fontWeight: 600, color: '#000' }}
        >
          {product.type ?? '—'}
        </Text>
        <Text
          fw={800}
          size={compact ? 'sm' : 'sm'}
          lineClamp={1}
          style={{ color: '#000' }}
          title={product.name}
        >
          {product.name ??
            (product.missing ? '(missing metadata)' : product.id)}
        </Text>
        <Group justify="space-between" align="center" mt={2}>
          <Text fw={700} size={compact ? 'sm' : 'md'} style={{ color: '#000' }}>
            {product.price != null ? `$${product.price.toFixed(2)}` : ''}
          </Text>
          <Text size="xs" ff="monospace" style={{ color: '#000' }}>
            #{product.id.slice(-6)}
          </Text>
        </Group>
      </Stack>
    </Card>
  )
}
