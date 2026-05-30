import { useEffect, useRef, useState } from 'react'
import {
  AppShell,
  Group,
  Title,
  Container,
  Text,
  Box,
  Stack,
  TextInput,
  Button,
  Badge,
  Divider,
  Grid,
  AspectRatio,
  Alert,
  Center,
  Loader,
  ActionIcon,
  Tabs,
} from '@mantine/core'
import {
  IconSearch,
  IconSparkles,
  IconFlame,
  IconAlertCircle,
  IconShoppingBagPlus,
  IconTrash,
  IconHeart,
  IconBarcode,
  IconHome,
  IconShoppingCart,
  IconLayersIntersect,
} from '@tabler/icons-react'
import { api } from './api'
import type { HomeResponse, Product } from './types'
import { ProductGrid } from './components/ProductGrid'
import './App.css'

const SAMPLE_IDS = [
  '00066fdcf5f0da690b898b287d05ce477bd2764ce975d138489da03510a42833',
  '0012ac5495638a445c5ac97d8080616ed98bc8f2dbffb22af8833b75ea23e085',
  '001324f693acaea0dea35333ba00ccddd0162d8bc81e76865034fca36cbb89c8',
  '001acb589c6b72863cc8bc9e95c6c7277a549ddff2314238651de8f475820568',
  '0027217107e6543586c853357873d338b57e3f624ea6222c94a30fbee728ba4d',
  '0027b54e5fcbcfcf54c348c4b1a34aac0fb1c9cd1e2bc409776419a263ce7d64',
]

export default function App() {
  const [activeTab, setActiveTab] = useState<string>('f2')
  const [customerId, setCustomerId] = useState('')
  const [home, setHome] = useState<HomeResponse | null>(null)
  const [homeLoading, setHomeLoading] = useState(false)
  const [homeError, setHomeError] = useState<string | null>(null)

  const [productIdInput, setProductIdInput] = useState('')
  const [selected, setSelected] = useState<Product | null>(null)
  const [similar, setSimilar] = useState<Product[]>([])
  const [similarLoading, setSimilarLoading] = useState(false)
  const [similarError, setSimilarError] = useState<string | null>(null)

  const [cart, setCart] = useState<Product[]>([])
  const [crossSell, setCrossSell] = useState<Product[]>([])
  const [crossLoading, setCrossLoading] = useState(false)

  const similarRef = useRef<HTMLDivElement>(null)
  const cartRef = useRef<HTMLDivElement>(null)

  async function loadHome(id: string) {
    const trimmed = id.trim()
    if (!trimmed) return
    setHomeLoading(true)
    setHomeError(null)
    try {
      const res = await api.home(trimmed)
      setHome(res)
    } catch (e) {
      setHomeError((e as Error).message)
      setHome(null)
    } finally {
      setHomeLoading(false)
    }
  }

  async function selectProductById(id: string) {
    const trimmed = id.trim()
    if (!trimmed) return
    setSimilarError(null)
    try {
      const product = await api.product(trimmed)
      setSelected(product)
      setProductIdInput(product.id)
      setActiveTab('f4')
    } catch (e) {
      setSimilarError((e as Error).message)
      setSelected(null)
      setSimilar([])
    }
  }

  function pickProduct(p: Product) {
    setSelected(p)
    setProductIdInput(p.id)
    setActiveTab('f4')
  }

  function addToCart(p: Product) {
    setCart((prev) => (prev.some((x) => x.id === p.id) ? prev : [...prev, p]))
    setActiveTab('f3')
  }

  function removeFromCart(id: string) {
    setCart((prev) => prev.filter((p) => p.id !== id))
  }

  useEffect(() => {
    if (!selected) {
      setSimilar([])
      return
    }
    let cancelled = false
    setSimilarLoading(true)
    api
      .similar(selected.id)
      .then((res) => {
        if (!cancelled) setSimilar(res.items)
      })
      .catch(() => {
        if (!cancelled) setSimilar([])
      })
      .finally(() => {
        if (!cancelled) setSimilarLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [selected])

  useEffect(() => {
    if (cart.length === 0) {
      setCrossSell([])
      return
    }
    let cancelled = false
    setCrossLoading(true)
    api
      .cartRecs(cart.map((i) => i.id))
      .then((res) => {
        if (!cancelled) setCrossSell(res.recommendations.slice(0, 6))
      })
      .catch(() => {
        if (!cancelled) setCrossSell([])
      })
      .finally(() => {
        if (!cancelled) setCrossLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [cart])

  const cartTotal = cart.reduce((sum, i) => sum + (i.price ?? 0), 0)

  return (
    <AppShell header={{ height: 64 }} padding={0}>
      <AppShell.Header style={{ border: 0 }}>
        <Box style={{ background: 'var(--green)', height: '100%', borderBottom: '1px solid #000' }}>
          <Container size="xl" h="100%">
            <Group h="100%" justify="space-between">
              <Group gap="xs">
                <Box
                  style={{
                    background: 'var(--yellow)',
                    color: '#000',
                    padding: '6px 12px',
                    fontWeight: 800,
                    fontSize: 18,
                    letterSpacing: 2,
                  }}
                >
                  H&M
                </Box>
                <Text size="xs" tt="uppercase" style={{ letterSpacing: 2, color: '#fff', fontWeight: 700 }}>
                  Dataset
                </Text>
              </Group>
              <Group gap="sm">
                <Badge
                  variant="filled"
                  styles={{ root: { color: '#000', background: 'var(--yellow)', fontWeight: 700 } }}
                >
                  {cart.length} in cart
                </Badge>
                <Button
                  size="sm"
                  styles={{ root: { background: '#000', color: '#fff' } }}
                  onClick={() => setActiveTab('f3')}
                >
                  Xem giỏ hàng
                </Button>
              </Group>
            </Group>
          </Container>
        </Box>
      </AppShell.Header>

      <AppShell.Main>
        <Container size="xl" py={32} px="md">
          <Tabs
            value={activeTab}
            onChange={(v) => v && setActiveTab(v)}
            variant="default"
            styles={{
              list: { borderBottom: "1px solid #000", gap: 0 },
              tab: {
                border: "1px solid #000",
                borderBottom: "none",
                background: "#fff",
                color: "#000",
                fontWeight: 700,
                textTransform: "uppercase",
                letterSpacing: 1,
                fontSize: 12,
                marginRight: -1,
                padding: "14px 20px",
              },
              panel: { paddingTop: 32 },
            }}
          >
            <Tabs.List>
              <Tabs.Tab
                value="f2"
                leftSection={<IconHome size={14} />}
                style={activeTab === "f2" ? { background: "var(--green)", color: "#fff" } : {}}
              >
                Feature 1 · Homepage
              </Tabs.Tab>
              <Tabs.Tab
                value="f3"
                leftSection={<IconShoppingCart size={14} />}
                rightSection={cart.length > 0 ? <Badge size="xs" variant="filled" styles={{ root: { background: "var(--yellow)", color: "#000", fontWeight: 700 } }}>{cart.length}</Badge> : null}
                style={activeTab === "f3" ? { background: "var(--green)", color: "#fff" } : {}}
              >
                Feature 2 · Cart · Cross-sell
              </Tabs.Tab>
              <Tabs.Tab
                value="f4"
                leftSection={<IconLayersIntersect size={14} />}
                style={activeTab === "f4" ? { background: "var(--green)", color: "#fff" } : {}}
              >
                Feature 3 · Up-sell · Similar
              </Tabs.Tab>
            </Tabs.List>

            <Tabs.Panel value="f2">
            {/* ========== FEATURE 1: HOMEPAGE ========== */}
            <Section
              tag="Feature 1"
              kicker="Homepage"
              title="Gợi ý cá nhân hóa theo Customer ID"
              desc="Nhập customer_id → nếu tồn tại trong final_recommendations.csv hiện 12 sản phẩm AI cá nhân hóa. Không có → fallback sang global_trending.json."
            >
              <form
                onSubmit={(e) => {
                  e.preventDefault()
                  loadHome(customerId)
                }}
              >
                <Group gap={0} wrap="nowrap" style={{ border: '1px solid #000', background: '#fff' }}>
                  <TextInput
                    placeholder="Nhập customer_id (64 ký tự hex)"
                    value={customerId}
                    onChange={(e) => setCustomerId(e.currentTarget.value)}
                    leftSection={<IconSearch size={16} />}
                    size="lg"
                    variant="unstyled"
                    style={{ flex: 1, minWidth: 280, paddingLeft: 8 }}
                  />
                  <Button type="submit" size="lg" color="brand.6" loading={homeLoading} style={{ height: 52 }}>
                    Xem gợi ý
                  </Button>
                </Group>
              </form>
              <Group gap="xs" mt="sm">
                <Text size="xs" style={{ color: '#000' }}>
                  Thử nhanh:
                </Text>
                {SAMPLE_IDS.map((id, i) => (
                  <Text
                    key={id}
                    size="xs"
                    className="link-hover"
                    style={{ cursor: 'pointer', color: '#000', fontWeight: 700 }}
                    onClick={() => {
                      setCustomerId(id)
                      loadHome(id)
                    }}
                  >
                    Khách {i + 1}
                  </Text>
                ))}
                <Text size="xs" style={{ color: '#000' }}>
                  /
                </Text>
                <Text
                  size="xs"
                  className="link-hover"
                  style={{ cursor: 'pointer', color: '#000', fontWeight: 700 }}
                  onClick={() => loadHome('new_user_demo')}
                >
                  Khách hàng mới (fallback)
                </Text>
              </Group>

              {homeError && (
                <Alert icon={<IconAlertCircle size={16} />} color="red" variant="outline" mt="md">
                  {homeError}
                </Alert>
              )}

              {homeLoading && (
                <Center py={32}>
                  <Loader type="dots" color="brand.6" />
                </Center>
              )}

              {home && !homeLoading && (
                <Stack gap="md" mt="lg" className="fade-in">
                  <Group gap="xs">
                    {home.source === 'personalized' ? (
                      <IconSparkles size={16} />
                    ) : (
                      <IconFlame size={16} />
                    )}
                    <Text size="xs" tt="uppercase" style={{ letterSpacing: 2, fontWeight: 700, color: '#000' }}>
                      {home.source === 'personalized' ? 'Curated for you' : 'Trending now'}
                    </Text>
                    <Badge
                      variant="filled"
                      color={home.source === 'personalized' ? 'brand.6' : 'sun.4'}
                      styles={{
                        root: {
                          color: home.source === 'personalized' ? '#fff' : '#000',
                          textTransform: 'uppercase',
                          letterSpacing: 1,
                        },
                      }}
                    >
                      {home.source === 'personalized' ? 'Personalized' : 'Fallback'}
                    </Badge>
                    <Text size="xs" ff="monospace" style={{ color: '#000' }}>
                      ID · {home.customerId.slice(0, 16)}…
                    </Text>
                  </Group>
                  <ProductGrid items={home.items} onOpen={pickProduct} onAdd={addToCart} />
                </Stack>
              )}
            </Section>

            </Tabs.Panel>

            <Tabs.Panel value="f3">
            {/* ========== FEATURE 2: CART CROSS-SELL ========== */}
            <div ref={cartRef}>
              <Section
                tag="Feature 2"
                kicker="Cart · Cross-sell"
                title="Giỏ hàng & gợi ý bán chéo"
                desc="Mỗi lần thêm sản phẩm → gọi cart_recommendations.json với từng mã, gộp mảng & loại trùng để đề xuất 6 món 'Thường được mua cùng nhau'."
              >
                {cart.length === 0 ? (
                  <Box style={{ border: '1px solid #000', background: 'var(--yellow-soft)', padding: 32, textAlign: 'center' }}>
                    <Text fw={600} style={{ color: '#000' }}>
                      Giỏ hàng trống. Nhấn "Thêm vào giỏ hàng" trên bất kỳ sản phẩm nào để bắt đầu.
                    </Text>
                  </Box>
                ) : (
                  <>
                    <Box style={{ border: '1px solid #000', background: '#fff' }}>
                      {cart.map((it, i) => (
                        <Group
                          key={it.id}
                          wrap="nowrap"
                          gap="md"
                          p="md"
                          style={{ borderTop: i === 0 ? 'none' : '1px solid #000' }}
                        >
                          <Box
                            onClick={() => pickProduct(it)}
                            style={{
                              width: 80,
                              height: 100,
                              background: 'var(--green-soft)',
                              flexShrink: 0,
                              cursor: 'pointer',
                              overflow: 'hidden',
                              border: '1px solid #000',
                            }}
                          >
                            {it.imageUrl && (
                              <img
                                src={it.imageUrl}
                                alt={it.name ?? it.id}
                                style={{ width: '100%', height: '100%', objectFit: 'cover' }}
                                onError={(e) => {
                                  const img = e.currentTarget as HTMLImageElement
                                  const fallback = `https://picsum.photos/seed/${it.id}/600/800`
                                  if (img.src !== fallback) img.src = fallback
                                  else img.style.display = 'none'
                                }}
                              />
                            )}
                          </Box>
                          <Stack gap={2} style={{ flex: 1, minWidth: 0 }}>
                            <Text size="xs" tt="uppercase" style={{ letterSpacing: 1, fontWeight: 700, color: '#000' }}>
                              {it.type ?? '—'}
                            </Text>
                            <Text
                              fw={500}
                              size="sm"
                              lineClamp={1}
                              style={{ color: '#000', cursor: 'pointer' }}
                              onClick={() => pickProduct(it)}
                            >
                              {it.name ?? it.id}
                            </Text>
                            <Text size="xs" ff="monospace" style={{ color: '#000' }}>
                              #{it.id}
                            </Text>
                          </Stack>
                          <Text fw={700} size="md" style={{ color: '#000' }}>
                            {it.price != null ? `$${it.price.toFixed(2)}` : ''}
                          </Text>
                          <ActionIcon
                            variant="subtle"
                            color="dark"
                            onClick={() => removeFromCart(it.id)}
                            aria-label="Remove"
                          >
                            <IconTrash size={16} />
                          </ActionIcon>
                        </Group>
                      ))}
                      <Group
                        justify="space-between"
                        p="md"
                        style={{ borderTop: '1px solid #000', background: 'var(--yellow-soft)' }}
                      >
                        <Text fw={700} tt="uppercase" style={{ letterSpacing: 2, color: '#000' }}>
                          Tổng cộng
                        </Text>
                        <Text fw={800} size="xl" style={{ color: '#000' }}>
                          ${cartTotal.toFixed(2)}
                        </Text>
                      </Group>
                    </Box>

                    <Divider my="xl" color="#000" />

                    <Group justify="space-between" align="flex-end" mb="md">
                      <Text size="xs" tt="uppercase" style={{ letterSpacing: 2, fontWeight: 700, color: '#000' }}>
                        Thường được mua cùng nhau · Cross-sell
                      </Text>
                      <Text size="sm" fw={600} style={{ color: '#000' }}>
                        {crossSell.length} sản phẩm
                      </Text>
                    </Group>

                    {crossLoading ? (
                      <Center py={32}>
                        <Loader type="dots" color="brand.6" />
                      </Center>
                    ) : (
                      <ProductGrid
                        items={crossSell}
                        onOpen={pickProduct}
                        onAdd={addToCart}
                        emptyText="Không có gợi ý cho giỏ hiện tại."
                      />
                    )}
                  </>
                )}
              </Section>
            </div>

            </Tabs.Panel>

            <Tabs.Panel value="f4">
            {/* ========== FEATURE 3: SIMILAR (UP-SELL) ========== */}
            <div ref={similarRef}>
              <Section
                tag="Feature 3"
                kicker="Up-sell · Similar products"
                title="Chọn 1 sản phẩm → xem sản phẩm tương tự"
                desc="Nhập mã article_id hoặc bấm vào bất kỳ sản phẩm nào bên trên. Hệ thống sẽ tra similar_products.json[id] để lấy 6 mã hình dáng/màu sắc tương đồng."
              >
                <form
                  onSubmit={(e) => {
                    e.preventDefault()
                    selectProductById(productIdInput)
                  }}
                >
                  <Group gap={0} wrap="nowrap" style={{ border: '1px solid #000', background: '#fff' }}>
                    <TextInput
                      placeholder="Nhập article_id (10 số)"
                      value={productIdInput}
                      onChange={(e) => setProductIdInput(e.currentTarget.value)}
                      leftSection={<IconBarcode size={16} />}
                      size="lg"
                      variant="unstyled"
                      style={{ flex: 1, minWidth: 280, paddingLeft: 8 }}
                    />
                    <Button type="submit" size="lg" color="brand.6" style={{ height: 52 }}>
                      Tìm
                    </Button>
                  </Group>
                </form>
                <Group gap="xs" mt="sm">
                  <Text size="xs" style={{ color: '#000' }}>
                    Ví dụ:
                  </Text>
                  {['0909370001', '0751471001', '0189616008'].map((id) => (
                    <Text
                      key={id}
                      size="xs"
                      className="link-hover"
                      ff="monospace"
                      style={{ cursor: 'pointer', color: '#000', fontWeight: 700 }}
                      onClick={() => {
                        setProductIdInput(id)
                        selectProductById(id)
                      }}
                    >
                      {id}
                    </Text>
                  ))}
                </Group>

                {similarError && (
                  <Alert icon={<IconAlertCircle size={16} />} color="red" variant="outline" mt="md">
                    {similarError}
                  </Alert>
                )}

                {!selected && !similarError && (
                  <Box
                    mt="lg"
                    style={{
                      border: '1px dashed #000',
                      background: 'var(--yellow-soft)',
                      padding: 32,
                      textAlign: 'center',
                    }}
                  >
                    <Text fw={600} style={{ color: '#000' }}>
                      Chưa chọn sản phẩm. Nhập mã hoặc bấm 1 sản phẩm ở trên.
                    </Text>
                  </Box>
                )}

                {selected && (
                  <Stack gap={0} mt="lg" className="fade-in">
                    <Grid gutter={0} style={{ border: '1px solid #000', background: '#fff' }}>
                      <Grid.Col span={{ base: 12, md: 5 }}>
                        <AspectRatio ratio={4 / 5}>
                          <Box style={{ background: 'var(--green-soft)', position: 'relative', borderRight: '1px solid #000' }}>
                            {selected.imageUrl && (
                              <img
                                src={selected.imageUrl}
                                alt={selected.name ?? selected.id}
                                style={{
                                  width: '100%',
                                  height: '100%',
                                  objectFit: 'cover',
                                  position: 'absolute',
                                  inset: 0,
                                }}
                                onError={(e) => {
                                  const img = e.currentTarget as HTMLImageElement
                                  const fallback = `https://picsum.photos/seed/${selected.id}/600/800`
                                  if (img.src !== fallback) img.src = fallback
                                  else img.style.display = 'none'
                                }}
                              />
                            )}
                          </Box>
                        </AspectRatio>
                      </Grid.Col>
                      <Grid.Col span={{ base: 12, md: 7 }}>
                        <Stack gap="md" p="xl" h="100%">
                          <Badge
                            variant="filled"
                            color="sun.4"
                            styles={{ root: { color: '#000', textTransform: 'uppercase', letterSpacing: 1 } }}
                            w="fit-content"
                          >
                            {selected.type ?? '—'}
                          </Badge>
                          <Title order={2} fw={700} style={{ fontSize: 32, letterSpacing: -0.5, color: '#000' }}>
                            {selected.name ?? selected.id}
                          </Title>
                          <Text size="xs" ff="monospace" style={{ color: '#000' }}>
                            Article · {selected.id}
                          </Text>
                          <Title order={3} fw={800} style={{ fontSize: 32, color: '#000' }}>
                            {selected.price != null ? `$${selected.price.toFixed(2)}` : 'N/A'}
                          </Title>

                          <Box mt="xs" p="sm" style={{ border: '1px solid #000', background: 'var(--yellow-soft)' }}>
                            <Text size="xs" tt="uppercase" style={{ letterSpacing: 2, fontWeight: 700, color: '#000' }} mb={6}>
                              Metadata · Dictionary lookup
                            </Text>
                            <Stack gap={2}>
                              <MetaRow label="prod_name" value={selected.name ?? '—'} />
                              <MetaRow label="product_type_name" value={selected.type ?? '—'} />
                              <MetaRow
                                label="display_price"
                                value={selected.price != null ? `$${selected.price.toFixed(2)}` : '—'}
                              />
                              <MetaRow label="image_folder" value={selected.imageFolder ?? '—'} />
                              <MetaRow
                                label="image_path"
                                value={`images/${selected.imageFolder ?? '---'}/${selected.id}.jpg`}
                              />
                            </Stack>
                          </Box>

                          <Group gap="xs" mt="sm">
                            <Button
                              size="lg"
                              color="brand.6"
                              leftSection={<IconShoppingBagPlus size={18} />}
                              onClick={() => addToCart(selected)}
                            >
                              Thêm vào giỏ hàng
                            </Button>
                            <ActionIcon size={50} variant="outline" style={{ border: '1px solid #000', color: '#000' }}>
                              <IconHeart size={18} stroke={1.5} />
                            </ActionIcon>
                            <Button
                              size="lg"
                              variant="subtle"
                              color="dark"
                              onClick={() => setSelected(null)}
                            >
                              Bỏ chọn
                            </Button>
                          </Group>
                        </Stack>
                      </Grid.Col>
                    </Grid>

                    <Divider color="#000" my="xl" />

                    <Group justify="space-between" align="flex-end" mb="md">
                      <Text size="xs" tt="uppercase" style={{ letterSpacing: 2, fontWeight: 700, color: '#000' }}>
                        Sản phẩm tương tự bạn có thể thích
                      </Text>
                      <Text size="sm" fw={600} style={{ color: '#000' }}>
                        {similar.length} sản phẩm
                      </Text>
                    </Group>

                    {similarLoading ? (
                      <Center py={32}>
                        <Loader type="dots" color="brand.6" />
                      </Center>
                    ) : (
                      <ProductGrid
                        items={similar}
                        onOpen={pickProduct}
                        onAdd={addToCart}
                        emptyText="Không có sản phẩm tương tự."
                      />
                    )}
                  </Stack>
                )}
              </Section>
            </div>
            </Tabs.Panel>
          </Tabs>
        </Container>
      </AppShell.Main>
    </AppShell>
  )
}

function MetaRow({ label, value }: { label: string; value: string }) {
  return (
    <Group gap="xs" wrap="nowrap">
      <Text size="xs" ff="monospace" style={{ color: '#000', opacity: 0.7, minWidth: 130 }}>
        {label}
      </Text>
      <Text size="xs" ff="monospace" fw={600} style={{ color: '#000', wordBreak: 'break-all' }}>
        {value}
      </Text>
    </Group>
  )
}

function Section({
  tag,
  kicker,
  title,
  desc,
  children,
}: {
  tag: string
  kicker: string
  title: string
  desc: string
  children: React.ReactNode
}) {
  return (
    <Box>
      <Group gap="xs" mb={4}>
        <Badge
          variant="filled"
          color="sun.4"
          styles={{ root: { color: '#000', textTransform: 'uppercase', letterSpacing: 1, fontWeight: 700 } }}
        >
          {tag}
        </Badge>
        <Text size="xs" tt="uppercase" style={{ letterSpacing: 2, fontWeight: 700, color: '#000' }}>
          {kicker}
        </Text>
      </Group>
      <Title order={2} fw={700} mb="xs" style={{ fontSize: 32, letterSpacing: -0.5, color: '#000' }}>
        {title}
      </Title>
      <Text size="sm" mb="lg" maw={760} style={{ color: '#000' }}>
        {desc}
      </Text>
      {children}
    </Box>
  )
}
