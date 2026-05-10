import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { MantineProvider, createTheme } from '@mantine/core'
import '@mantine/core/styles.css'
import './index.css'
import App from './App.tsx'

const theme = createTheme({
  primaryColor: 'brand',
  defaultRadius: 0,
  fontFamily:
    'Inter, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif',
  headings: {
    fontFamily: 'Inter, system-ui, sans-serif',
    fontWeight: '600',
  },
  colors: {
    brand: [
      '#ecfdf5',
      '#d1fae5',
      '#a7f3d0',
      '#6ee7b7',
      '#34d399',
      '#10b981',
      '#059669',
      '#047857',
      '#065f46',
      '#064e3b',
    ],
    sun: [
      '#fffdea',
      '#fff8c4',
      '#fff085',
      '#ffe747',
      '#ffdd1a',
      '#f7c900',
      '#dcb100',
      '#b48a00',
      '#8f6c00',
      '#6b5000',
    ],
  },
  components: {
    Button: { defaultProps: { radius: 0 } },
    Card: { defaultProps: { radius: 0 } },
    Paper: { defaultProps: { radius: 0 } },
    Badge: { defaultProps: { radius: 0 } },
    TextInput: { defaultProps: { radius: 0 } },
    ActionIcon: { defaultProps: { radius: 0 } },
    Drawer: { defaultProps: { radius: 0 } },
    Alert: { defaultProps: { radius: 0 } },
    Indicator: { defaultProps: { radius: 0 } },
  },
})

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <MantineProvider theme={theme} forceColorScheme="light">
      <App />
    </MantineProvider>
  </StrictMode>,
)
