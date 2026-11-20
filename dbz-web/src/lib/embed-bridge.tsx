/**
 * uiam 嵌入桥（子应用端）。
 *
 * 只在 URL 带 `?embed=1` 时启用；独立访问时所有函数都是空操作，不改变现有行为。
 * 协议与宿主 uiam-web/src/apps/bridge.ts 对应：同源 postMessage + 严格校验来源。
 */
import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'

const CHANNEL = 'rusthing-uiam-embed'
const THEME_EVENT = 'rusthing:embed-theme'
const NAV_EVENT = 'rusthing:embed-navigate'
const REFRESH_EVENT = 'rusthing:embed-refresh'
const SESSION_EXPIRED_EVENT = 'rusthing:session-expired'

type Theme = 'light' | 'dark'
type Level = 'success' | 'error' | 'info'

let currentAppId = ''

export function isEmbedded(): boolean {
  return new URLSearchParams(window.location.search).get('embed') === '1'
}

/** 跨域开发联调时宿主在另一台 dev server 上，用 referrer 反推它的 origin */
function hostOrigin(): string {
  try {
    return new URL(document.referrer).origin
  } catch {
    return ''
  }
}

function post(type: string, payload?: Record<string, unknown>) {
  if (!isEmbedded() || window.parent === window || !currentAppId) return
  window.parent.postMessage(
    { channel: CHANNEL, appId: currentAppId, type, payload },
    hostOrigin() || window.location.origin,
  )
}

/** 在 createRoot 之前调用一次：装监听器并在文档就绪后回报 ready */
export function initEmbed(appId: string) {
  currentAppId = appId
  if (!isEmbedded()) return
  document.documentElement.dataset.embed = '1'

  window.addEventListener('message', (event) => {
    if (event.origin !== window.location.origin && event.origin !== hostOrigin()) return
    if (event.source !== window.parent) return
    const data = event.data as
      | { channel?: string; appId?: string; type?: string; payload?: Record<string, unknown> }
      | null
    if (!data || data.channel !== CHANNEL || data.appId !== appId) return

    if (data.type === 'init' || data.type === 'theme') {
      const theme = data.payload?.theme
      if (theme === 'light' || theme === 'dark') {
        document.documentElement.dataset.theme = theme
        window.dispatchEvent(new CustomEvent<Theme>(THEME_EVENT, { detail: theme }))
      }
      if (data.type === 'init') {
        const route = data.payload?.route
        if (typeof route === 'string' && route) {
          window.dispatchEvent(new CustomEvent<string>(NAV_EVENT, { detail: route }))
        }
      }
    } else if (data.type === 'refresh') {
      window.dispatchEvent(new Event(REFRESH_EVENT))
    }
  })

  const ready = () => post('ready')
  if (document.readyState === 'complete') ready()
  else window.addEventListener('load', ready, { once: true })

  window.addEventListener(SESSION_EXPIRED_EVENT, () => post('session-expired'))
}

/** 当前页面标题与路由回报给宿主，用于窗口标题栏 */
export function reportView(title: string, path: string) {
  post('title', { title })
  post('route', { path })
}

export function reportToast(level: Level, text: string) {
  post('notify', { level, text })
}

/** 宿主下发的主题；非嵌入态始终 light，由各应用自己的设置决定 */
export function useEmbedTheme(): Theme {
  const [theme, setTheme] = useState<Theme>(() =>
    document.documentElement.dataset.theme === 'dark' ? 'dark' : 'light',
  )
  useEffect(() => {
    const handler = (event: Event) => setTheme((event as CustomEvent<Theme>).detail)
    window.addEventListener(THEME_EVENT, handler)
    return () => window.removeEventListener(THEME_EVENT, handler)
  }, [])
  return theme
}

/** 读取宿主 init 消息里带的初始路由；读完即失效，避免刷新时又跳一次 */
export function consumeInitialRoute(): string | null {
  const url = new URL(window.location.href)
  const route = url.searchParams.get('route')
  if (route) url.searchParams.delete('route')
  if (route) window.history.replaceState(window.history.state, '', url.toString())
  return route
}

/** 放在 Router 内：响应宿主的初始路由与后续 navigate 指令 */
export function EmbedNavigation() {
  const navigate = useNavigate()
  useEffect(() => {
    const initial = consumeInitialRoute()
    if (initial) navigate(initial, { replace: true })
    const onNavigate = (event: Event) => {
      const path = (event as CustomEvent<string>).detail
      if (path) navigate(path)
    }
    const onRefresh = () => window.location.reload()
    window.addEventListener(NAV_EVENT, onNavigate)
    window.addEventListener(REFRESH_EVENT, onRefresh)
    return () => {
      window.removeEventListener(NAV_EVENT, onNavigate)
      window.removeEventListener(REFRESH_EVENT, onRefresh)
    }
  }, [navigate])
  return null
}
