import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

/**
 * # 规范化 base
 *
 * 允许 `VITE_PUBLIC_BASE` 写成 `dbz-web`、`/dbz-web`、`/dbz-web/` 三种形态，
 * 统一成 Vite 要求的 `/dbz-web/`；空值或 `/` 表示部署在根路径。
 */
function normalizeBase(raw?: string): string {
  const trimmed = (raw ?? '').trim();
  if (!trimmed || trimmed === '/') {
    return '/';
  }
  return `/${trimmed.replace(/^\/+|\/+$/g, '')}/`;
}

/**
 * # Vite 配置
 *
 * 开发态把 `/dbz/**` 反代到 dbz-svr，避免跨域；生产态由网关或 nginx 承担同样的转发，
 * 因此前端代码里始终使用 `/dbz/...` 这样的同源相对路径。
 *
 * `base` 由 `VITE_PUBLIC_BASE` 决定：部署在域名根路径时留空即可；挂到 `/dbz-web/`
 * 这类子路径时必须一起改，否则 `assets/*.js` 会去根路径找而 404。
 */
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const proxyTarget = env.VITE_PROXY_TARGET || 'http://127.0.0.1:9006';
  // 统一保证是「/xxx/」形式：Vite 要求 base 以 / 开头、以 / 结尾
  const base = normalizeBase(env.VITE_PUBLIC_BASE);

  return {
    base,
    plugins: [react()],
    server: {
      port: 5174,
      proxy: {
        '/dbz': {
          target: proxyTarget,
          changeOrigin: true,
        },
      },
    },
    build: {
      outDir: 'dist',
      sourcemap: false,
      // antd 本身接近 1MB（gzip 后约 330KB），拆包后仍会超过默认 500KB 阈值
      chunkSizeWarningLimit: 1100,
      rollupOptions: {
        output: {
          manualChunks: {
            react: ['react', 'react-dom', 'react-router-dom'],
            antd: ['antd'],
            icons: ['@ant-design/icons'],
          },
        },
      },
    },
  };
});
