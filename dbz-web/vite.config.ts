import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

/**
 * # Vite 配置
 *
 * 开发态把 `/dbz/**` 反代到 dbz-svr，避免跨域；生产态由网关或 nginx 承担同样的转发，
 * 因此前端代码里始终使用 `/dbz/...` 这样的同源相对路径。
 */
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const proxyTarget = env.VITE_PROXY_TARGET || 'http://127.0.0.1:9006';

  return {
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
