/// <reference types="vite/client" />

/** # 环境变量 */
interface ImportMetaEnv {
  /** 接口前缀，留空表示同源 */
  readonly VITE_API_BASE?: string;
  /** 前端自身被访问的路径前缀，如 `/dbz-web`；留空表示部署在根路径 */
  readonly VITE_PUBLIC_BASE?: string;
  /** 开发态代理目标 */
  readonly VITE_PROXY_TARGET?: string;
  /** 写接口使用的当前用户ID */
  readonly VITE_USER_ID?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
