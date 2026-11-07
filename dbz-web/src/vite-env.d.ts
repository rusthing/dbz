/// <reference types="vite/client" />

/** # 环境变量 */
interface ImportMetaEnv {
  /** 接口前缀，留空表示同源 */
  readonly VITE_API_BASE?: string;
  /** 前端自身被访问的路径前缀，如 `/dbz-web`；留空表示部署在根路径 */
  readonly VITE_PUBLIC_BASE?: string;
  /** 开发态 dbz-svr 代理目标 */
  readonly VITE_PROXY_TARGET?: string;
  /** 开发态 adm-rs 代理目标 */
  readonly VITE_ADM_PROXY_TARGET?: string;
  /** adm-rs 认证接口前缀，如 `/admrs-svr` */
  readonly VITE_AUTH_BASE?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
