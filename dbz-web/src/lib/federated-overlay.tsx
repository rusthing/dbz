/**
 * # Modal / Drawer 的挂载点
 *
 * antd 默认把对话框挂到 `document.body`：嵌进 uiam 桌面后，遮罩和对话框按整块
 * 屏幕居中，会盖住别的窗口，窗口挪小了对话框还会跑到窗口外面。
 *
 * 这里改成挂当前窗口的根节点（`.dbz-federated`）：
 * - 遮罩只盖这一个窗口，对话框在窗口内容区里居中；
 * - uiam 给 `.federation-app` 加的 `transform` 正好充当 `fixed` 后代的包含块，
 *   所以挂根节点后对话框仍然是「相对窗口」而不是「相对桌面」；
 * - 主题令牌（`.dbz-federated[data-theme]`）是根节点上的变量，天然命中。
 *
 * 独立站由 main.tsx 传入 `#root`，与原来的 body 行为等价；没有提供解析函数的
 * 场景（例如组件被单独渲染）退回 `document.body`。
 */
import { createContext, useContext, useMemo, type ReactNode } from 'react';
import { Drawer as AntDrawer, Modal as AntModal } from 'antd';
import type { DrawerProps, ModalProps } from 'antd';

type ContainerResolver = () => HTMLElement | null;

const OverlayContainerContext = createContext<ContainerResolver | null>(null);

/** 入口处提供「本窗口根节点」的解析函数 */
export function FederatedOverlayRoot({
  resolve,
  children,
}: {
  resolve: ContainerResolver;
  children: ReactNode;
}) {
  return <OverlayContainerContext.Provider value={resolve}>{children}</OverlayContainerContext.Provider>;
}

/** 取挂载点函数：每次打开对话框时才解析，避免 ref 尚未赋值时定死。 */
function useContainer(): () => HTMLElement {
  const resolve = useContext(OverlayContainerContext);
  return useMemo(() => () => resolve?.() ?? document.body, [resolve]);
}

export function Modal(props: ModalProps) {
  const container = useContainer();
  return <AntModal {...props} getContainer={props.getContainer ?? container} />;
}

export function Drawer(props: DrawerProps) {
  const container = useContainer();
  return <AntDrawer {...props} getContainer={props.getContainer ?? container} />;
}
