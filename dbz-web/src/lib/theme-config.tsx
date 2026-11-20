/**
 * # antd 主题配置（独立站与联邦入口共用）
 *
 * dbz 的令牌比 msg 多两项：button.autoInsertSpace=false（中文按钮不插空格）和抬高的
 * colorTextTertiary（11–12px 说明文字的对比度），两处都要在联邦窗口里保持一致。
 */
import { theme as antdTheme, type ThemeConfig } from 'antd';
import zhCN from 'antd/locale/zh_CN';

export interface AntdThemeOptions {
  dark: boolean;
}

export function antdConfig({ dark }: AntdThemeOptions): {
  locale: typeof zhCN;
  button: { autoInsertSpace: boolean };
  theme: ThemeConfig;
} {
  return {
    locale: zhCN,
    button: { autoInsertSpace: false },
    theme: {
      algorithm: dark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
      token: {
        colorPrimary: '#1677ff',
        borderRadius: 6,
        fontSize: 14,
        // 默认的 colorTextTertiary 压在白底上只有 3.4:1，这里抬到约 4.9:1。
        colorTextTertiary: dark ? 'rgba(255,255,255,.65)' : 'rgba(0, 0, 0, 0.56)',
      },
      components: {
        Layout: {
          headerBg: dark ? '#1f1f1f' : '#ffffff',
          bodyBg: dark ? '#141414' : '#f4f6fa',
          siderBg: dark ? '#1f1f1f' : '#ffffff',
        },
        Table: {
          headerBg: dark ? '#262626' : '#fafbfd',
        },
      },
    },
  };
}
