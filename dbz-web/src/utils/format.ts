/**
 * # 格式化工具
 */

import dayjs from 'dayjs';

/**
 * # 毫秒时间戳转本地时间
 *
 * 服务端的 `create_ms` / `update_ms` 是 u64 毫秒时间戳。
 */
export function formatMs(ms?: number | null): string {
  if (!ms) {
    return '-';
  }
  return dayjs(ms).format('YYYY-MM-DD HH:mm:ss');
}

/** # 空值兜底 */
export function orDash(value?: string | number | null): string {
  if (value === undefined || value === null || value === '') {
    return '-';
  }
  return String(value);
}

/** # 美化 JSON 文本，解析失败时原样返回 */
export function prettyJson(text?: string | null): string {
  if (!text) {
    return '';
  }
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}

/**
 * # 从异常中提取可展示的文案
 *
 * 表单校验失败抛出的是 antd 的 `errorFields` 对象，不应该直接甩给用户看。
 */
export function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === 'string') {
    return error;
  }
  return '操作失败，请稍后重试';
}

/** # 字符串截断，用于表格里展示长 JSON */
export function truncate(text: string, max = 60): string {
  return text.length > max ? `${text.slice(0, max)}…` : text;
}
