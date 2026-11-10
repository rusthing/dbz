/**
 * # 格式化工具
 *
 * 展示层统一在这里收敛：空值、时间、长 ID、错误文案。约定见仓库 README
 * 「展示约定」一节——空值一律 `--`，不区分「没有」与「空字符串」。
 */

import dayjs from 'dayjs';
import { ApiError } from '../api/client';

/** 空值占位。统一用两个连字符，避免和真正的 `-`（如正则里的减号）混淆 */
export const EMPTY_TEXT = '--';

/**
 * # 毫秒时间戳转本地时间
 *
 * 服务端的 `create_ms` / `update_ms` 是 `u64`，线上是**字符串**（`"1760000000000"`）。
 * 必须先用 `Number()` 转成数字再交给 dayjs：`dayjs("1760000000000")` 会把它当日期字符串解析，
 * 得到 1759 年这种荒唐结果。毫秒时间戳约 `1.7e12`，远小于 `2^53`，转数字不会丢精度。
 */
export function formatMs(ms?: string | number | null): string {
  if (ms === null || ms === undefined || ms === '') {
    return EMPTY_TEXT;
  }
  const value = Number(ms);
  if (!Number.isFinite(value) || value <= 0) {
    return EMPTY_TEXT;
  }
  return dayjs(value).format('YYYY-MM-DD HH:mm:ss');
}

/** # 空值兜底：只把「没有内容」归为 `--`，数字 0 原样展示 */
export function orDash(value?: string | number | null): string {
  if (value === undefined || value === null) {
    return EMPTY_TEXT;
  }
  const text = String(value);
  return text.trim() === '' ? EMPTY_TEXT : text;
}

/**
 * # 长 ID 截断
 *
 * 雪花 ID 有 19 位，整段铺在列表里既占宽又难读；保留头尾、中间省略号，
 * 全量值由调用方放到 Tooltip / title 里。
 */
export function shortId(id?: string | null): string {
  if (!id) {
    return EMPTY_TEXT;
  }
  return id.length <= 10 ? id : `${id.slice(0, 6)}…${id.slice(-4)}`;
}

/**
 * # 耗时
 *
 * 后端返回的都是毫秒。小于 1 秒按毫秒展示（运维关心 30ms 还是 80ms），
 * 超过 1 秒换成秒，避免出现「耗时 12345ms」这种难读的数字。
 */
export function formatDuration(ms?: number | null): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms) || ms < 0) {
    return EMPTY_TEXT;
  }
  if (ms < 1000) {
    return `${Math.round(ms)} ms`;
  }
  if (ms < 60_000) {
    return `${(ms / 1000).toFixed(1)} s`;
  }
  const minutes = Math.floor(ms / 60_000);
  const seconds = Math.round((ms % 60_000) / 1000);
  return `${minutes} 分 ${seconds} 秒`;
}

/**
 * # 业务编码到可操作提示
 *
 * 服务端已经给了「哪个键重复 / 哪条外键不存在」这类细节，前端再补一句
 * 「接下来该怎么做」，避免运维只看到一句数据库错误。
 */
const CODE_HINTS: Record<string, string> = {
  RTW00001: '名称已被占用，请换一个再保存',
  RTW00002: '关联的集群或上游连接器不存在，请刷新后重新选择',
  RTW00003: '仍有关联记录，请先处理关联记录再删除',
};

/** HTTP 层错误的兜底提示：这些情况下服务端通常没有业务文案 */
const HTTP_HINTS: Record<number, string> = {
  401: '登录状态已失效，请重新登录',
  403: '当前账号没有权限执行该操作',
  404: '接口不存在，请检查网关前缀与反代配置',
  409: '数据已被修改，请刷新后重试',
  413: '提交内容过大，请缩短后重试',
  422: '提交内容未通过服务端校验',
  500: '服务端异常，请稍后重试',
  502: '后端服务暂不可用，请稍后重试',
  503: '后端服务暂不可用，请稍后重试',
  504: '后端响应超时，请稍后重试',
};

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
 * 已知业务编码 / HTTP 状态额外补一句「怎么办」。
 */
export function errorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    const base = error.message || '操作失败，请稍后重试';
    const codeHint = error.code ? CODE_HINTS[error.code] : undefined;
    if (codeHint) {
      return `${base}（${codeHint}）`;
    }
    const statusHint = error.status ? HTTP_HINTS[error.status] : undefined;
    if (statusHint) {
      // 422 多来自框架层请求体反序列化：原文里有字段名与期望类型，比笼统提示有用
      const detail = error.status === 422 && error.detail
        ? `：${error.detail.slice(0, 180)}`
        : '';
      return `${base}（${statusHint}${detail}）`;
    }
    return base;
  }
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
