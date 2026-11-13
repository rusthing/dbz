/**
 * # 统一登录骨架（E2 光轴穿联 · 定稿）
 *
 * 七个前端共用同一份组件：只负责视图、字段校验、错误呈现与提交节流，
 * 认证调用、returnUrl 安全校验与业务错误文案仍留在各项目自己的登录页里。
 *
 * 页面只渲染四样东西：字标（mark + name）、一句逐站独立的 tagline、
 * 两个输入框 / 登录按钮、插画左下角一行极短术语 shortTerms。
 * 不渲染说明卡、要点列表、统计浮卡或系统切换按钮；
 * 业务差异全部来自 BRANDS[brand]，本文件里不写任何站点判断。
 */

import { useState, type FormEvent, type ReactNode } from "react";
import { BRANDS, type BrandKey } from "./brands";
import "./kit.css";

export interface LoginShellProps {
  brand: BrandKey;
  /** 由各项目的认证逻辑实现；抛出的 Error.message 会显示在错误条 */
  onSubmit: (account: string, password: string) => Promise<void>;
  /** 表单上方提示，例如「登录状态已失效」 */
  banner?: ReactNode;
  submitLabel?: string;
  accountPlaceholder?: string;
  passwordPlaceholder?: string;
}

function EyeIcon({ open }: { open: boolean }) {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M2 12s3.6-6.5 10-6.5S22 12 22 12s-3.6 6.5-10 6.5S2 12 2 12Z" />
      <circle cx="12" cy="12" r="2.7" />
      {/* 口令可见时才画斜线（提示「点击隐藏」），与 antd Input.Password 的语义一致 */}
      {open && <path d="M4 4l16 16" />}
    </svg>
  );
}

function ArrowIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M5 12h13M13 6l6 6-6 6" />
    </svg>
  );
}

function Spinner() {
  return (
    <svg className="sl-spinner" width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <circle cx="12" cy="12" r="9" stroke="currentColor" strokeOpacity="0.3" strokeWidth="3" />
      <path d="M21 12a9 9 0 0 0-9-9" stroke="currentColor" strokeWidth="3" strokeLinecap="round" />
    </svg>
  );
}

export function LoginShell({
  brand,
  onSubmit,
  banner,
  submitLabel,
  accountPlaceholder,
  passwordPlaceholder,
}: LoginShellProps) {
  const data = BRANDS[brand];
  const [account, setAccount] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (busy) return;
    if (!account.trim() || !password) {
      setError("请输入账号与口令");
      return;
    }
    setBusy(true);
    setError("");
    try {
      await onSubmit(account.trim(), password);
    } catch (reason) {
      setError(
        reason instanceof Error
          ? reason.message
          : typeof reason === "string"
            ? reason
            : "登录失败，请稍后重试",
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="sl-shell" data-brand={data.key} data-login-page>
      <div className="sl-bg" aria-hidden="true">
        <span className="sl-arc" />
        <span className="sl-arc2" />
      </div>

      <div className="sl-card">
        <div className="sl-art">
          {data.glyph}
          <span className="sl-cap">{data.shortTerms}</span>
        </div>

        <span className="sl-axis" aria-hidden="true" />
        <span className="sl-node" style={{ top: "26%" }} aria-hidden="true" />
        <span className="sl-node" style={{ top: "50%" }} aria-hidden="true" />
        <span className="sl-node" style={{ top: "74%" }} aria-hidden="true" />

        <main className="sl-pane">
          <header className="sl-top">
            <span className="sl-mark">{data.mark}</span>
            <span className="sl-name">{data.name}</span>
          </header>

          <h1 className="sl-tagline">{data.tagline}</h1>
          <div className="sl-rule" aria-hidden="true" />

          {banner && <div className="sl-banner" role="status">{banner}</div>}

          <form className="sl-form" onSubmit={handleSubmit} noValidate>
            <label className="sl-field">
              <span className="sl-label">账号</span>
              <span className="sl-input-wrap">
                <input
                  className="sl-input"
                  data-login-account
                  type="text"
                  autoFocus
                  autoComplete="username"
                  value={account}
                  placeholder={accountPlaceholder ?? data.accountPlaceholder}
                  disabled={busy}
                  onChange={(event) => {
                    setAccount(event.target.value);
                    setError("");
                  }}
                />
              </span>
            </label>

            <label className="sl-field">
              <span className="sl-label">口令</span>
              <span className="sl-input-wrap">
                <input
                  className="sl-input"
                  data-login-password
                  type={showPassword ? "text" : "password"}
                  autoComplete="current-password"
                  value={password}
                  placeholder={passwordPlaceholder ?? data.passwordPlaceholder}
                  disabled={busy}
                  onChange={(event) => {
                    setPassword(event.target.value);
                    setError("");
                  }}
                />
                <button
                  className="sl-eye"
                  type="button"
                  aria-label={showPassword ? "隐藏口令" : "显示口令"}
                  onClick={() => setShowPassword((value) => !value)}
                >
                  <EyeIcon open={showPassword} />
                </button>
              </span>
            </label>

            {error && (
              <div className="sl-error" role="alert" data-login-error>
                {error}
              </div>
            )}

            <button className="sl-submit" type="submit" data-login-submit disabled={busy}>
              {busy ? (
                <>
                  <Spinner /> {data.busyLabel}
                </>
              ) : (
                <>
                  {submitLabel ?? data.submitLabel} <ArrowIcon />
                </>
              )}
            </button>
          </form>
        </main>
      </div>
    </div>
  );
}
