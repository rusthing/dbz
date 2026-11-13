/**
 * # 登录页
 *
 * 复用 adm-rs 的账号体系，视图统一走 `login/LoginShell`；
 * returnUrl 安全校验、认证调用与跳转仍留在本文件，测试用的 data-login-* 由骨架统一提供。
 */

import { useNavigate, useSearchParams } from 'react-router-dom';
import { useAuth } from '../../auth/AuthContext';
import { LoginShell } from '../../login/LoginShell';

/** 只接受站内绝对路径，拒绝 `//host`、反斜杠与控制字符 */
function safeReturnUrl(raw: string | null): string {
  if (
    raw &&
    raw.startsWith('/') &&
    !raw.startsWith('//') &&
    !raw.includes('\\') &&
    !/[\u0000-\u001f]/.test(raw) &&
    raw.split(/[?#]/)[0] !== '/login'
  ) {
    return raw;
  }
  return '/control-panel';
}

export default function LoginPage() {
  const { login } = useAuth();
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const returnUrl = safeReturnUrl(params.get('returnUrl'));

  async function submit(account: string, password: string) {
    // auth-core 已把业务编码翻译成可读文本，异常直接交给骨架展示。
    await login(account, password);
    navigate(returnUrl, { replace: true });
  }

  return <LoginShell brand="dbz" onSubmit={submit} />;
}
