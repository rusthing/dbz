/**
 * # 登录页
 *
 * 复用 adm-rs 的账号体系。样式沿用 dbz 的白底卡片、蓝色主色与品牌头，
 * 不引入另一套视觉语言。
 */

import { Alert, Button, Input, Typography } from 'antd';
import { useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useAuth } from '../../auth/AuthContext';

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
  const [account, setAccount] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const returnUrl = safeReturnUrl(params.get('returnUrl'));

  const submit = async () => {
    if (submitting) {
      return;
    }
    if (!account.trim() || !password) {
      setError('请填写账号与口令');
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      await login(account, password);
      navigate(returnUrl, { replace: true });
    } catch (reason) {
      // auth core 已把业务编码翻译成可读文本，这里直接展示。
      setError(reason instanceof Error ? reason.message : '登录失败');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="dbz-login" data-login-page>
      <div className="dbz-login-card">
        <div className="dbz-brand dbz-login-brand">
          <div className="dbz-brand-logo">dbz</div>
          <div className="dbz-brand-text">
            <span className="dbz-brand-title">Debezium 控制台</span>
            <span className="dbz-brand-sub">dbz-svr</span>
          </div>
        </div>

        <Typography.Title level={4} style={{ margin: '0 0 4px' }}>
          登录控制台
        </Typography.Title>
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          使用 adm-rs 的账号登录；口令在浏览器侧做 SM3 摘要后传输，不会以明文提交。
        </Typography.Text>

        <div className="dbz-login-form">
          <label htmlFor="dbz-login-account">账号</label>
          <Input
            id="dbz-login-account"
            autoFocus
            autoComplete="username"
            value={account}
            placeholder="登录名 / 手机号 / 邮箱 / 身份证号"
            data-login-account
            onChange={(event) => {
              setAccount(event.target.value);
              setError(null);
            }}
          />

          <label htmlFor="dbz-login-password">口令</label>
          <Input.Password
            id="dbz-login-password"
            autoComplete="current-password"
            value={password}
            placeholder="请输入口令"
            data-login-password
            onChange={(event) => {
              setPassword(event.target.value);
              setError(null);
            }}
            onPressEnter={() => void submit()}
          />

          {error && (
            <Alert
              type="error"
              showIcon
              message={error}
              data-login-error
              style={{ marginTop: 6 }}
            />
          )}

          <Button
            type="primary"
            block
            loading={submitting}
            data-login-submit
            style={{ marginTop: 10 }}
            onClick={() => void submit()}
          >
            登录
          </Button>
        </div>

        <div className="dbz-login-foot">
          登录态同时用于删除等高风险操作的二次确认；令牌失效会自动回到这里，重新登录后继续。
        </div>
      </div>
    </div>
  );
}
