/**
 * # 取消时确认脏表单
 *
 * 编辑到一半点「取消」/按 Esc / 点遮罩，默认行为会直接关掉弹窗；下次打开虽然会重新
 * `setFieldsValue` 回滚，但用户往往以为「刚才的修改已经保存了」。这里在字段被真正
 * 改动过时先问一句，确认后丢弃并重置，保证「取消 = 回退」。
 *
 * 回填完成后由表单调用 `markPristine()` 记录基线，仅浏览不会触发确认。
 */

import { App as AntdApp } from 'antd';
import type { FormInstance } from 'antd';
import { useCallback, useRef } from 'react';

interface CancelConfirm {
  /** 挂到 Modal 的 onCancel */
  handleCancel: () => void;
  /** 回填完成后调用，记录「打开时」的值作为脏判断基线 */
  markPristine: () => void;
}

export function useCancelConfirm(form: FormInstance, onCancel: () => void): CancelConfirm {
  const { modal } = AntdApp.useApp();
  /**
   * 不能用 `isFieldsTouched()`：antd 的 `setFieldsValue` 从 external 来源写入时
   * 也会把字段标记为 touched（见 rc-field-form Field.tsx 的 valueUpdate 分支），
   * 编辑回填后会被误判成「改过」。这里比对「打开时快照 vs 当前值」。
   */
  const pristineRef = useRef('');

  const markPristine = useCallback(() => {
    pristineRef.current = JSON.stringify(form.getFieldsValue(true));
  }, [form]);

  const handleCancel = useCallback(() => {
    // 只浏览、没改过：直接关闭，不打扰用户
    if (JSON.stringify(form.getFieldsValue(true)) === pristineRef.current) {
      onCancel();
      return;
    }
    modal.confirm({
      title: '放弃未保存的修改？',
      content: '关闭后本次修改不会保留，重新打开会恢复为原值。',
      okText: '放弃修改',
      okButtonProps: { danger: true },
      cancelText: '继续编辑',
      onOk: () => {
        form.resetFields();
        onCancel();
      },
    });
  }, [form, modal, onCancel]);

  return { handleCancel, markPristine };
}
