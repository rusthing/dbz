/**
 * # 集群表单
 *
 * 新增与编辑共用一个弹窗：有 `record` 就是编辑，没有就是新增。
 *
 * 地址（`url`）是集群的唯一业务标识，也是健康检查的探测目标；服务端在**改地址或禁用**时
 * 会先把旧地址上的连接器全部注销，所以这里给出明确提示，避免运维误以为只是换个字符串。
 */

import { App as AntdApp, Form, Input, Modal, Switch } from 'antd';
import { useState } from 'react';
import { kafkaConnectApi } from '../../api/kafka-connect';
import type { DbzKafkaConnectVo } from '../../api/types';
import { errorMessage } from '../../utils/format';

interface KafkaConnectFormProps {
  open: boolean;
  /** 为空表示新增 */
  record?: DbzKafkaConnectVo | null;
  onCancel: () => void;
  onSuccess: () => void;
}

interface FormValues {
  name: string;
  url: string;
  remark?: string;
  enabled: boolean;
}

export default function KafkaConnectForm({
  open,
  record,
  onCancel,
  onSuccess,
}: KafkaConnectFormProps) {
  const { message } = AntdApp.useApp();
  const [form] = Form.useForm<FormValues>();
  const [submitting, setSubmitting] = useState(false);
  const isEdit = Boolean(record);

  /**
   * # 弹窗完全打开后同步表单
   *
   * Modal 的子树是**懒挂载**的：`open` 从 false 变 true 的那次提交里 Form 还没挂上，
   * 此时在 effect 里调 `setFieldsValue` 会静默失效（form 未连接）。放在
   * `afterOpenChange` 里就一定能拿到已挂载的实例。
   */
  const syncForm = (visible: boolean) => {
    if (!visible) {
      return;
    }
    if (record) {
      form.setFieldsValue({
        name: record.name,
        url: record.url,
        remark: record.remark ?? '',
        enabled: record.enabled,
      });
    } else {
      // 回到 initialValues（enabled: true）
      form.resetFields();
    }
  };

  const handleSubmit = async () => {
    const values = await form.validateFields().catch(() => null);
    if (!values) {
      return;
    }

    setSubmitting(true);
    try {
      const payload = {
        name: values.name.trim(),
        url: values.url.trim(),
        // 传空串而不是 undefined：ModifyDto 用 Option<Option<String>> 区分「不改」与「清空」
        remark: values.remark?.trim() ?? '',
        enabled: values.enabled,
      };
      if (record) {
        await kafkaConnectApi.modify({ ...payload, id: record.id });
      } else {
        await kafkaConnectApi.add(payload);
      }
      message.success(isEdit ? '修改成功' : '新增成功');
      onSuccess();
    } catch (error) {
      message.error(errorMessage(error));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Modal
      open={open}
      title={isEdit ? `编辑集群 · ${record?.name ?? ''}` : '新增 Kafka Connect 集群'}
      okText="保存"
      cancelText="取消"
      confirmLoading={submitting}
      onOk={handleSubmit}
      onCancel={onCancel}
      afterOpenChange={syncForm}
      width={560}
    >
      <Form form={form} layout="vertical" initialValues={{ enabled: true }} style={{ marginTop: 12 }}>
        <Form.Item
          name="name"
          label="名称"
          rules={[
            { required: true, message: '请填写名称' },
            { max: 64, message: '名称不能超过 64 个字符' },
          ]}
        >
          <Input placeholder="例如 生产集群-华东" allowClear />
        </Form.Item>

        <Form.Item
          name="url"
          label="地址"
          extra="Kafka Connect REST 地址，形如 http://host:8083。修改地址会先注销旧地址上的全部连接器，再由下一轮检查重新部署。"
          rules={[
            { required: true, message: '请填写地址' },
            { pattern: /^https?:\/\/.+/, message: '地址需以 http:// 或 https:// 开头' },
          ]}
        >
          <Input placeholder="http://10.0.0.1:8083" allowClear />
        </Form.Item>

        <Form.Item name="remark" label="备注">
          <Input.TextArea rows={2} maxLength={255} showCount placeholder="选填" />
        </Form.Item>

        <Form.Item
          name="enabled"
          label="启用"
          valuePropName="checked"
          extra="禁用后该集群不参与健康检查，名下连接器也会从集群上注销。"
        >
          <Switch checkedChildren="启用" unCheckedChildren="禁用" />
        </Form.Item>
      </Form>
    </Modal>
  );
}
