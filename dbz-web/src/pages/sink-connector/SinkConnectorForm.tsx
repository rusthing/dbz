/**
 * # 下游连接器表单
 *
 * 下游连接器通过 `sourceConnectorId` 归属某条上游，通过 `kafkaConnectId` 指定运行它的集群。
 * 两者相互独立：下游与上游可以在同一台集群上，也可以分处不同集群（只要对接同一个 Kafka）。
 */

import { App as AntdApp, Form, Input, Modal, Select, Switch } from 'antd';
import { useState } from 'react';
import { sinkConnectorApi } from '../../api/sink-connector';
import type { DbzSinkConnectorVo } from '../../api/types';
import JsonEditor from '../../components/JsonEditor';
import { errorMessage } from '../../utils/format';

interface SinkConnectorFormProps {
  open: boolean;
  record?: DbzSinkConnectorVo | null;
  clusterOptions: { value: number; label: string }[];
  sourceOptions: { value: number; label: string }[];
  onCancel: () => void;
  onSuccess: () => void;
}

interface FormValues {
  sourceConnectorId: number;
  kafkaConnectId: number;
  name: string;
  connectorName: string;
  connectorConfig: string;
  remark?: string;
  enabled: boolean;
}

/** 校验连接器配置是否为合法 JSON */
async function validateJson(_rule: unknown, value?: string): Promise<void> {
  if (!value || !value.trim()) {
    throw new Error('请填写连接器配置');
  }
  try {
    JSON.parse(value);
  } catch {
    throw new Error('连接器配置不是合法的 JSON');
  }
}

export default function SinkConnectorForm({
  open,
  record,
  clusterOptions,
  sourceOptions,
  onCancel,
  onSuccess,
}: SinkConnectorFormProps) {
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
        sourceConnectorId: record.sourceConnectorId,
        kafkaConnectId: record.kafkaConnectId,
        name: record.name,
        connectorName: record.connectorName,
        connectorConfig: record.connectorConfig,
        remark: record.remark ?? '',
        enabled: record.enabled,
      });
    } else {
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
        sourceConnectorId: values.sourceConnectorId,
        kafkaConnectId: values.kafkaConnectId,
        name: values.name.trim(),
        connectorName: values.connectorName.trim(),
        connectorConfig: values.connectorConfig,
        remark: values.remark?.trim() ?? '',
        enabled: values.enabled,
      };
      if (record) {
        await sinkConnectorApi.modify({ ...payload, id: record.id });
      } else {
        await sinkConnectorApi.add(payload);
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
      title={isEdit ? `编辑下游连接器 · ${record?.name ?? ''}` : '新增下游连接器'}
      okText="保存"
      cancelText="取消"
      confirmLoading={submitting}
      onOk={handleSubmit}
      onCancel={onCancel}
      afterOpenChange={syncForm}
      width={720}
    >
      <Form form={form} layout="vertical" initialValues={{ enabled: true }} style={{ marginTop: 12 }}>
        <Form.Item
          name="sourceConnectorId"
          label="上游连接器"
          extra="下游归属的上游，用于把同一链路的上下游串起来展示与告警收敛。"
          rules={[{ required: true, message: '请选择上游连接器' }]}
        >
          <Select
            showSearch
            optionFilterProp="label"
            placeholder="选择上游连接器"
            options={sourceOptions}
          />
        </Form.Item>

        <Form.Item
          name="kafkaConnectId"
          label="Kafka Connect 集群"
          extra="运行该下游连接器的集群，可以与上游不在同一台集群上。"
          rules={[{ required: true, message: '请选择集群' }]}
        >
          <Select
            showSearch
            optionFilterProp="label"
            placeholder="选择集群"
            options={clusterOptions}
          />
        </Form.Item>

        <Form.Item
          name="name"
          label="名称"
          rules={[
            { required: true, message: '请填写名称' },
            { max: 64, message: '名称不能超过 64 个字符' },
          ]}
        >
          <Input placeholder="业务侧名称，例如 订单库同步到数仓" allowClear />
        </Form.Item>

        <Form.Item
          name="connectorName"
          label="连接器名称"
          extra="Kafka Connect 上的连接器名称，全局唯一，形如 jdbc-orders-sink。"
          rules={[
            { required: true, message: '请填写连接器名称' },
            { max: 128, message: '连接器名称不能超过 128 个字符' },
          ]}
        >
          <Input placeholder="jdbc-orders-sink" allowClear className="dbz-mono" />
        </Form.Item>

        <Form.Item
          name="connectorConfig"
          label="连接器配置（JSON）"
          rules={[{ validator: validateJson }]}
        >
          <JsonEditor />
        </Form.Item>

        <Form.Item name="remark" label="备注">
          <Input.TextArea rows={2} maxLength={255} showCount placeholder="选填" />
        </Form.Item>

        <Form.Item
          name="enabled"
          label="启用"
          valuePropName="checked"
          extra="禁用后不参与健康检查，已部署的连接器不会被自动删除。"
        >
          <Switch checkedChildren="启用" unCheckedChildren="禁用" />
        </Form.Item>
      </Form>
    </Modal>
  );
}
