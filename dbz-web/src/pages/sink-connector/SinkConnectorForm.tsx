/**
 * # 下游连接器表单
 *
 * 下游连接器通过 `sourceConnectorId` 归属某条上游，通过 `kafkaConnectId` 指定运行它的集群。
 * 两者相互独立：下游与上游可以在同一台集群上，也可以分处不同集群（只要对接同一个 Kafka）。
 *
 * 配置区用 {@link ConnectorConfigEditor}（`kind="sink"`）：图形化填「消费哪个 topic、写哪个库、
 * 按什么语义写」，也可以直接写 JSON。sink 的参数键与上游完全不同（没有 `database.*`，
 * 地址都在 `connection.*` 上），所以分组由编辑器按方向各带一份。
 *
 * 「启用」不在这里：它在列表的启用列上切换（见 `EnabledSwitch`），因为禁用会连带注销
 * 集群上的连接器，不适合混在改配置的表单里一起提交。
 */

import { App as AntdApp, Form, Input, Modal, Select } from 'antd';
import { useState } from 'react';
import { sinkConnectorApi } from '../../api/sink-connector';
import type { DbzSinkConnectorVo, Id } from '../../api/types';
import ConnectorConfigEditor from '../../components/ConnectorConfigEditor';
import { errorMessage } from '../../utils/format';

interface SinkConnectorFormProps {
  open: boolean;
  record?: DbzSinkConnectorVo | null;
  clusterOptions: { value: Id; label: string }[];
  sourceOptions: { value: Id; label: string }[];
  onCancel: () => void;
  onSuccess: () => void;
}

interface FormValues {
  sourceConnectorId: Id;
  kafkaConnectId: Id;
  name: string;
  connectorName: string;
  connectorConfig: string;
  remark?: string;
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
      };
      if (record) {
        // 不带 enabled：没传的字段不会被写回，不会把列表上刚切换的开关覆盖掉
        await sinkConnectorApi.modify({ ...payload, id: record.id });
      } else {
        // 新增一律先建成启用，之后在列表的启用列里切换（与服务端库默认值一致）
        await sinkConnectorApi.add({ ...payload, enabled: true });
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
      width={860}
    >
      <Form form={form} layout="vertical" style={{ marginTop: 12 }} className="dbz-form-scroll">
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
          label="连接器配置"
          rules={[{ validator: validateJson }]}
        >
          <ConnectorConfigEditor kind="sink" />
        </Form.Item>

        <Form.Item name="remark" label="备注">
          <Input.TextArea rows={2} maxLength={255} showCount placeholder="选填" />
        </Form.Item>
      </Form>
    </Modal>
  );
}
