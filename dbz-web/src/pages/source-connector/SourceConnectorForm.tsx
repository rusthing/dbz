/**
 * # 上游连接器表单
 *
 * 上游连接器的 `connectorConfig` 就是 Debezium 原样的 JSON：检查任务发现连接器不存在时，
 * 直接拿这段文本去 `POST /connectors` 部署。所以提交前会校验：必须是 JSON 对象、
 * 当前方言的必填项齐全、端口等数值合法（见 `checkConnectorConfig`）。更细的语义
 * 正确性仍由集群的部署结果反馈。
 *
 * 配置区用 {@link ConnectorConfigEditor}：图形化填常用项、或直接写 JSON，两种模式共享
 * 同一份文本，图形化没覆盖的参数写在 JSON 里也不会被覆盖掉。
 *
 * 「启用」不在这里：它在列表的启用列上切换（见 `EnabledSwitch`），因为禁用会连带注销
 * 集群上的连接器，不适合混在改配置的表单里一起提交。
 */

import { App as AntdApp, Form, Input, Modal, Select } from 'antd';
import { useState } from 'react';
import { sourceConnectorApi } from '../../api/source-connector';
import type { DbzSourceConnectorVo, Id } from '../../api/types';
import ConnectorConfigEditor, { checkConnectorConfig } from '../../components/ConnectorConfigEditor';
import { useCancelConfirm } from '../../hooks/useCancelConfirm';
import { errorMessage } from '../../utils/format';

interface SourceConnectorFormProps {
  open: boolean;
  record?: DbzSourceConnectorVo | null;
  /** 集群下拉选项，由页面统一提供，避免弹窗里再请求一次 */
  clusterOptions: { value: Id; label: string }[];
  onCancel: () => void;
  onSuccess: () => void;
}

interface FormValues {
  kafkaConnectId: Id;
  name: string;
  connectorName: string;
  connectorConfig: string;
  remark?: string;
}

/** 校验连接器配置：JSON 对象、必填项与数值范围（规则同步自 Debezium 参数） */
async function validateJson(_rule: unknown, value?: string): Promise<void> {
  const problem = checkConnectorConfig('source', value ?? '');
  if (problem) {
    throw new Error(problem);
  }
}

export default function SourceConnectorForm({
  open,
  record,
  clusterOptions,
  onCancel,
  onSuccess,
}: SourceConnectorFormProps) {
  const { message } = AntdApp.useApp();
  const [form] = Form.useForm<FormValues>();
  const [submitting, setSubmitting] = useState(false);
  const isEdit = Boolean(record);
  const { handleCancel, markPristine } = useCancelConfirm(form, onCancel);

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
    // 先清空上一次的取值与 touched，避免「新建 -> 立刻编辑」时残留状态
    form.resetFields();
    if (record) {
      form.setFieldsValue({
        kafkaConnectId: record.kafkaConnectId,
        name: record.name,
        connectorName: record.connectorName,
        connectorConfig: record.connectorConfig,
        remark: record.remark ?? '',
      });
    }
    // 回填完成，记录基线；此后值有变化才算脏
    markPristine();
  };

  const handleSubmit = async () => {
    const values = await form.validateFields().catch(() => null);
    if (!values) {
      return;
    }

    setSubmitting(true);
    try {
      const payload = {
        kafkaConnectId: values.kafkaConnectId,
        name: values.name.trim(),
        connectorName: values.connectorName.trim(),
        connectorConfig: values.connectorConfig,
        remark: values.remark?.trim() ?? '',
      };
      if (record) {
        // 不带 enabled：没传的字段不会被写回，不会把列表上刚切换的开关覆盖掉
        await sourceConnectorApi.modify({ ...payload, id: record.id });
      } else {
        // 新增一律先建成启用，之后在列表的启用列里切换（与服务端库默认值一致）
        await sourceConnectorApi.add({ ...payload, enabled: true });
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
      title={isEdit ? `编辑上游连接器 · ${record?.name ?? ''}` : '新增上游连接器'}
      okText="保存"
      cancelText="取消"
      confirmLoading={submitting}
      onOk={handleSubmit}
      onCancel={handleCancel}
      afterOpenChange={syncForm}
      width={860}
    >
      <Form form={form} layout="vertical" style={{ marginTop: 12 }} className="dbz-form-scroll">
        <Form.Item
          name="kafkaConnectId"
          label="Kafka Connect 集群"
          extra="保存时会当场把连接器部署到这台集群上；改挂集群则先注销旧位置，再在新集群部署。"
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
            { required: true, whitespace: true, message: '请填写名称' },
            // 库列 `name` 是 varchar(50)
            { max: 50, message: '名称不能超过 50 个字符' },
          ]}
        >
          <Input placeholder="业务侧名称，例如 订单库增量" allowClear />
        </Form.Item>

        <Form.Item
          name="connectorName"
          label="连接器名称"
          extra="Kafka Connect 上的连接器名称，全局唯一，形如 mysql-orders-source。"
          rules={[
            { required: true, whitespace: true, message: '请填写连接器名称' },
            // 库列 `connector_name` 是 varchar(50)，且 Kafka Connect 侧全局唯一
            { max: 50, message: '连接器名称不能超过 50 个字符' },
          ]}
        >
          <Input placeholder="mysql-orders-source" allowClear className="dbz-mono" />
        </Form.Item>

        <Form.Item
          name="connectorConfig"
          label="连接器配置"
          rules={[{ validator: validateJson }]}
        >
          <ConnectorConfigEditor />
        </Form.Item>

        <Form.Item name="remark" label="备注">
          <Input.TextArea rows={2} maxLength={50} showCount placeholder="选填" />
        </Form.Item>
      </Form>
    </Modal>
  );
}
