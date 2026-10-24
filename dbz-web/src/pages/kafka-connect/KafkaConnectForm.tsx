/**
 * # 集群表单
 *
 * 新增与编辑共用一个弹窗：有 `record` 就是编辑，没有就是新增。
 *
 * 两件事要在这里定下来：
 *
 * 1. **地址**（`url`）—— 健康检查的探测目标。服务端在改地址时会先把旧地址上的
 *    连接器全部注销，所以这里给出明确提示，避免运维误以为只是换个字符串。
 * 2. **Kafka 集群**（`kafkaBrokerId`）—— 这台 worker 上的连接器把信号投到哪个
 *    集群。「再同步」走 Kafka 通道时要顺着它找 broker，所以新增必填；老记录可能
 *    还空着，编辑时补上即可。
 *
 * 「启用」不在这里：它在列表的启用列上切换（见 `EnabledSwitch`），因为禁用会连带
 * 注销集群上的连接器，不适合混在「改个名字、改个地址」的表单里一起提交。
 */

import { App as AntdApp, Form, Input, Modal, Select } from 'antd';
import { useEffect, useState } from 'react';
import { kafkaBrokerApi } from '../../api/kafka-broker';
import { kafkaConnectApi } from '../../api/kafka-connect';
import type { DbzKafkaConnectVo, Id } from '../../api/types';
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
  kafkaBrokerId: Id;
  remark?: string;
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
  const [brokerOptions, setBrokerOptions] = useState<{ value: Id; label: string }[]>([]);
  const isEdit = Boolean(record);

  /**
   * 打开时拉一次 Kafka 集群列表：集群数量不多，而选项要显示「名称 + 地址」，
   * 由页面传进来会多一层参数传递，这里自己取更省事。
   */
  useEffect(() => {
    if (!open) {
      return;
    }
    kafkaBrokerApi
      .list()
      .then((brokers) =>
        setBrokerOptions(
          brokers.map((broker) => ({
            value: broker.id,
            label: `${broker.name}（${broker.bootstrapServers}）`,
          })),
        ),
      )
      .catch(() => setBrokerOptions([]));
  }, [open]);

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
        kafkaBrokerId: record.kafkaBrokerId ?? undefined,
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
        name: values.name.trim(),
        url: values.url.trim(),
        kafkaBrokerId: values.kafkaBrokerId,
        // 传空串而不是 undefined：ModifyDto 用 Option<Option<String>> 区分「不改」与「清空」
        remark: values.remark?.trim() ?? '',
      };
      if (record) {
        // 不带 enabled：没传的字段不会被写回，不会把列表上刚切换的开关覆盖掉
        await kafkaConnectApi.modify({ ...payload, id: record.id });
      } else {
        // 新增一律先建成启用，之后在列表的启用列里切换（与服务端库默认值一致）
        await kafkaConnectApi.add({ ...payload, enabled: true });
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
      width={600}
    >
      <Form form={form} layout="vertical" style={{ marginTop: 12 }}>
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

        <Form.Item
          name="kafkaBrokerId"
          label="Kafka 集群"
          extra="这台 worker 上的连接器把变更（以及再同步的信号）写到这里。没有可选项时先去「Kafka 集群」页建一条。"
          rules={[{ required: true, message: '请选择 Kafka 集群' }]}
        >
          <Select
            showSearch
            optionFilterProp="label"
            placeholder="选择 Kafka 集群"
            options={brokerOptions}
            notFoundContent="还没有 Kafka 集群记录"
          />
        </Form.Item>

        <Form.Item name="remark" label="备注">
          <Input.TextArea rows={2} maxLength={255} showCount placeholder="选填" />
        </Form.Item>
      </Form>
    </Modal>
  );
}
