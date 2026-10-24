/**
 * # Kafka 集群（broker 侧）表单
 *
 * 一条记录 = 一组「地址 + 认证」。这两半必须一起看，所以表单里也是联动的：
 * 选了 SASL 明文才出现机制与用户名密码，选回明文时机制自动归零 —— 服务端同样校验
 * 这组组合（`security_protocol = 明文却带着机制` 会被直接拒掉）。
 *
 * 密码**只进不出**：编辑时输入框永远为空，留空表示沿用原密码，不是清空。
 */

import { Alert, App as AntdApp, Form, Input, Modal, Select } from 'antd';
import { useState } from 'react';
import { kafkaBrokerApi } from '../../api/kafka-broker';
import type {
  DbzKafkaBrokerVo,
  KafkaSaslMechanism,
  KafkaSecurityProtocol,
} from '../../api/types';
import { dicOptions, kafkaSaslMechanismDic, kafkaSecurityProtocolDic } from '../../dic';
import { errorMessage } from '../../utils/format';

interface KafkaBrokerFormProps {
  open: boolean;
  record?: DbzKafkaBrokerVo | null;
  onCancel: () => void;
  onSuccess: () => void;
}

interface FormValues {
  name: string;
  bootstrapServers: string;
  securityProtocol: KafkaSecurityProtocol;
  saslMechanism: KafkaSaslMechanism;
  saslUsername?: string;
  saslPassword?: string;
  remark?: string;
}

const SASL: KafkaSecurityProtocol = 1;

export default function KafkaBrokerForm({ open, record, onCancel, onSuccess }: KafkaBrokerFormProps) {
  const { message } = AntdApp.useApp();
  const [form] = Form.useForm<FormValues>();
  const [submitting, setSubmitting] = useState(false);
  const isEdit = Boolean(record);
  // 协议决定后面三个字段出不出现，用 watch 跟着变
  const protocol = Form.useWatch('securityProtocol', form) ?? record?.securityProtocol ?? 0;
  const needsSasl = protocol === SASL;

  /**
   * # 弹窗完全打开后同步表单
   *
   * Modal 的子组件是懒挂载的，`open` 变 true 的那次渲染里 Form 还没连上，
   * `setFieldsValue` 会静默失效，所以放在 `afterOpenChange` 里。
   */
  const syncForm = (visible: boolean) => {
    if (!visible) {
      return;
    }
    if (record) {
      form.setFieldsValue({
        name: record.name,
        bootstrapServers: record.bootstrapServers,
        securityProtocol: record.securityProtocol,
        // 明文下机制必须是 None；SASL 下不允许 None，兜一个能连通的默认值
        saslMechanism: record.saslMechanism || (record.securityProtocol === SASL ? 1 : 0),
        saslUsername: record.saslUsername ?? '',
        // 密码不返回，编辑时必须重新输入才会改
        saslPassword: '',
        remark: record.remark ?? '',
      });
    } else {
      form.resetFields();
      form.setFieldsValue({ securityProtocol: 0, saslMechanism: 0 });
    }
  };

  const handleSubmit = async () => {
    const values = await form.validateFields().catch(() => null);
    if (!values) {
      return;
    }

    // 协议是明文时机制必须归零，否则服务端会判「自相矛盾的组合」
    const payload = {
      name: values.name.trim(),
      bootstrapServers: values.bootstrapServers.trim(),
      securityProtocol: values.securityProtocol,
      saslMechanism: needsSasl ? values.saslMechanism : (0 as KafkaSaslMechanism),
      saslUsername: needsSasl ? (values.saslUsername ?? '').trim() : '',
      saslPassword: needsSasl ? (values.saslPassword ?? '') : '',
      remark: values.remark?.trim() ?? '',
    };

    setSubmitting(true);
    try {
      if (record) {
        // 空密码不传：没传的字段不会写回，库里原密码保持不动
        const { saslPassword, ...rest } = payload;
        await kafkaBrokerApi.modify({
          ...rest,
          id: record.id,
          ...(saslPassword ? { saslPassword } : {}),
        });
      } else {
        await kafkaBrokerApi.add({ ...payload, enabled: true });
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
      title={isEdit ? `编辑 Kafka 集群 · ${record?.name ?? ''}` : '新增 Kafka 集群'}
      okText="保存"
      cancelText="取消"
      confirmLoading={submitting}
      onOk={handleSubmit}
      onCancel={onCancel}
      afterOpenChange={syncForm}
      width={640}
    >
      <Form form={form} layout="vertical" style={{ marginTop: 12 }} className="dbz-form-scroll">
        <Form.Item
          name="name"
          label="名称"
          rules={[
            { required: true, message: '请填写名称' },
            { max: 50, message: '名称不能超过 50 个字符' },
          ]}
        >
          <Input placeholder="例如 生产消息集群" allowClear />
        </Form.Item>

        <Form.Item
          name="bootstrapServers"
          label="Broker 地址"
          extra="host:port，多个用逗号分隔；保存时会去空格、去重。信号 topic 的消息就发到这里。"
          rules={[
            { required: true, message: '请填写 broker 地址' },
            { max: 500, message: '地址不能超过 500 个字符' },
          ]}
        >
          <Input placeholder="10.0.0.1:9092,10.0.0.2:9092" allowClear className="dbz-mono" />
        </Form.Item>

        <Form.Item
          name="securityProtocol"
          label="安全协议"
          extra="TLS 需要证书与 CA 配置，暂不支持；这两项是 dbz 真正连得上的取值。选了 SASL_PLAINTEXT 才需要下面的机制与账号。"
          rules={[{ required: true, message: '请选择安全协议' }]}
        >
          <Select
            options={dicOptions(kafkaSecurityProtocolDic)}
            onChange={(next: KafkaSecurityProtocol) => {
              // 机制为 None 时切到 SASL，下面的选择框会显出一个没有标签的 0；
              // 直接给个能连上的默认值（服务端也要求 SASL 必须带机制）
              if (next === SASL && !form.getFieldValue('saslMechanism')) {
                form.setFieldValue('saslMechanism', 1);
              }
            }}
          />
        </Form.Item>

        {needsSasl && (
          <>
            <Form.Item
              name="saslMechanism"
              label="SASL 机制"
              rules={[{ required: true, message: '请选择 SASL 机制' }]}
            >
              <Select
                options={dicOptions(kafkaSaslMechanismDic).filter((option) => option.value !== 0)}
              />
            </Form.Item>

            <Form.Item
              name="saslUsername"
              label="认证用户名"
              rules={[{ required: true, message: '请填写认证用户名' }, { max: 100 }]}
            >
              <Input placeholder="例如 dbz" allowClear />
            </Form.Item>

            <Form.Item
              name="saslPassword"
              label="认证密码"
              extra={isEdit ? '留空表示沿用原密码；服务端不返回密码，看不到原值。' : undefined}
              rules={isEdit ? [{ max: 200 }] : [{ required: true, message: '请填写认证密码' }, { max: 200 }]}
            >
              <Input.Password placeholder={isEdit ? '留空表示不修改' : '认证密码'} autoComplete="new-password" />
            </Form.Item>
          </>
        )}

        <Form.Item name="remark" label="备注">
          <Input.TextArea rows={2} maxLength={50} showCount placeholder="选填" />
        </Form.Item>

        <Alert
          type="info"
          showIcon
          message="新增后建议点一次「检测」"
          description="检测会真去拉一次 topic 列表并把结果写进状态；建记录时不去连集群，集群暂时没上线也能先把配置存下来。"
        />
      </Form>
    </Modal>
  );
}
