/**
 * # 下游连接器表单
 *
 * 下游连接器只选「上游连接器」一个归属项：所属 Kafka Connect 由所选上游自动推导，
 * 不再让用户单独选择，避免写出上下游分处不同 Connect 的断裂链路。表单里不展示
 * Connect / Kafka 集群等已确定项，归属信息统一由控制面卡片副标题与属性框呈现。
 *
 * 从控制面「+」进来时上游已经固定，表单连上游选择也不出现，只填名称与配置。
 *
 * 配置区用 {@link ConnectorConfigEditor}（`kind="sink"`）：图形化填「消费哪个 topic、写哪个库、
 * 按什么语义写」，也可以直接写 JSON。sink 的参数键与上游完全不同（没有 `database.*`，
 * 地址都在 `connection.*` 上），所以分组由编辑器按方向各带一份。
 *
 * 「启用」不在这里：它在列表的启用列上切换（见 `EnabledSwitch`），因为禁用会连带注销
 * 集群上的连接器，不适合混在改配置的表单里一起提交。
 */

import { Alert, App as AntdApp, Form, Input, Modal, Select } from 'antd';
import { useEffect, useMemo, useState } from 'react';
import { sourceConnectorApi } from '../../api/source-connector';
import { sinkConnectorApi } from '../../api/sink-connector';
import type { DbzSinkConnectorVo, DbzSourceConnectorVo, Id } from '../../api/types';
import ConnectorConfigEditor, { checkConnectorConfig } from '../../components/ConnectorConfigEditor';
import { useCancelConfirm } from '../../hooks/useCancelConfirm';
import { errorMessage } from '../../utils/format';

interface FixedSource {
  id: Id;
  /** 上游所在的 Kafka Connect，由控制面当前节点直接带进来，不用再查列表 */
  kafkaConnectId: Id;
}

interface SinkConnectorFormProps {
  open: boolean;
  record?: DbzSinkConnectorVo | null;
  /** 从控制面节点的「+」新增时，上游已固定，表单不再出现上游选择 */
  fixedSource?: FixedSource | null;
  onCancel: () => void;
  onSuccess: () => void;
}

interface FormValues {
  sourceConnectorId?: Id;
  name: string;
  connectorName: string;
  connectorConfig: string;
  remark?: string;
}

/** 校验连接器配置：JSON 对象、必填项与数值范围（规则同步自 Kafka Connect 参数） */
async function validateJson(_rule: unknown, value?: string): Promise<void> {
  const problem = checkConnectorConfig('sink', value ?? '');
  if (problem) {
    throw new Error(problem);
  }
}

export default function SinkConnectorForm({
  open,
  record,
  fixedSource,
  onCancel,
  onSuccess,
}: SinkConnectorFormProps) {
  const { message } = AntdApp.useApp();
  const [form] = Form.useForm<FormValues>();
  const [submitting, setSubmitting] = useState(false);
  const [sources, setSources] = useState<DbzSourceConnectorVo[]>([]);
  const [sourceError, setSourceError] = useState<string | null>(null);
  const [loadingSources, setLoadingSources] = useState(false);
  const isEdit = Boolean(record);
  const { handleCancel, markPristine } = useCancelConfirm(form, onCancel);

  const sourceById = useMemo(
    () => new Map(sources.map((source) => [source.id, source])),
    [sources],
  );
  const sourceOptions = useMemo(
    () =>
      [...sources]
        .sort((left, right) => left.name.localeCompare(right.name))
        .map((source) => ({ value: source.id, label: source.name })),
    [sources],
  );

  /** 上游固定时不需要整份列表；新增/编辑要靠列表把上游换算成所属 Connect */
  useEffect(() => {
    if (!open || fixedSource) {
      return;
    }
    setLoadingSources(true);
    setSourceError(null);
    sourceConnectorApi
      .list()
      .then((list) => setSources(list))
      .catch((reason: unknown) => {
        setSources([]);
        setSourceError(errorMessage(reason));
      })
      .finally(() => setLoadingSources(false));
  }, [open, fixedSource]);

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
        sourceConnectorId: record.sourceConnectorId,
        name: record.name,
        connectorName: record.connectorName,
        connectorConfig: record.connectorConfig,
        remark: record.remark ?? '',
      });
    } else if (fixedSource) {
      form.setFieldsValue({ sourceConnectorId: fixedSource.id });
    }
    // 回填完成，记录基线；此后值有变化才算脏
    markPristine();
  };

  /** 从所选上游推导所属 Connect；列表加载失败时只允许沿用原记录里的同一上游 */
  const deriveConnectId = (sourceConnectorId: Id | undefined): Id | null => {
    if (!sourceConnectorId) {
      return null;
    }
    if (fixedSource?.id === sourceConnectorId) {
      return fixedSource.kafkaConnectId;
    }
    const source = sourceById.get(sourceConnectorId);
    if (source) {
      return source.kafkaConnectId;
    }
    if (record && record.sourceConnectorId === sourceConnectorId) {
      return record.kafkaConnectId;
    }
    return null;
  };

  const handleSubmit = async () => {
    const values = await form.validateFields().catch(() => null);
    if (!values) {
      return;
    }

    const sourceConnectorId = fixedSource?.id ?? values.sourceConnectorId;
    if (!sourceConnectorId) {
      message.error('请选择上游连接器');
      return;
    }
    // 所属 Connect 完全由上游决定，不单独提交用户选择
    const kafkaConnectId = deriveConnectId(sourceConnectorId);
    if (!kafkaConnectId) {
      message.error('拿不到上游连接器所属的 Kafka Connect，请刷新后重试');
      return;
    }

    setSubmitting(true);
    try {
      const payload = {
        sourceConnectorId,
        kafkaConnectId,
        name: values.name.trim(),
        connectorName: values.connectorName.trim(),
        connectorConfig: values.connectorConfig,
        remark: values.remark?.trim() ?? '',
      };
      if (record) {
        // 不带 enabled：没传的字段不会被写回，不会把列表上刚切换的开关覆盖掉
        await sinkConnectorApi.modify({ ...payload, id: record.id });
        if (record.kafkaConnectId !== kafkaConnectId) {
          message.success('修改成功，已按上游连接器对齐所属 Kafka Connect');
          onSuccess();
          return;
        }
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
      onCancel={handleCancel}
      afterOpenChange={syncForm}
      width={860}
    >
      <Form form={form} layout="vertical" style={{ marginTop: 12 }} className="dbz-form-scroll">
        {sourceError && (
          <Alert
            type="error"
            showIcon
            style={{ marginBottom: 16 }}
            message="上游连接器列表加载失败"
            description={sourceError}
          />
        )}

        {/* 上游固定（从控制面 + 新增）时整项不出现，连只读控件也不留 */}
        {!fixedSource && (
          <Form.Item
            name="sourceConnectorId"
            label="上游连接器"
            rules={[{ required: true, message: '请选择上游连接器' }]}
          >
            <Select
              showSearch
              optionFilterProp="label"
              placeholder="选择上游连接器"
              loading={loadingSources}
              options={sourceOptions}
              notFoundContent={
                sourceError ? '列表加载失败，请重试' : loadingSources ? '加载中…' : '暂无上游连接器'
              }
            />
          </Form.Item>
        )}

        <Form.Item
          name="name"
          label="名称"
          rules={[
            { required: true, whitespace: true, message: '请填写名称' },
            // 库列 `name` 是 varchar(50)，前端超长会在服务端被截断/拒绝
            { max: 50, message: '名称不能超过 50 个字符' },
          ]}
        >
          <Input placeholder="业务侧名称，例如 订单库同步到数仓" allowClear />
        </Form.Item>

        <Form.Item
          name="connectorName"
          label="连接器名称"
          extra="Kafka Connect 上的连接器名称，全局唯一，形如 jdbc-orders-sink。"
          rules={[
            { required: true, whitespace: true, message: '请填写连接器名称' },
            // 库列 `connector_name` 是 varchar(50)，且 Kafka Connect 侧全局唯一
            { max: 50, message: '连接器名称不能超过 50 个字符' },
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
          <Input.TextArea rows={2} maxLength={50} showCount placeholder="选填" />
        </Form.Item>
      </Form>
    </Modal>
  );
}
