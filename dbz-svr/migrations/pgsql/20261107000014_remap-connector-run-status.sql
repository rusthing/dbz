-- 连接器运行状态收敛为统一枚举
--
-- 控制面只展示一个状态，不再把「库内状态 / 实时状态 / 不一致」并列：
--   0=Unknown        未知
--   1=Running        运行中
--   2=Disabled       已停用
--   3=Failed         失败
--   4=NotDeployed    仅纳管未部署
--   5=Unreachable    不可达（Connect 连不上）
--   6=Stopped        已停止（PAUSED / STOPPED）
--
-- 旧枚举里 4/6/7/8 分别是「注册失败 / 检测异常 / 状态查询失败 / 状态异常」，
-- 统一归并为 Failed(3)；3 与 5 的语义可直接对应新值。迁移幂等，重复执行无副作用。

UPDATE public.dbz_source_connector
SET run_status = 3
WHERE run_status IN (4, 6, 7, 8);

UPDATE public.dbz_sink_connector
SET run_status = 3
WHERE run_status IN (4, 6, 7, 8);

COMMENT ON COLUMN public.dbz_source_connector.run_status IS '连接器统一状态
@enum ConnectorRunStatus default=Unknown
  0=Unknown       未知
  1=Running       运行中
  2=Disabled      已停用
  3=Failed        失败
  4=NotDeployed   仅纳管未部署
  5=Unreachable   不可达（Connect 连不上，保留最后已知状态）
  6=Stopped       已停止（PAUSED / STOPPED）';

COMMENT ON COLUMN public.dbz_sink_connector.run_status IS '连接器统一状态
@enum ConnectorRunStatus default=Unknown
  0=Unknown       未知
  1=Running       运行中
  2=Disabled      已停用
  3=Failed        失败
  4=NotDeployed   仅纳管未部署
  5=Unreachable   不可达（Connect 连不上，保留最后已知状态）
  6=Stopped       已停止（PAUSED / STOPPED）';
