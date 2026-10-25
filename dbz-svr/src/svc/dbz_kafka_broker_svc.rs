//! # Kafka 集群（broker 侧）服务
//!
//! 管的是「消息往哪个集群投」这件事：地址、认证、连通状态。连接器部署到哪台 worker
//! 由 `dbz_kafka_connect` 表达，两者是同一套 Kafka 的两面，分开的理由见
//! `mo::dbz_kafka_broker`。
//!
//! ## 写入时的约束
//!
//! 地址与认证是**一组**配置，任何一项变了都得整组重新校验，所以新增与修改共用一个
//! [`BrokerProfile`]：先把「最终会落库的那组值」合出来，再统一规整与校验，最后才写。
//! 只改备注时走不到这些逻辑。
//!
//! ## 状态
//!
//! 状态只由「检测」改写（手动点按钮或定时任务）。改地址/认证、重新启用都会把状态置回
//! 「无状态」，等下一轮检查给结论 —— 沿用了 `dbz_kafka_connect` 的做法。
//!
//! ## 密码
//!
//! 密码只进不出：`DbzKafkaBrokerVo` 里没有这一列，前端拿不到。检测与投递信号都要用它，
//! 所以这两个动作读的是实体（[`BrokerConn`]），而不是 VO。

use crate::dic::{KafkaBrokerStatus, KafkaSaslMechanism, KafkaSecurityProtocol};
use crate::dto::DbzKafkaBrokerCheckDto;
use crate::mo::dbz_kafka_broker::Model as BrokerModel;
use crate::svc::DbzKafkaConnectSvc;
use crate::utils::{BrokerConn, KafkaBrokerUtils, RUN_MSG_MAX, one_line};
use crate::vo::DbzKafkaBrokerCheckVo;
use anyhow::anyhow;
use robotech::db::get_db_conn;
use robotech::macros::svc;
use sea_orm::{Condition, DatabaseConnection};
use std::time::Instant;
use tracing::{info, warn};
use wheel_rs::time_utils::now_ms;

#[svc(
    before_add = DbzKafkaBrokerSvc::before_add_hook,
    before_modify = DbzKafkaBrokerSvc::before_modify_hook,
    before_del_by_id = DbzKafkaBrokerSvc::before_del_by_id_hook
)]
pub struct DbzKafkaBrokerSvc;

/// # 一组完整的 broker 配置
///
/// 地址与认证必须一起看：协议是 SASL 却留着空密码、协议是明文却带着机制，都是自相
/// 矛盾的组合。合到一个结构里，校验只有一份。
struct BrokerProfile {
    bootstrap_servers: String,
    security_protocol: KafkaSecurityProtocol,
    sasl_mechanism: KafkaSaslMechanism,
    sasl_username: Option<String>,
    sasl_password: Option<String>,
}

impl BrokerProfile {
    /// # 规整并校验
    ///
    /// 规整是**幂等**的：地址去空格去重、用户名去首尾空白。校验只看组合是否自洽，
    /// 不去连 broker —— 新增记录时集群可能还没上线，连不上不该挡住建档。
    fn normalize(mut self) -> Result<Self, anyhow::Error> {
        self.bootstrap_servers = KafkaBrokerUtils::normalize_servers(&self.bootstrap_servers)?;
        self.sasl_username = self
            .sasl_username
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty());

        match self.security_protocol {
            KafkaSecurityProtocol::Plaintext => {
                // 明文下机制必须为空，否则下次连接会拿着机制去认证一个不认证的集群
                if self.sasl_mechanism != KafkaSaslMechanism::None {
                    return Err(anyhow!(
                        "安全协议为明文时不应配置 SASL 机制，请把机制改为 None"
                    ));
                }
            }
            KafkaSecurityProtocol::SaslPlaintext => {
                if self.sasl_mechanism == KafkaSaslMechanism::None {
                    return Err(anyhow!(
                        "安全协议选择了 SASL 明文，但没有选择 SASL 机制（PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512）"
                    ));
                }
                if self.sasl_username.is_none() {
                    return Err(anyhow!("安全协议选择了 SASL 明文，但没有填认证用户名"));
                }
                if self.sasl_password.as_deref().unwrap_or_default().is_empty() {
                    return Err(anyhow!("安全协议选择了 SASL 明文，但没有填认证密码"));
                }
            }
        }
        Ok(self)
    }

    /// 地址或认证是否有变化（决定是否把状态置回「无状态」）
    fn auth_or_address_changed(&self, old: &BrokerModel) -> bool {
        self.bootstrap_servers != old.bootstrap_servers
            || i16::from(self.security_protocol) != old.security_protocol
            || i16::from(self.sasl_mechanism) != old.sasl_mechanism
            || self.sasl_username.as_deref() != old.sasl_username.as_deref()
    }
}

// SvcError 由框架定义且体积较大，而钩子的返回类型必须与宏约定一致
#[allow(clippy::result_large_err)]
impl DbzKafkaBrokerSvc {
    /// # 新增前钩子：规整地址与认证，落定初始状态
    ///
    /// `#[crud_dto]` 把每个字段都包成 `Option`，可空列再包一层，所以这里读写都要
    /// 显式拆一层；写回用 `Some(..)`，`Some(None)` 才是「置 NULL」。
    async fn before_add_hook(
        mut add_dto: DbzKafkaBrokerAddDto,
    ) -> Result<DbzKafkaBrokerAddDto, SvcError> {
        let name = add_dto
            .name
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_string();
        if name.is_empty() {
            return Err(anyhow!("名称不能为空").into());
        }

        let profile = BrokerProfile {
            bootstrap_servers: add_dto.bootstrap_servers.clone().unwrap_or_default(),
            security_protocol: add_dto.security_protocol.unwrap_or_default(),
            sasl_mechanism: add_dto.sasl_mechanism.unwrap_or_default(),
            sasl_username: add_dto.sasl_username.clone().flatten(),
            sasl_password: add_dto.sasl_password.clone().flatten(),
        }
        .normalize()?;

        add_dto.name = Some(name);
        add_dto.bootstrap_servers = Some(profile.bootstrap_servers);
        add_dto.security_protocol = Some(profile.security_protocol);
        add_dto.sasl_mechanism = Some(profile.sasl_mechanism);
        add_dto.sasl_username = Some(profile.sasl_username);
        add_dto.sasl_password = Some(profile.sasl_password);

        // 新建就禁用时不参与检查，直接落为「已禁用」，否则会一直停在「无状态」
        add_dto.run_status = Some(if add_dto.enabled == Some(false) {
            KafkaBrokerStatus::Disabled
        } else {
            KafkaBrokerStatus::None
        });
        // 新记录还没有检查过，结论必须为空：带上一条的理由会和状态自相矛盾
        add_dto.run_msg = Some(None);
        Ok(add_dto)
    }

    /// # 修改前钩子：合并旧值后整组校验，判断要不要重置状态
    ///
    /// 只改名字/备注时走不掉查询，省一次读库。
    async fn before_modify_hook(
        mut modify_dto: DbzKafkaBrokerModifyDto,
    ) -> Result<DbzKafkaBrokerModifyDto, SvcError> {
        let Some(id) = modify_dto.id else {
            return Ok(modify_dto);
        };

        if let Some(name) = &modify_dto.name {
            modify_dto.name = Some(name.trim().to_string());
        }

        // 检测回写状态、改备注都碰不到地址与认证
        let touches_profile = modify_dto.bootstrap_servers.is_some()
            || modify_dto.security_protocol.is_some()
            || modify_dto.sasl_mechanism.is_some()
            || modify_dto.sasl_username.is_some()
            || modify_dto.sasl_password.is_some();
        if !touches_profile && modify_dto.enabled.is_none() {
            return Ok(modify_dto);
        }

        let old = Self::load_model(id).await?;

        let enabled_after = modify_dto.enabled.unwrap_or(old.enabled);
        let mut reset_status = false;

        if touches_profile {
            // 密码留空表示「沿用旧密码」：改备注、改用户名时不必把密码重打一遍
            let password = modify_dto
                .sasl_password
                .clone()
                .flatten()
                .filter(|value| !value.is_empty())
                .or_else(|| old.sasl_password.clone());

            let profile = BrokerProfile {
                bootstrap_servers: modify_dto
                    .bootstrap_servers
                    .clone()
                    .unwrap_or_else(|| old.bootstrap_servers.clone()),
                security_protocol: modify_dto
                    .security_protocol
                    .unwrap_or_else(|| old.security_protocol.into()),
                sasl_mechanism: modify_dto
                    .sasl_mechanism
                    .unwrap_or_else(|| old.sasl_mechanism.into()),
                sasl_username: modify_dto
                    .sasl_username
                    .clone()
                    .flatten()
                    .or_else(|| old.sasl_username.clone()),
                sasl_password: password,
            }
            .normalize()?;

            reset_status = profile.auth_or_address_changed(&old);
            modify_dto.bootstrap_servers = Some(profile.bootstrap_servers);
            modify_dto.security_protocol = Some(profile.security_protocol);
            modify_dto.sasl_mechanism = Some(profile.sasl_mechanism);
            modify_dto.sasl_username = Some(profile.sasl_username);
            // 空密码不写回，保持库里原值
            let password_given = modify_dto
                .sasl_password
                .clone()
                .flatten()
                .is_some_and(|value| !value.is_empty());
            if !password_given {
                modify_dto.sasl_password = None;
            }
        }

        let enabled_changed = modify_dto
            .enabled
            .is_some_and(|enabled| enabled != old.enabled);
        if enabled_changed || reset_status {
            modify_dto.run_status = Some(if enabled_after {
                KafkaBrokerStatus::None
            } else {
                KafkaBrokerStatus::Disabled
            });
            // 地址/认证变了或重新启用，上一轮的失败原因已经不适用于新配置
            modify_dto.run_msg = Some(None);
        }

        Ok(modify_dto)
    }

    /// # 删除前钩子：还有 Connect 记录挂着时拒绝删除
    ///
    /// 数据库侧已经是 `on delete restrict`，这里提前拦一道把外键错误换成可读提示。
    async fn before_del_by_id_hook(id: U64) -> Result<U64, SvcError> {
        let count = Self::count_kafka_connects_of(id).await?;
        if count > 0 {
            return Err(anyhow!(
                "Kafka 集群(id={})下还有 {count} 台 Kafka Connect 在用它，请先删除或改挂这些记录",
                id.value()
            )
            .into());
        }
        Ok(id)
    }

    /// # 取一条 broker 的实体（带密码）
    ///
    /// 检测与投递信号都要认证信息，而 VO 里没有密码，所以这些路径读实体。
    ///
    /// ## 返回值
    /// * `Ok(BrokerModel)` - 实体
    /// * `Err(anyhow::Error)` - 不存在或读库失败
    pub(crate) async fn load_model(id: U64) -> Result<BrokerModel, anyhow::Error> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        DbzKafkaBrokerDao::get_by_id::<_, BrokerModel>(id, db.as_ref())
            .await
            .map_err(|e| anyhow!("查询 Kafka 集群({})失败: {e}", id.value()))?
            .ok_or_else(|| anyhow!("Kafka 集群({})不存在", id.value()))
    }

    /// # 取一条 broker 的连接参数（含密码）
    ///
    /// 投递信号（`dbz_signal_utils`）用它拿 broker 地址与认证。
    ///
    /// ## 返回值
    /// * `Ok(BrokerConn)` - 可直接用于建连
    /// * `Err(anyhow::Error)` - 不存在或读库失败
    pub(crate) async fn load_conn(id: U64) -> Result<BrokerConn, anyhow::Error> {
        let model = Self::load_model(id).await?;
        Ok(BrokerConn::from_model(&model))
    }

    /// # 检测一台 broker 是否可用
    ///
    /// 手动点「检测」走这里；定时任务走 [`check_all`](Self::check_all)。
    ///
    /// 检测会**回写状态**，所以即使连不上也返回 `Ok`：连不上是业务结论，不是接口错误。
    /// 只有记录不存在这种调用方错误才返回 `Err`。
    ///
    /// ## 参数
    /// * `dto` - Kafka 集群ID
    ///
    /// ## 返回值
    /// * `Ok(Ro<DbzKafkaBrokerCheckVo>)` - 是否连通、topic 数量、耗时、失败原因
    /// * `Err(anyhow::Error)` - 记录不存在
    #[log_call]
    pub async fn check(
        dto: DbzKafkaBrokerCheckDto,
    ) -> Result<Ro<DbzKafkaBrokerCheckVo>, anyhow::Error> {
        let model = Self::load_model(dto.id).await?;
        let conn = BrokerConn::from_model(&model);

        if !model.enabled {
            let result = DbzKafkaBrokerCheckVo {
                reachable: false,
                topic_count: 0,
                elapsed_ms: 0,
                message: Some("记录已禁用，未发起检测".to_string()),
            };
            return Ok(Ro::success("记录已禁用，未发起检测".to_string()).extra(Some(result)));
        }

        let started = Instant::now();
        let checked = KafkaBrokerUtils::check(&conn).await;
        let elapsed_ms = started.elapsed().as_millis() as u64;

        let (result, status, run_msg) = match checked {
            Ok((topic_count, _)) => {
                info!(
                    "Kafka 集群<{}>(id={})检测通过，{} 个 topic，耗时 {elapsed_ms}ms",
                    conn.name, model.id, topic_count
                );
                (
                    DbzKafkaBrokerCheckVo {
                        reachable: true,
                        topic_count,
                        elapsed_ms,
                        message: None,
                    },
                    KafkaBrokerStatus::Normal,
                    None,
                )
            }
            Err(e) => {
                warn!("Kafka 集群<{}>(id={})检测失败: {e}", conn.name, model.id);
                (
                    DbzKafkaBrokerCheckVo {
                        reachable: false,
                        topic_count: 0,
                        elapsed_ms,
                        message: Some(format!("{e:#}")),
                    },
                    KafkaBrokerStatus::ConnectFailed,
                    Some(one_line(&format!("{e:#}"), RUN_MSG_MAX)),
                )
            }
        };

        Self::update_status(&model, status, run_msg).await?;

        let msg = if result.reachable {
            format!("连接正常，{} 个 topic", result.topic_count)
        } else {
            "连接失败".to_string()
        };
        Ok(Ro::success(msg).extra(Some(result)))
    }

    /// # 定时检查所有 broker
    ///
    /// 挂在既有的健康检查任务里（连接器检查之前），一台失败不影响其他台。这里只回写
    /// 状态、不单独告警：broker 不通时，用它发信号的连接器会在下一轮检查里报出来，
    /// 再为单位报一遍等于同一根因刷两条。
    ///
    /// ## 返回值
    /// * `Ok(usize)` - 状态有变化的记录数
    /// * `Err(anyhow::Error)` - 读库失败
    pub async fn check_all() -> anyhow::Result<usize> {
        let db = get_db_conn().map_err(|e| anyhow!("获取数据库连接失败: {e}"))?;
        let brokers = DbzKafkaBrokerDao::list_by_condition::<_, BrokerModel>(
            Condition::all(),
            &None,
            db.as_ref(),
        )
        .await
        .map_err(|e| anyhow!("查询 Kafka 集群列表失败: {e}"))?;

        let mut changed = 0;
        for model in brokers.iter().filter(|model| model.enabled) {
            let (status, run_msg) =
                match KafkaBrokerUtils::check(&BrokerConn::from_model(model)).await {
                    Ok(_) => (KafkaBrokerStatus::Normal, None),
                    Err(e) => {
                        warn!("Kafka 集群<{}>(id={})检测失败: {e}", model.name, model.id);
                        (
                            KafkaBrokerStatus::ConnectFailed,
                            Some(one_line(&format!("{e:#}"), RUN_MSG_MAX)),
                        )
                    }
                };
            // 状态没变也可能换了原因（超时 → 认证失败），所以两个都要比
            if status != KafkaBrokerStatus::from(model.run_status) || run_msg != model.run_msg {
                changed += 1;
                if let Err(e) = Self::update_status(model, status, run_msg).await {
                    warn!("回写 Kafka 集群<{}>状态失败: {e}", model.name);
                }
            }
        }

        // 已禁用的记录不进检查，但也不该停在「正常」上
        for model in brokers.iter().filter(|model| !model.enabled) {
            if KafkaBrokerStatus::from(model.run_status) != KafkaBrokerStatus::Disabled {
                changed += 1;
                if let Err(e) = Self::update_status(model, KafkaBrokerStatus::Disabled, None).await
                {
                    warn!("回写 Kafka 集群<{}>状态失败: {e}", model.name);
                }
            }
        }

        Ok(changed)
    }

    /// # 回写状态与检查结论
    ///
    /// 状态没变、结论也没变就不写库：检查每轮都跑，每轮写一次会让 `update_ms`
    /// 失去意义。**结论变了要写**——一直是「连接失败」但原因从「超时」变成
    /// 「认证失败」时，正是最该让人看到的时刻。
    ///
    /// ## 参数
    /// * `model` - 库里的记录（用来比对是否真的有变化）
    /// * `run_status` - 检查得出的状态
    /// * `run_msg` - 失败原因（成功时传 `None`，库里会被清空）
    async fn update_status(
        model: &BrokerModel,
        run_status: KafkaBrokerStatus,
        run_msg: Option<String>,
    ) -> Result<(), anyhow::Error> {
        if KafkaBrokerStatus::from(model.run_status) == run_status && model.run_msg == run_msg {
            return Ok(());
        }

        let mut modify_dto = DbzKafkaBrokerModifyDto::builder()
            .id(U64(model.id as u64))
            .run_status(run_status)
            ._current_user_id(U64(1))
            ._current_ms(U64(now_ms()))
            .build();
        // 直接赋值：字段是 Option<Option<String>>，Some(None) 才是「置 NULL」
        modify_dto.run_msg = Some(run_msg);

        Self::modify::<DatabaseConnection>(modify_dto, None)
            .await
            .map_err(|e| anyhow!("更新 Kafka 集群<{}>状态失败: {e}", model.name))?;
        info!(
            "Kafka 集群<{}>状态已更新: {}",
            model.name,
            run_status.value()
        );
        Ok(())
    }

    /// # 统计有多少台 Kafka Connect 挂在某个 broker 上
    async fn count_kafka_connects_of(kafka_broker_id: U64) -> Result<usize, SvcError> {
        DbzKafkaConnectSvc::count_by_kafka_broker(kafka_broker_id).await
    }
}
