# Table Dictionary

## `daily`

- `market`: 市场代码（`sh`/`sz`/`bj`）
- `symbol`: 证券代码（6位）
- `secid`: `market + symbol`
- `trade_date`: 交易日（`yyyymmdd`）
- `open`, `high`, `low`, `close`: 开高低收
- `amount`: 成交额（通达信原值）
- `volume`: 成交量（通达信原值）
- `hfq_factor`: 后复权累计因子（基于已确认 `gbbq` 事件正向递推）
- `hfq_open`: 后复权开盘价（`open * hfq_factor`）
- `hfq_high`: 后复权最高价（`high * hfq_factor`）
- `hfq_low`: 后复权最低价（`low * hfq_factor`）
- `hfq_close`: 后复权收盘价（`close * hfq_factor`）

注意：`hfq_open/high/low/close` 更适合收益率、形态与趋势研究；涨跌停、撮合规则、盘口限制等研究仍应优先使用原始 OHLC。

## `min5`

- `market`, `symbol`, `secid`: 同 `daily`
- `trade_date`: 交易日（`yyyymmdd`）
- `bar_time`: K线时间（`yyyymmddHHMM`）
- `open`, `high`, `low`, `close`: 5分钟开高低收
- `amount`: 成交额
- `volume`: 成交量

## `reference/security_master`

- `market`, `symbol`, `secid`: 证券主键
- `name`: 证券简称
- `pinyin_hint`: 拼音缩写提示（若存在）
- `tnf_flag_u32_272`, `tnf_flag_u32_276`, `tnf_type_u16`, `tnf_market_u16`: `tnf` 原始标志位（厂商私有字段，保留原值）
- `source_file`: 来源 `tnf` 文件
- `row_index`: 文件内行号

注意：`name` 反映的是采集时点的当前简称，不能直接作为历史逐日 ST 状态来源。

## `reference/security_profile`

来源 `base.dbf`。除 `market/symbol/secid/source_file` 外，保留通达信原字段名。

- `SC`: 市场数字代码（0深/1沪/2北）
- `GPDM`: 股票代码
- `GXRQ`: 更新日期
- `ZGB`: 总股本
- `GJG`: 国家股（历史字段）
- `FQRFRG`: 发起人法人股
- `FRG`: 法人股
- `BG`: B股
- `HG`: H股
- `LTAG`: 流通A股
- `ZGG`: 职工股
- `ZPG`: 转配股（历史字段）
- `ZZC`: 总资产
- `LDZC`: 流动资产
- `GDZC`: 固定资产
- `WXZC`: 无形资产
- `CQTZ`: 长期投资
- `LDFZ`: 流动负债
- `CQFZ`: 长期负债
- `ZBGJJ`: 资本公积金
- `JZC`: 净资产
- `ZYSY`: 主营收入
- `ZYLY`: 主营利润
- `QTLY`: 其他利润
- `YYLY`: 营业利润
- `TZSY`: 投资收益
- `BTSY`: 补贴收入
- `YYWSZ`: 营业外收支
- `SNSYTZ`: 上年损益调整
- `LYZE`: 利润总额
- `SHLY`: 税后利润
- `JLY`: 净利润
- `WFPLY`: 未分配利润
- `TZMGJZ`: 通达信原字段（每股净资相关，保留原名）
- `DY`: 地域代码
- `HY`: 行业代码
- `ZBNB`: 报表类别标志
- `SSDATE`: 上市日期
- `MODIDATE`: 修改日期（通达信格式）
- `GDRS`: 股东人数

## `reference/security_industry_map`

- `market_digit`, `market`, `symbol`, `secid`: 证券主键
- `tdx_industry_code`: 通达信行业编码（如 `T1001`）
- `reserved_1`, `reserved_2`: 预留字段
- `csi_industry_code`: 另一套行业编码（如 `X500102`）
- `source_file`, `line_no`: 溯源字段

## `reference/block_definition`

- `block_source`: 来源系统（`tdxzs`/`infoharbor`/`csiblock`/`hkblock` 等）
- `block_kind`: 板块类别
- `block_name`: 板块名称
- `block_code`: 板块代码或合成ID
- `member_count_hint`: 成分数量提示（若有）
- `create_date`, `update_date`: 日期字段（若有）
- `extra_1`, `extra_2`: 原文件附加字段（若有）
- `field_03`~`field_06`: `tdxzs*.cfg` 原字段
- `block_id`: 板块唯一ID（用于关联成员）
- `source_file`, `line_no`: 溯源字段

## `reference/block_member`

- `block_source`, `block_id`, `block_code`, `block_name`: 关联板块
- `member_market_digit`: 原始市场数字（若有）
- `market`, `symbol`, `secid`: 解析后的证券主键（无法识别则为空）
- `raw_member`: 原始成分串
- `source_file`, `line_no`: 溯源字段

注意：板块成员表默认是采集时点快照，不是逐日成分历史；`ST板块` 等成员不能直接回填到历史每个交易日。

## `reference/index_snapshot`

来源 `tdxzsbase.cfg`，固定 26 列。可确定字段：

- `market_digit`, `market`, `symbol`, `secid`
- `trade_date`: 交易日
- `direction_flag`: 方向/状态标志
- `metric_01`~`metric_22`: 通达信指数快照指标（厂商私有含义，保留原顺序数值）
- `source_file`, `line_no`

## `reference/security_business`

来源 `specgpext.txt`。

- `market_digit`, `market`, `symbol`, `secid`
- `business_summary`: 主营业务摘要
- `field_04`, `field_05`, `field_06`: 原文件扩展字段
- `related_etf_code`: 关联ETF代码（若有）
- `related_weight`: 关联权重（若有）
- `reserved`: 预留
- `source_file`, `line_no`

## `reference/etf_meta`

来源 `specetfdata.txt`（ETF 扩展元数据，固定 8 列，逗号分隔）。

- `market_digit`, `market`, `symbol`, `secid`: 证券主键
- `tracking_code`: 跟踪标的代码（指数/主题代码；部分为空）
- `tracking_market_digit`: 跟踪标的市场数字代码（厂商编码）
- `manager_code`: 基金管理人代码（如 `jjjl0000033`）
- `reserved`: 预留字段
- `list_date`: 上市日期（`yyyymmdd`，若可解析）
- `first_trade_date`: 首个交易日期（`yyyymmdd`，若可解析）
- `source_file`, `line_no`: 溯源字段

## `reference/lof_meta`

来源 `speclofdata.txt`（LOF 扩展元数据，固定 6 列，逗号分隔）。

- `market_digit`, `market`, `symbol`, `secid`: 证券主键
- `tracking_code`: 跟踪标的代码（指数/主题代码）
- `tracking_market_digit`: 跟踪标的市场数字代码（厂商编码）
- `manager_code`: 基金管理人代码
- `reserved`: 预留字段
- `source_file`, `line_no`: 溯源字段

## `reference/fund_nav_snapshot`

来源 `specjjdata.txt`（基金日快照，固定 6 列，逗号分隔）。

- `market_digit`, `market`, `symbol`, `secid`: 证券主键
- `tracking_code`: 跟踪标的代码（若有）
- `trade_date`: 快照日期（`yyyymmdd`）
- `metric_01`, `metric_02`: 快照数值字段（厂商私有语义，保留原顺序）
- `source_file`, `line_no`: 溯源字段

## `reference/map_offsets`

来源 `base.map` 与 `gbbq.map`。

- `map_name`: `base` 或 `gbbq`
- `symbol`, `market`, `secid`
- `offset`: 原映射偏移值
- `source_file`, `line_no`

## `reference/derivatives_meta`

来源 `code2name.ini`（期货/期权合约元数据）。

- `instrument_prefix`: 合约前缀
- `instrument_name`: 合约简称
- `exchange_code`: 交易所代码
- `contract_type`: 合约类型
- `contract_month`: 合约月份
- `expire_date`: 到期日
- `multiplier`: 合约乘数
- `price_tick`: 最小变动价位
- `fee_open`, `fee_close`: 费用/保证金相关字段（按原文件位置）
- `margin_ratio`: 比率字段（按原文件位置）
- `fee_unit`, `quote_unit`
- `session_type`: 交易时段类型
- `price_decimals`: 小数位数
- `reserved_flag`: 预留标志
- `delivery_rule`: 交割规则文本
- `source_file`, `line_no`

## `reference/corporate_action`

来源 `T0002/hq_cache/gbbq`（通达信「股本变迁/除权除息」二进制文件，29 字节/条）。

**新版 TDX 的 `gbbq` 文件前 24 字节加密**（类 Feistel + 内置 S-box），后 5 字节明文。
采集层调用 `pytdx.reader.gbbq_reader.GbbqReader` 解密，随后按 pytdx 原生字段落入本表。

- `market_digit`: 原始市场数字（0=深 / 1=沪）
- `market`, `symbol`, `secid`: 证券主键
- `ex_date`: 除权除息日（`yyyymmdd`）
- `category`: 事件类型。实测常见取值：`1` 派息、`2` 配股、`3` 送转、`10` 股权分置、`11` 份额拆并
- `field_01`..`field_04`: pytdx 原生字段依次落入 4 列，语义随 `category` 变化
  - `category=1`：`field_01` 为每 10 份派现（元）
  - `category=11`：`field_03` 为拆并比例因子（新份额/旧份额）
- `source_file`, `line_no`: 溯源字段（`line_no` 为记录在文件中的顺序）

## `reference/source_manifest`

- `source_file`: `hq_cache` 文件路径
- `size`: 文件大小
- `mtime_ns`: 文件修改时间（纳秒）
- `parsed`: 是否被结构化解析
- `sha256_head4096`: 文件前 4096 字节哈希（用于快速变更识别）
