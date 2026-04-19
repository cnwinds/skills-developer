# 通用 trade table 输入列定义

下面列出 engine 真正用到的列，以及 adapter 会自动识别的别名。
传进来的 CSV 有任意附加列都会被忽略（不会报错），可以用来放策略元信息（因子值、tier、备注等）。

## 必填列

| 列名 | 类型 | 说明 |
| --- | --- | --- |
| `code` | str | 证券代码，如 `688006.SH` / `300xxx.SZ`。 |
| `bucket` | str | 桶 / 板块 / 策略变体名。engine 会按它分独立账户。 |
| `buy_date` | 日期 | 建仓日，`YYYYMMDD` / `YYYY-MM-DD` / datetime 都可以，会清洗成 8 位数字字符串。 |
| `buy_price` | float | 建仓价。必须 > 0。 |
| `sell_date` | 日期 | 平仓日。 |
| `sell_price` | float | 平仓价。必须 > 0。 |

## 可选列

| 列名 | 类型 | 说明 |
| --- | --- | --- |
| `name` | str | 证券简称；缺省用 `code` 填充。 |
| `signal_date` | 日期 | 信号日；只作为展示字段，不进入撮合逻辑。 |
| `rank` | int | 当日撮合顺序。rank 小的先走，缺省按 `code` 字典序。|

## 自动别名

adapter 会把下列列名自动改写成标准名，方便直接喂 Qinglong/其它回测的中间产物：

| 原列名 | 映射到 |
| --- | --- |
| `buy_date_25d` | `buy_date` |
| `sell_date_25d` | `sell_date` |
| `buy_price_25d` | `buy_price` |
| `sell_price_25d` | `sell_price` |
| `setting_rank` / `tier_rank` | `rank` |
| `pool` / `board` / `sector` | `bucket` |
| `买入日` / `买入价` / `卖出日` / `卖出价` | `buy_date` / `buy_price` / `sell_date` / `sell_price` |
| `ts_code` / `symbol` | `code` |

**含歧义的列不走内置别名**（例如 `sell_date_8d` vs `sell_date_25d`），请在 CLI 上
显式指定：`--col-map sell_date=sell_date_8d`。

## 不接受的情况

- `sell_date` 还没到或 `sell_price` 缺失 —— engine 会把所有输入都当作「到期能卖的笔」，否则会因为「仍有未平仓」抛错。未到期的记录请在上游过滤掉。
- `buy_price`、`sell_price` 为 0 或负数 —— 会在 `_normalize` 里被丢掉。
- `buy_date > sell_date` —— 会导致买入挂在卖出事件之后，逻辑上顺序不会出错，但请避免这种脏数据。

## 一张最小可用的 CSV 例子

```csv
code,name,bucket,signal_date,buy_date,buy_price,sell_date,sell_price,rank
688006.SH,杭可科技,KCB,20191209,20191210,37.23,20200114,47.01,1
688058.SH,宝兰德,KCB,20191209,20191210,97.01,20200114,117.50,2
300xxx.SZ,XXXXX,CYB,20191209,20191210,12.34,20200114,13.56,1
```
