# dbt ECサイトモデル

このディレクトリには、ECサイトのデータを三層構造でモデリングしたdbtモデルが含まれています。

## ディレクトリ構造

```
models/
├── sources.yml              # ソーステーブルの定義
├── staging/                 # 第1層: Staging層
│   ├── stg_categories.sql
│   ├── stg_customers.sql
│   ├── stg_products.sql
│   ├── stg_orders.sql
│   ├── stg_order_items.sql
│   └── stg_payments.sql
├── intermediate/            # 第2層: Intermediate層（スタースキーマ）
│   ├── dim_categories.sql
│   ├── dim_customers.sql
│   ├── dim_products.sql
│   ├── fct_orders.sql
│   └── fct_payments.sql
└── marts/                   # 第3層: Marts層（集約モデル）
    ├── mart_category_sales.sql
    ├── mart_customer_sales.sql
    ├── mart_product_sales.sql
    ├── mart_daily_sales.sql
    └── mart_order_summary.sql
```

## 三層構造の説明

### 1. Staging層 (`staging/`)
ソーステーブルからデータを読み込み、基本的なクリーンアップとリネームを行います。
- すべて`view`としてマテリアライズ
- カラム名の標準化（例: `name` → `category_name`, `status` → `order_status`）
- 基本的な計算（例: `line_total = quantity * unit_price`）

### 2. Intermediate層 (`intermediate/`) - スタースキーマ
スタースキーマを構成するファクトテーブルとディメンションテーブルを配置します。
- すべて`table`としてマテリアライズ
- 正規化された構造で、分析の基盤となるデータモデル

**ディメンションテーブル（dim_*）:**
- `dim_categories`: カテゴリマスタ
- `dim_customers`: 顧客マスタ
- `dim_products`: 商品マスタ（カテゴリ情報を含む）

**ファクトテーブル（fct_*）:**
- `fct_orders`: 注文明細レベルのファクトテーブル（注文ID、顧客ID、商品ID、数量、金額など）
- `fct_payments`: 支払いトランザクションのファクトテーブル

### 3. Marts層 (`marts/`) - 集約モデル
Intermediate層のスタースキーマから集約・分析したビジネス用モデルです。
- すべて`table`としてマテリアライズ
- ビジネスロジックやセグメント情報を含む

**集約モデル一覧:**
- `mart_category_sales`: カテゴリ別の売上集約（注文数、商品数、売上金額、売上カテゴリなど）
- `mart_customer_sales`: 顧客別の売上集約（注文履歴、支払い状況、顧客セグメントなど）
- `mart_product_sales`: 商品別の売上集約（販売実績、価格トレンド、販売カテゴリなど）
- `mart_daily_sales`: 日次売上集約（日別の注文数、売上金額、支払い状況など）
- `mart_order_summary`: 注文サマリー（注文ごとの集約情報、支払い状況など）

## データフロー

```
ソーステーブル (ecommerce_system)
    ↓
Staging層 (stg_*)
    ↓
Intermediate層 (スタースキーマ: dim_*, fct_*)
    ↓
Marts層 (集約モデル: mart_*)
```

## 使用方法

1. ソーステーブルの定義を確認:
   ```bash
   dbt list --select source:*
   ```

2. Staging層のモデルを実行:
   ```bash
   dbt run --select staging.*
   ```

3. すべてのモデルを実行:
   ```bash
   dbt run
   ```

4. モデルの依存関係を確認:
   ```bash
   dbt docs generate
   dbt docs serve
   ```

