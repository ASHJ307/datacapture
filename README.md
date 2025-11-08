# A股财务数据抓取工具

## 简介

本程序使用 AKShare 库获取 A 股上市公司的财务数据，自动计算关键财务指标（如营业收入、净利润、ROE 等），并支持与去年同期数据进行对比分析。程序可以输出格式化的表格到终端，也可以导出为美观的 Excel 文件。

## 环境配置

### 1. 安装 Conda

如果还没有安装 Conda，请先下载并安装 [Miniconda](https://docs.conda.io/en/latest/miniconda.html) 或 [Anaconda](https://www.anaconda.com/download)。

### 2. 创建 Conda 环境

在项目目录下打开终端，执行以下命令创建新的 Conda 环境：

```bash
conda create -n datacapture python=3.10
```

### 3. 激活环境

```bash
conda activate datacapture
```

### 4. 安装依赖

```bash
pip install akshare pandas openpyxl
```

或者使用 requirements.txt（如果存在）：

```bash
pip install -r requirements.txt
```

## 使用方法

### 基本用法

查询单只股票（默认查询 600418）：

```bash
python akshare_financials.py
```

查询指定股票：

```bash
python akshare_financials.py --symbol 600418
```

查询多只股票：

```bash
python akshare_financials.py --symbols 600418 000333 601012
```

### 导出 Excel

将结果导出为 Excel 文件：

```bash
python akshare_financials.py --symbol 600418 --excel output.xlsx
```

多股票模式下，可以指定目录：

```bash
python akshare_financials.py --symbols 600418 000333 --excel ./reports/
```

### 自定义报告期

默认查询以下报告期：
- 2025三季报
- 2025中报
- 2025一季报
- 2024年报

可以通过 `--report` 参数自定义：

```bash
python akshare_financials.py --symbol 600418 --report 2025Q3 2025-09-30 2025Q2 2025-06-30
```

注意：`--report` 参数需要成对出现（报告期名称 和 日期）。

### 查看帮助

```bash
python akshare_financials.py --help
```

## 输出说明

程序会在终端输出格式化的财务指标表格，包含以下主要指标：

- **利润表**：营业收入、营业收入增长率、毛利率、营业利润率、净利润、净利润率、归母净利润
- **ROE**：归母净利润率、总资产周转率、权益乘数、ROE
- **现金流量表**：经营活动现金流量净额、净利润现金保障倍数、资本性支出、投资活动现金流量净额、自由现金流、现金收入比率
- **资产负债表**：资产负债率、流动比率、净资产

每个指标都会显示本期值和去年同期值，方便进行对比分析。

## 注意事项

1. 股票代码格式：支持 6 位数字代码（如 `600418`），程序会自动识别沪深交易所
2. 数据来源：数据来自 AKShare 库，需要网络连接
3. Excel 导出：导出的 Excel 文件包含格式化的表格，适合进一步分析和展示

