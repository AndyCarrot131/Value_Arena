# AI Stock War - Decision Service

AI 投资决策服务，让不同 AI 模型（Claude, GPT, Gemini）在相同投资规则下竞技炒股。

## 项目概述

这是一个价值投资导向的 AI 竞技系统，通过 RAG（Retrieval-Augmented Generation）让 AI 从历史决策中学习，在严格的投资规则约束下进行长期持有和深度思考。

### 核心特性

- **双账户系统**: 70% 长期账户（≥30 天持有）+ 30% 短期账户（灵活交易）
- **RAG 学习**: 使用 Bedrock Knowledge Base 检索历史决策，从成功和失败中学习
- **严格合规**: 6 条投资规则验证（股票池、交易次数、Wash Trade 等）
- **两阶段运行**: 学习期（Day 1-7）+ 交易期（Day 8+）
- **多 AI 竞技**: Claude, GPT, Gemini 独立决策，顺序执行

## 架构组件

### 核心基础设施（core/）
- `secrets_manager.py` - AWS Secrets Manager 配置加载
- `database.py` - PostgreSQL 连接池管理
- `redis_client.py` - Redis 客户端（实时股价）
- `ai_client.py` - BaiCai API 统一调用
- `bedrock_client.py` - Bedrock Titan V2 Embedding + KB Retrieve
- `opensearch_client.py` - OpenSearch 向量数据库操作
- `logger.py` - 结构化日志（JSON 格式）

### 业务服务（services/）
- `data_collector.py` - 收集新闻、财报、股价
- `memory_manager.py` - 管理 AI 状态、关键事件、钱包
- `rag_retriever.py` - RAG 检索相似历史决策
- `decision_validator.py` - 6 条规则验证
- `portfolio_executor.py` - 原子性交易执行
- `ai_orchestrator.py` - 编排多个 AI 顺序调用

### 工作流（workflows/）
- `daily_learning.py` - 学习期流程（Day 1-7）
- `hourly_news_analysis.py` - 每小时新闻分析
- `daily_summary.py` - 每晚日总结（含 RAG 检索）
- `trading_decision.py` - 工作日交易决策
- `weekly_report.py` - 周五周总结 + 个股分析

## 快速开始

### 本地开发

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置环境变量
cp .env.example .env
# 编辑 .env 填入配置

# 3. 运行学习期（Day 1-7）
python main.py --workflow daily_learning --agent_id claude

# 4. 运行交易期（Day 8+）
python main.py --workflow trading_decision --agent_id all
```

### Docker 构建

```bash
# 构建镜像
docker build -t ai-decision:latest .

# 运行容器
docker run --rm \
  -e AWS_REGION=us-east-1 \
  ai-decision:latest \
  --workflow daily_summary --agent_id claude
```

### ECS 部署

```bash
# 1. 推送到 ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 314146313406.dkr.ecr.us-east-1.amazonaws.com
docker tag ai-decision:latest 314146313406.dkr.ecr.us-east-1.amazonaws.com/ai-decision:latest
docker push 314146313406.dkr.ecr.us-east-1.amazonaws.com/ai-decision:latest

# 2. 创建 Task Definition
aws ecs register-task-definition --cli-input-json file://task-definition.json

# 3. Lambda 触发运行（通过 EventBridge）
```

## 命令行参数

```
python main.py --workflow <workflow_name> --agent_id <agent_id>

参数:
  --workflow         工作流名称
                     - daily_learning: 学习期流程
                     - hourly_news_analysis: 每小时新闻分析
                     - daily_summary: 每晚日总结
                     - trading_decision: 工作日交易决策
                     - weekly_report: 周五周总结
  
  --agent_id         AI ID
                     - claude: Claude AI
                     - gpt: GPT AI
                     - gemini: Gemini AI
                     - all: 所有 AI（顺序执行）
```

## 投资规则

### 6 条核心规则
1. **股票池限制**: 只能交易 20 只指定股票
2. **交易频率**: 每周最多 5 次交易（周一重置）
3. **钱包余额**: 根据 position_type 检查对应钱包余额
4. **账户配比**: 长期账户 70%，短期账户 30%（固定不变）
5. **Wash Trade**: 长期账户首次买入后 30 天内不能卖出
6. **违规记录**: 所有违规记录到 `compliance_violations` 表

### 双账户系统
- **长期账户（70% 资金）**: 
  - 预期持有 1-10 年
  - 首次买入后最少持有 30 天才能卖出
  - 适合价值投资
  
- **短期账户（30% 资金）**: 
  - 可快进快出，今天买明天卖
  - 无持有期限制
  - 适合波段操作

## 数据流

### 学习期（Day 1-7）
```
PostgreSQL (新闻、财报) + Redis (股价)
    ↓
AI 分析 20 只股票
    ↓
保存到 ai_learning_logs + hourly_news_analysis
```

### 交易期 - 交易决策
```
PostgreSQL (日总结、持仓、钱包) + Redis (股价)
    ↓
Bedrock KB API 检索相似决策
    ↓
AI 生成决策（JSON）
    ↓
6 条规则验证
    ↓
原子性执行交易
    ↓
更新数据库 + 写入 OpenSearch
```

## 环境变量

详见 `.env.example`，主要配置项：

- `AWS_REGION`: AWS 区域（默认 us-east-1）
- `SECRET_DATABASE_CONFIG`: 数据库配置 Secret 名称
- `SECRET_OPENSEARCH_CONFIG`: OpenSearch 配置 Secret 名称
- `LOG_LEVEL`: 日志级别（INFO/DEBUG/WARNING/ERROR）
- `LOG_FORMAT`: 日志格式（json/text）

## 日志格式

结构化 JSON 日志输出到 CloudWatch：

```json
{
  "timestamp": "2025-01-05T14:30:00Z",
  "level": "INFO",
  "agent_id": "claude",
  "workflow": "trading_decision",
  "message": "Generated trading decision",
  "details": {
    "symbol": "NVDA",
    "decision_type": "BUY",
    "quantity": 10
  }
}
```

## 测试

```bash
# 运行单元测试
pytest tests/unit -v

# 运行集成测试
pytest tests/integration -v

# 生成覆盖率报告
pytest --cov=. --cov-report=html
```

## 故障排查

### 常见问题

1. **AI 调用失败**
   - 检查 BaiCai API Key 是否正确
   - 查看日志中的错误详情
   - 确认网络连接正常

2. **数据库连接失败**
   - 检查 Security Group 配置
   - 确认 VPC 和 Subnet 配置正确
   - 验证数据库凭证

3. **RAG 检索无结果**
   - 确认 OpenSearch 中有数据
   - 检查 Knowledge Base ID 是否正确
   - 验证 embedding 生成成功

## 监控与告警

- **CloudWatch Logs**: `/aws/ecs/ai-decision`
- **自定义指标**: AI 调用延迟、决策验证失败率、交易成功率
- **告警规则**: 错误率 > 5%、超时、合规违规

## 相关文档

- [项目详细设计](../../AI_promote/project_detail.md)
- [系统架构](../../AI_promote/project_Architect.md)
- [开发进度](../../AI_promote/project_process.md)
- [数据库 Schema](../../SQL/DB_schema.txt)

## License

内部项目，禁止外部传播。