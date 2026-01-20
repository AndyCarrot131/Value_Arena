# News Classifier with FinBERT

新闻分类服务，使用 FinBERT 进行金融新闻情感分析和股票匹配。

## 功能特性

- **FinBERT 情感分析**: 使用 ProsusAI/finbert 模型进行金融文本情感分析
- **股票匹配**: 自动识别新闻中提及的股票
- **分类系统**:
  - `direct`: 直接提及特定公司/股票
  - `indirect`: 间接相关（行业/板块相关）
  - `macro`: 宏观经济新闻
  - `irrelevant`: 无关新闻

## 架构

```
NewsClassifier
├── FinBERT Model (ProsusAI/finbert)
├── Stock Matching Engine
├── AWS Comprehend (实体/关键词提取)
└── PostgreSQL + S3 Storage
```

## 输入/输出

### 输入
从 `news_articles` 表读取待分类的新闻（`classification = 'pending'`）

### 输出
- **数据库更新**:
  - `classification`: direct/indirect/macro/irrelevant
  - `related_stocks`: 相关股票列表
  - `sentiment`: positive/negative/neutral
  - `sentiment_score`: 置信度 (0.0-1.0)

- **S3 存储**:
  - 路径: `s3://bucket/classified/{classification}/YYYY/MM/DD/{news_id}.json`

## 部署步骤

### 1. 数据库迁移

首先添加 sentiment 字段:

```bash
psql -h <db-host> -U <db-user> -d <db-name> -f SQL/migrations/add_sentiment_columns.sql
```

### 2. 构建 Docker 镜像

```bash
cd services/news-classifier

# 构建镜像（注意：这会下载 FinBERT 模型，约 500MB）
docker build -t news-classifier:finbert .

# 或使用 AWS ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com

docker tag news-classifier:finbert <account-id>.dkr.ecr.us-east-1.amazonaws.com/news-classifier:finbert

docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/news-classifier:finbert
```

### 3. 更新 ECS Task Definition

确保 ECS 任务有足够的资源:

```json
{
  "cpu": "2048",
  "memory": "4096",
  "containerDefinitions": [
    {
      "name": "news-classifier",
      "image": "<account-id>.dkr.ecr.us-east-1.amazonaws.com/news-classifier:finbert",
      "environment": [
        {
          "name": "CLASSIFY_INTERVAL_MINUTES",
          "value": "10"
        }
      ]
    }
  ]
}
```

**资源建议**:
- **CPU**: 2048 (2 vCPU) - FinBERT 推理需要较多计算
- **内存**: 4096 MB (4 GB) - 模型加载需要约 1.5-2 GB 内存

### 4. 部署到 ECS

```bash
# 更新服务
aws ecs update-service \
  --cluster ai-stock-war-cluster \
  --service news-classifier-service \
  --force-new-deployment \
  --region us-east-1
```

## 环境变量

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `CLASSIFY_INTERVAL_MINUTES` | 分类任务执行间隔（分钟） | 10 |
| `TRANSFORMERS_CACHE` | Hugging Face 模型缓存目录 | /app/.cache/huggingface |

## 性能优化

### GPU 支持（可选）

如果使用 GPU 实例（如 ECS with GPU），可以显著提升推理速度:

1. 使用支持 GPU 的基础镜像
2. 安装 CUDA 版本的 PyTorch
3. FinBERT 会自动检测并使用 GPU

### 模型缓存

模型已预下载到 Docker 镜像中，无需每次启动时下载，减少冷启动时间。

## 监控

### 日志示例

```
2026-01-07 10:00:00 - INFO - Loading FinBERT model...
2026-01-07 10:00:05 - INFO - Using device: CPU
2026-01-07 10:00:10 - INFO - FinBERT model loaded successfully
2026-01-07 10:00:15 - INFO - Found 50 pending articles for classification
2026-01-07 10:00:20 - INFO - Classified: direct [positive:0.95] [AAPL] - Apple Reports Record Earnings...
2026-01-07 10:00:21 - INFO - Classified: indirect [negative:0.82] [TSLA, F, GM] - Auto Industry Faces Supply...
```

### CloudWatch 指标

- 分类速度: 约 1-2 秒/篇 (CPU)
- 模型加载时间: 约 5-10 秒
- 内存使用: 2-3 GB

## 故障排查

### 问题: 内存不足 (OOM)

**解决**: 增加 ECS 任务内存到至少 4 GB

### 问题: 模型加载失败

**检查**:
1. 网络连接是否正常（首次启动需要下载模型）
2. 磁盘空间是否足够（模型约 500 MB）

### 问题: 分类速度慢

**优化方案**:
1. 使用 GPU 实例
2. 增加 CPU 资源
3. 减少 batch size

## 测试

本地测试:

```bash
# 安装依赖
pip install -r requirements.txt

# 运行分类器
python classifier.py
```

## 下一步改进

1. [ ] 批量推理优化（一次处理多条新闻）
2. [ ] 添加情感分析缓存（相似内容复用结果）
3. [ ] 支持多语言新闻分析
4. [ ] 添加自定义 FinBERT 微调
5. [ ] 实现 SageMaker Endpoint 方案（更好的扩展性）

## 参考资料

- [FinBERT Paper](https://arxiv.org/abs/1908.10063)
- [ProsusAI/finbert on Hugging Face](https://huggingface.co/ProsusAI/finbert)
- [Transformers Documentation](https://huggingface.co/docs/transformers)
