FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 预下载 FinBERT 模型到镜像中 (减少冷启动时间)
RUN python -c "from transformers import AutoTokenizer, AutoModelForSequenceClassification; \
    model_name = 'ProsusAI/finbert'; \
    print('Downloading FinBERT model...'); \
    AutoTokenizer.from_pretrained(model_name); \
    AutoModelForSequenceClassification.from_pretrained(model_name); \
    print('Model downloaded successfully')"

# 复制应用代码
COPY . .

# 设置环境变量
ENV PYTHONUNBUFFERED=1
ENV CLASSIFY_INTERVAL_MINUTES=10
ENV TRANSFORMERS_CACHE=/app/.cache/huggingface
ENV HF_HOME=/app/.cache/huggingface

# 创建缓存目录
RUN mkdir -p /app/.cache/huggingface

# 健康检查
HEALTHCHECK --interval=5m --timeout=3s \
  CMD python -c "import psycopg2; import torch; import transformers" || exit 1

# Set AWS region
ENV AWS_DEFAULT_REGION=us-east-1

# Create non-root user for security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Default command
ENTRYPOINT ["python", "main.py"]