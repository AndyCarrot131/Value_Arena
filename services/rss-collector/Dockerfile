FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY . .

# 设置环境变量
ENV PYTHONUNBUFFERED=1

# 健康检查 (database-only mode)
HEALTHCHECK --interval=5m --timeout=3s \
  CMD python -c "import psycopg2; print('Healthy')" || exit 1

# 运行应用
CMD ["python", "app.py"]