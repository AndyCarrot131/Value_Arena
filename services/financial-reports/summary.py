import boto3
import psycopg2
import json
import logging
import requests
import os
import time
from typing import List, Dict

logger = logging.getLogger(__name__)

class FinancialReportSummarizer:
    def __init__(self):
        # 1. 从 Task Definition 注入的环境变量读取配置
        self.api_url = os.getenv("SUMMARY_API_URL")
        self.api_key = os.getenv("SUMMARY_API_KEY")
        self.model = os.getenv("SUMMARY_MODEL")
        self.s3_bucket = os.getenv("S3_RSS_BUCKET")
        
        if not self.api_key:
            raise ValueError("Environment variable SUMMARY_API_KEY is required")

        self.setup_connections()
        
    def setup_connections(self):
        """初始化数据库和 S3 客户端"""
        try:
            # 1. 从 Secrets Manager 获取统一配置
            secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
            response = secrets_client.get_secret_value(
                SecretId='ai-stock-war/database-config'
            )
            db_config = json.loads(response['SecretString'])
            
            # 2. 建立数据库连接
            self.db_conn = psycopg2.connect(
                host=db_config['db_host'],
                port=db_config['db_port'],
                database=db_config['db_name'],
                user=db_config['db_user'],
                password=db_config['db_password']
            )
            
            # 3. 初始化 S3 客户端
            self.s3_client = boto3.client('s3', region_name='us-east-1')

            # 4. 确定 S3 桶名 (优先使用 Secret 中的配置，确保与 extractor 一致)
            # 根据你的配置，这里将获取到 "ai-stock-rss-data-131"
            self.s3_bucket = db_config.get('s3_rss_bucket') or os.getenv("S3_RSS_BUCKET")
            
            if not self.s3_bucket:
                raise ValueError("S3 bucket name not found in Secrets Manager or Environment Variables")

            logger.info(f"Summarizer: DB and S3 connections established. Using bucket: {self.s3_bucket}")
            
        except Exception as e:
            logger.error(f"Summarizer setup failed: {e}")
            raise
        
    def close(self):
        """关闭数据库连接"""
        if hasattr(self, 'db_conn') and self.db_conn:
            self.db_conn.close()
            logger.info("Summarizer: Database connection closed.")

    def get_pending_summaries(self, limit: int = 5) -> List[Dict]:
        """获取已提取文本但未生成摘要的记录"""
        cursor = self.db_conn.cursor()
        query = """
            SELECT id, symbol, txt_s3_key, report_type, fiscal_year, fiscal_quarter 
            FROM financial_reports 
            WHERE extraction_status = 'completed' AND summary_en IS NULL
            ORDER BY filing_date DESC LIMIT %s
        """
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()
        cursor.close()
        return [{
            'id': r[0], 'symbol': r[1], 'key': r[2], 
            'type': r[3], 'year': r[4], 'quarter': r[5]
        } for r in rows]

    def call_ai_api(self, text: str) -> str:
        """调用中转 API 生成深度摘要"""
        # 使用我们测试通过的高质量增强版英文 Prompt
        prompt = """
        Please analyze the provided financial report and generate a professional, data-driven summary in English. 
        Focus on specific insights. 
        Sections:
        1. [Key Financial Metrics]: Markdown table (Revenue by segment, Net Income, Diluted EPS, Gross Margin, and RPO). Include YoY growth.
        2. [Strategic Analysis: AI & Product]: Detail AI monetization and new product progress.
        3. [Risk & Macro Outlook]: Identify specific risks (regulatory, competitive) and management guidance.
        """
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # 截取前 100,000 字符以平衡成本和完整度
        payload = {
            "model": self.model,
            "messages": [
                {"role": "user", "content": f"{prompt}\n\nReport Content:\n{text}"}
            ],
            "temperature": 0.1
        }
        
        # 180s 超时以应对长文本分析
        response = requests.post(self.api_url, headers=headers, json=payload, timeout=300)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
    
    def process_batch(self):
        """执行批处理摘要任务"""
        # 1. 获取待提取摘要的报告（已完成文本提取且摘要为空）
        reports = self.get_pending_summaries()
        
        if not reports:
            logger.info("No pending reports found for summarization.")
            return

        for report in reports:
            try:
                if not report['key']:
                    logger.warning(f"No S3 key for {report['symbol']}, skipping.")
                    continue
                logger.info(f"Summarizing {report['symbol']} {report['year']} {report['type']}...")
                
                full_text = self.read_text_from_s3(report['key'])
                if not full_text:
                    logger.warning(f"Skipping {report['symbol']}: No text content retrieved from S3.")
                    continue
                
                # 3. 调用 AI API 进行总结（基于 Gemini-3-Pro）
                summary_text = self.call_ai_api(full_text)
                
                if not summary_text:
                    logger.error(f"AI API returned empty summary for {report['symbol']}")
                    continue
                
                # 4. 将生成的摘要保存回数据库 summary_en 字段
                self.save_summary_to_db(report['id'], summary_text)
                
                logger.info(f"Successfully saved summary for {report['symbol']}")
                
                # 5. 频率限制保护：防止 API 中转站限流
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing {report['symbol']}: {e}")

    def read_text_from_s3(self, key):
        """
        从 S3 读取纯文本文件
        对应 extractor.py 的 save_text_to_s3 逻辑
        """
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=key)
            return response['Body'].read().decode('utf-8')
        except self.s3_client.exceptions.NoSuchKey:
            logger.error(f"S3 file not found: {key}")
            return None
        except Exception as e:
            logger.error(f"Error reading from S3: {e}")
            return None

    def save_summary_to_db(self, report_id, summary_text):
        """
        将摘要持久化到数据库
        """
        cursor = self.db_conn.cursor()
        try:
            cursor.execute(
                "UPDATE financial_reports SET summary_en = %s WHERE id = %s",
                (summary_text, report_id)
            )
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Database update failed for report {report_id}: {e}")
            self.db_conn.rollback()
            raise
        finally:
            cursor.close()