import boto3
import psycopg2
import json
import logging
from typing import Optional, List, Dict
from bs4 import BeautifulSoup  # 需要安装: pip install beautifulsoup4

logger = logging.getLogger(__name__)

class PDFTextExtractor:
    """从 S3 中的 HTML/PDF 文件提取文本 (针对 SEC HTML 优化)"""
    
    def __init__(self):
        self.load_config()
        self.setup_database()
        self.setup_aws_clients()
    
    def load_config(self):
        """从 Secrets Manager 加载配置"""
        try:
            secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
            response = secrets_client.get_secret_value(
                SecretId='ai-stock-war/database-config'
            )
            self.config = json.loads(response['SecretString'])
            logger.info("Configuration loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def setup_database(self):
        """连接 PostgreSQL"""
        try:
            self.db_conn = psycopg2.connect(
                host=self.config['db_host'],
                port=self.config['db_port'],
                database=self.config['db_name'],
                user=self.config['db_user'],
                password=self.config['db_password']
            )
            logger.info("Database connected successfully")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def setup_aws_clients(self):
        """初始化 AWS S3 客户端"""
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        logger.info("S3 client initialized")
    
    def get_pending_reports(self, limit: int = 10) -> List[Dict]:
        """获取待提取文本的报告"""
        cursor = self.db_conn.cursor()
        cursor.execute("""
            SELECT id, symbol, pdf_s3_key, txt_s3_key
            FROM financial_reports 
            WHERE extraction_status = 'pending'
            ORDER BY filing_date DESC
            LIMIT %s
        """, (limit,))
        
        reports = []
        for row in cursor.fetchall():
            reports.append({
                'id': row[0],
                'symbol': row[1],
                'pdf_s3_key': row[2],
                'txt_s3_key': row[3]
            })
        
        cursor.close()
        return reports

    def extract_text_from_s3_html(self, bucket: str, key: str) -> Optional[str]:
        """从 S3 读取 HTML 并提取纯文本 (跳过 XBRL Header)"""
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            html_content = response['Body'].read().decode('utf-8')
            
            soup = BeautifulSoup(html_content, 'lxml')
            
            # 1. 移除脚本、样式以及 XBRL 隐藏的元数据 header (这行是关键)
            for junk in soup(["script", "style", "ix:header"]):
                junk.decompose()

            # 2. 提取文本，使用换行符作为分隔符以保持基本的视觉结构
            text = soup.get_text(separator='\n')
            
            # 3. 清理多余的空白行：保留非空行并去除首尾空格
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            
            # 4. 重新组合
            clean_text = '\n'.join(lines)
            
            return clean_text
        except Exception as e:
            logger.error(f"HTML extraction failed for {key}: {e}")
            return None

    def save_text_to_s3(self, text: str, s3_key: str) -> bool:
        """保存提取后的文本到 S3"""
        try:
            self.s3_client.put_object(
                Bucket=self.config['s3_rss_bucket'],
                Key=s3_key,
                Body=text.encode('utf-8'),
                ContentType='text/plain'
            )
            return True
        except Exception as e:
            logger.error(f"Failed to save text to S3: {e}")
            return False

    def update_extraction_status(self, report_id: int, status: str, page_count: int = 0):
        """更新数据库状态"""
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                UPDATE financial_reports 
                SET extraction_status = %s,
                    extraction_date = CURRENT_TIMESTAMP,
                    page_count = %s
                WHERE id = %s
            """, (status, page_count, report_id))
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Failed to update database status: {e}")
            self.db_conn.rollback()
        finally:
            cursor.close()

    def process_batch(self):
        """处理待提取任务"""
        logger.info("=" * 50)
        logger.info("Starting HTML text extraction task")
        
        reports = self.get_pending_reports(limit=20) # HTML 解析很快，可以增加 batch
        
        if not reports:
            logger.info("No pending reports")
            return
        
        for report in reports:
            logger.info(f"Processing: {report['symbol']} - {report['pdf_s3_key']}")
            
            # 提取文本
            text = self.extract_text_from_s3_html(
                self.config['s3_rss_bucket'],
                report['pdf_s3_key']
            )
            
            if text:
                # 简单估算页数 (SEC HTML 文本量较大，约 3000 字一页)
                page_count = len(text) // 3000
                
                if self.save_text_to_s3(text, report['txt_s3_key']):
                    self.update_extraction_status(report['id'], 'completed', max(1, page_count))
                    logger.info(f"Successfully processed {report['symbol']}")
                else:
                    self.update_extraction_status(report['id'], 'failed')
            else:
                self.update_extraction_status(report['id'], 'failed')

        logger.info("Batch extraction complete")
        logger.info("=" * 50)

    def close(self):
        if hasattr(self, 'db_conn'):
            self.db_conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    extractor = PDFTextExtractor()
    try:
        extractor.process_batch()
    finally:
        extractor.close()