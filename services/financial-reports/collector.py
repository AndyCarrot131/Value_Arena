import boto3
import psycopg2
import requests
import json
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import time
import os

logger = logging.getLogger(__name__)


class FinancialReportCollector:
    """Financial Report Collector - Fetches reports from SEC EDGAR"""
    
    # SEC EDGAR User-Agent（必须提供）
    USER_AGENT = "AI Stock War ai.stock.war@example.com"
    
    def __init__(self):
        self.load_config()
        self.setup_database()
        self.setup_aws_clients()
        self.load_stock_symbols()
    
    def load_config(self):
        """Load configuration from Secrets Manager"""
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
        """Connect to PostgreSQL"""
        try:
            self.db_conn = psycopg2.connect(
                host=self.config['db_host'],
                port=self.config['db_port'],
                database=self.config['db_name'],
                user=self.config['db_user'],
                password=self.config['db_password']
            )
            logger.info("Database connection successful")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def setup_aws_clients(self):
        """Initialize AWS clients"""
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        logger.info("AWS clients initialized")
    
    def load_stock_symbols(self):
        """Load stock symbol list"""
        cursor = self.db_conn.cursor()
        cursor.execute("""
            SELECT symbol, name 
            FROM stocks 
            WHERE enabled = TRUE AND type = 'stock'
        """)
        
        self.stocks = {}
        for row in cursor.fetchall():
            self.stocks[row[0]] = row[1]
        
        cursor.close()
        logger.info(f"Loaded {len(self.stocks)} stocks")
    
    def get_company_cik(self, symbol: str) -> Optional[str]:
        """完善获取 CIK 的逻辑，处理 BRK.B 等特殊符号"""
        try:
            # 1. 处理特殊符号：SEC 使用连字符
            search_symbol = symbol.replace('.', '-').upper()
            
            # 2. 调用 SEC 官方映射文件
            url = "https://www.sec.gov/files/company_tickers.json"
            headers = {'User-Agent': self.USER_AGENT}
            response = requests.get(url, headers=headers, timeout=10)
            data = response.json()
            
            # 3. 遍历查找匹配的 CIK
            for entry in data.values():
                if entry['ticker'] == search_symbol:
                    # SEC CIK 必须是10位数字（前面补0）
                    return str(entry['cik_str']).zfill(10)
            
            return None
        except Exception as e:
            logger.error(f"Failed to get CIK for {symbol}: {e}")
            return None
    
    def fetch_recent_filings(self, symbol: str, cik: str, filing_type: str = '10-Q') -> List[Dict]:
        """
        Get recent financial report files
        
        Args:
            symbol: Stock symbol
            cik: CIK number
            filing_type: Report type ('10-Q' quarterly, '10-K' annual)
        
        Returns:
            List of report info
        """
        try:
            # SEC的submissions API
            url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            headers = {'User-Agent': self.USER_AGENT}
            
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            filings = data.get('filings', {}).get('recent', {})
            
            if not filings:
                logger.warning(f"No filings found: {symbol}")
                return []
            
            # 提取指定类型的财报
            results = []
            forms = filings.get('form', [])
            filing_dates = filings.get('filingDate', [])
            accession_numbers = filings.get('accessionNumber', [])
            primary_documents = filings.get('primaryDocument', [])
            
            for i, form in enumerate(forms):
                if form == filing_type:
                    # 只获取最近2个财报
                    if len(results) >= 2:
                        break
                    
                    filing_date = filing_dates[i]
                    accession = accession_numbers[i].replace('-', '')
                    primary_doc = primary_documents[i]
                    
                    # 构建文档URL
                    doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{primary_doc}"
                    
                    # 解析财年和季度
                    filing_datetime = datetime.strptime(filing_date, '%Y-%m-%d')
                    fiscal_year = filing_datetime.year
                    fiscal_quarter = ((filing_datetime.month - 1) // 3) + 1 if filing_type == '10-Q' else None
                    
                    results.append({
                        'filing_date': filing_date,
                        'report_type': filing_type,
                        'fiscal_year': fiscal_year,
                        'fiscal_quarter': fiscal_quarter,
                        'document_url': doc_url,
                        'accession_number': accession
                    })
            
            logger.info(f"Found {len(results)} {filing_type} filings: {symbol}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to get filings list {symbol}: {e}")
            return []
    
    def check_if_exists(self, symbol: str, fiscal_year: int, fiscal_quarter: Optional[int]) -> bool:
        """Check if report already downloaded"""
        cursor = self.db_conn.cursor()
        cursor.execute("""
            SELECT 1 FROM financial_reports 
            WHERE symbol = %s 
              AND fiscal_year = %s 
              AND (fiscal_quarter = %s OR (fiscal_quarter IS NULL AND %s IS NULL))
        """, (symbol, fiscal_year, fiscal_quarter, fiscal_quarter))
        
        exists = cursor.fetchone() is not None
        cursor.close()
        return exists
    
    def download_file(self, url: str, local_path: str) -> bool:
        """Download file (supports PDF and HTML)"""
        try:
            headers = {'User-Agent': self.USER_AGENT}
            response = requests.get(url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            file_size = os.path.getsize(local_path)
            logger.info(f"File downloaded successfully: {local_path} ({file_size} bytes)")
            return True
            
        except Exception as e:
            logger.error(f"File download failed: {e}")
            return False
    
    def upload_to_s3(self, local_path: str, s3_key: str) -> bool:
        """Upload file to S3"""
        try:
            self.s3_client.upload_file(
                local_path,
                self.config['s3_rss_bucket'],
                s3_key
            )
            logger.info(f"Uploaded to S3 successfully: {s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload to S3: {e}")
            return False
    
    def save_report_metadata(self, report_info: Dict) -> Optional[int]:
        """Save report metadata to database"""
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO financial_reports 
                (symbol, report_type, fiscal_year, fiscal_quarter, filing_date,
                 pdf_s3_key, txt_s3_key, extraction_status, file_size_bytes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, report_type, fiscal_year, fiscal_quarter) 
                DO NOTHING
                RETURNING id
            """, (
                report_info['symbol'],
                report_info['report_type'],
                report_info['fiscal_year'],
                report_info.get('fiscal_quarter'),
                report_info['filing_date'],
                report_info['pdf_s3_key'],
                report_info.get('txt_s3_key'),
                'pending',
                report_info.get('file_size_bytes', 0)
            ))
            
            result = cursor.fetchone()
            self.db_conn.commit()
            
            if result:
                logger.info(f"Report metadata saved successfully: {report_info['symbol']} {report_info['report_type']}")
                return result[0]
            else:
                logger.debug(f"Report already exists: {report_info['symbol']} {report_info['report_type']}")
            
        except Exception as e:
            logger.error(f"Failed to save metadata: {e}")
            self.db_conn.rollback()
        finally:
            cursor.close()
        
        return None
    
    def collect_all(self):
        """Collect the latest financial reports for all stocks"""
        logger.info("=" * 50)
        logger.info("Starting financial report collection task")
        
        stats = {'checked': 0, 'downloaded': 0, 'skipped': 0, 'failed': 0}
        
        for symbol in self.stocks.keys():
            stats['checked'] += 1
            
            try:
                # Get CIK
                cik = self.get_company_cik(symbol)
                if not cik:
                    logger.warning(f"Skipped {symbol}: Unable to get CIK")
                    stats['failed'] += 1
                    continue
                
                # Check quarterly reports (10-Q)
                quarterly_filings = self.fetch_recent_filings(symbol, cik, '10-Q')
                
                # Check annual reports (10-K)
                annual_filings = self.fetch_recent_filings(symbol, cik, '10-K')
                
                all_filings = quarterly_filings + annual_filings
                
                for filing in all_filings:
                    # Check if already exists
                    if self.check_if_exists(
                        symbol,
                        filing['fiscal_year'],
                        filing.get('fiscal_quarter')
                    ):
                        stats['skipped'] += 1
                        logger.info(f"Already exists, skipped: {symbol} {filing['report_type']} {filing['fiscal_year']}")
                        continue
                    
                    # Download file
                    file_ext = 'html'  # SEC files are usually HTML
                    local_file = f"/tmp/{symbol}_{filing['report_type']}_{filing['fiscal_year']}.{file_ext}"
                    
                    if not self.download_file(filing['document_url'], local_file):
                        stats['failed'] += 1
                        continue
                    
                    # Upload to S3
                    quarter_str = f"-Q{filing['fiscal_quarter']}" if filing.get('fiscal_quarter') else ""
                    s3_key = f"financial-reports/pdf/{symbol}/{filing['fiscal_year']}{quarter_str}-{filing['report_type']}.{file_ext}"
                    
                    if not self.upload_to_s3(local_file, s3_key):
                        stats['failed'] += 1
                        continue
                    
                    # Get file size
                    file_size = os.path.getsize(local_file)
                    
                    # Clean up temp file
                    try:
                        os.remove(local_file)
                    except:
                        pass
                    
                    # Save metadata
                    report_info = {
                        'symbol': symbol,
                        'report_type': filing['report_type'],
                        'fiscal_year': filing['fiscal_year'],
                        'fiscal_quarter': filing.get('fiscal_quarter'),
                        'filing_date': filing['filing_date'],
                        'pdf_s3_key': s3_key,
                        'txt_s3_key': s3_key.replace('/pdf/', '/txt/').replace(f'.{file_ext}', '.txt'),
                        'file_size_bytes': file_size
                    }
                    
                    if self.save_report_metadata(report_info):
                        stats['downloaded'] += 1
                
                # SEC API rate limit (10 requests/second), add delay
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Processing failed {symbol}: {e}")
                stats['failed'] += 1
        
        logger.info(f"Collection completed - Checked:{stats['checked']} Downloaded:{stats['downloaded']} Skipped:{stats['skipped']} Failed:{stats['failed']}")
        logger.info("=" * 50)
    
    def close(self):
        """Close connections"""
        if hasattr(self, 'db_conn'):
            self.db_conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    collector = FinancialReportCollector()
    try:
        collector.collect_all()
    finally:
        collector.close()