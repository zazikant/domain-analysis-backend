"""
BigQuery Client Module - Updated with Job Processing Tables
Handles DataFrame to BigQuery integration for domain analysis results + job management
"""

import os
import logging
from typing import Optional, List, Dict
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)


class BigQueryClient:
    """Client for handling BigQuery operations"""
    
    def __init__(self, project_id: str, dataset_id: str = "advanced_csv_analysis", table_id: str = "email_domain_results"):
        """
        Initialize BigQuery client
        
        Args:
            project_id (str): Google Cloud project ID
            dataset_id (str): BigQuery dataset ID
            table_id (str): BigQuery table ID
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        
        # Initialize BigQuery client
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = self.client.dataset(dataset_id)
        self.table_ref = self.dataset_ref.table(table_id)
        
        # Table references for new job tables
        self.batch_jobs_table_ref = self.dataset_ref.table("batch_jobs")
        self.email_queue_table_ref = self.dataset_ref.table("email_queue")
        self.job_progress_table_ref = self.dataset_ref.table("job_progress")
        
        # Ensure dataset and tables exist
        self._ensure_dataset_exists()
        self._ensure_table_exists()
        self._ensure_batch_jobs_table_exists()
        self._ensure_email_queue_table_exists()
        self._ensure_job_progress_table_exists()
    
    def _ensure_dataset_exists(self):
        """Create dataset if it doesn't exist"""
        try:
            self.client.get_dataset(self.dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except NotFound:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "US"  # Set location for the dataset
            dataset.description = "Domain intelligence analysis results"
            
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {self.dataset_id}")
    
    def _ensure_table_exists(self):
        """Create main analysis table if it doesn't exist"""
        try:
            self.client.get_table(self.table_ref)
            logger.info(f"Table {self.table_id} already exists")
        except NotFound:
            # Define table schema matching our DataFrame structure
            schema = [
                bigquery.SchemaField("original_email", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("extracted_domain", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("selected_url", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("scraping_status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("company_summary", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("confidence_score", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("selection_reasoning", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("completed_timestamp", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("processing_time_seconds", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                # Sector classification columns
                bigquery.SchemaField("real_estate", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("infrastructure", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("industrial", "STRING", mode="NULLABLE"),
                # New company information columns
                bigquery.SchemaField("company_type", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("company_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("base_location", "STRING", mode="NULLABLE"),
            ]
            
            table = bigquery.Table(self.table_ref, schema=schema)
            table.description = "Domain analysis results from email processing pipeline"
            
            # Add partitioning for better performance
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at"
            )
            
            # Add clustering for common query patterns (BigQuery limit: 4 fields max)
            table.clustering_fields = ["extracted_domain", "scraping_status", "real_estate", "infrastructure"]
            
            table = self.client.create_table(table, timeout=30)
            logger.info(f"Created table {self.table_id}")

    def _ensure_batch_jobs_table_exists(self):
        """Create batch jobs tracking table if it doesn't exist"""
        try:
            self.client.get_table(self.batch_jobs_table_ref)
            logger.info("Table batch_jobs already exists")
        except NotFound:
            schema = [
                bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("session_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("filename", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("total_emails", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("processed_emails", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("successful_emails", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("failed_emails", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("duplicate_emails", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("started_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("completed_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("last_updated", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
            ]
            
            table = bigquery.Table(self.batch_jobs_table_ref, schema=schema)
            table.description = "Batch job tracking for CSV email processing"
            
            # Add partitioning and clustering
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at"
            )
            table.clustering_fields = ["status", "job_id"]
            
            table = self.client.create_table(table, timeout=30)
            logger.info("Created table batch_jobs")

    def _ensure_email_queue_table_exists(self):
        """Create email processing queue table if it doesn't exist"""
        try:
            self.client.get_table(self.email_queue_table_ref)
            logger.info("Table email_queue already exists")
        except NotFound:
            schema = [
                bigquery.SchemaField("queue_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("extracted_domain", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("position", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("started_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("completed_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("retry_count", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
            ]
            
            table = bigquery.Table(self.email_queue_table_ref, schema=schema)
            table.description = "Email processing queue for batch jobs"
            
            # Add partitioning and clustering
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at"
            )
            table.clustering_fields = ["job_id", "status", "extracted_domain"]
            
            table = self.client.create_table(table, timeout=30)
            logger.info("Created table email_queue")

    def _ensure_job_progress_table_exists(self):
        """Create job progress tracking table if it doesn't exist"""
        try:
            self.client.get_table(self.job_progress_table_ref)
            logger.info("Table job_progress already exists")
        except NotFound:
            schema = [
                bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("progress_message", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("progress_percentage", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("current_email", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            ]
            
            table = bigquery.Table(self.job_progress_table_ref, schema=schema)
            table.description = "Real-time progress tracking for batch jobs"
            
            # Add partitioning and clustering
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="timestamp"
            )
            table.clustering_fields = ["job_id"]
            
            table = self.client.create_table(table, timeout=30)
            logger.info("Created table job_progress")

    # NEW METHODS FOR JOB MANAGEMENT

    def create_batch_job(self, session_id: str, emails: List[str], filename: str) -> str:
        """Create a persistent batch job"""
        
        job_id = f"batch_{uuid.uuid4().hex[:12]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create batch job record
        job_data = {
            'job_id': [job_id],
            'session_id': [session_id],
            'filename': [filename],
            'total_emails': [len(emails)],
            'processed_emails': [0],
            'successful_emails': [0],
            'failed_emails': [0],
            'duplicate_emails': [0],
            'status': ['pending'],
            'created_at': [datetime.utcnow()],
            'started_at': [None],
            'completed_at': [None],
            'last_updated': [datetime.utcnow()],
            'error_message': [None]
        }
        
        # Insert job record
        try:
            df = pd.DataFrame(job_data)
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                autodetect=False
            )
            
            job = self.client.load_table_from_dataframe(
                df, self.batch_jobs_table_ref, job_config=job_config
            )
            job.result()
            
            # Store individual emails in processing queue
            email_queue = []
            for i, email in enumerate(emails):
                email_queue.append({
                    'queue_id': str(uuid.uuid4()),
                    'job_id': job_id,
                    'email': email,
                    'extracted_domain': email.split('@')[1] if '@' in email else None,
                    'position': i,
                    'status': 'pending',
                    'started_at': None,
                    'completed_at': None,
                    'created_at': datetime.utcnow(),
                    'retry_count': 0,
                    'error_message': None
                })
            
            queue_df = pd.DataFrame(email_queue)
            queue_job = self.client.load_table_from_dataframe(
                queue_df, self.email_queue_table_ref, job_config=job_config
            )
            queue_job.result()
            
            logger.info(f"Created batch job {job_id} with {len(emails)} emails")
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to create batch job: {e}")
            raise

    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Get current job status"""
        try:
            query = f"""
            SELECT 
                job_id,
                session_id,
                filename,
                total_emails,
                processed_emails,
                successful_emails,
                failed_emails,
                duplicate_emails,
                status,
                created_at,
                started_at,
                completed_at,
                last_updated,
                error_message
            FROM `{self.project_id}.{self.dataset_id}.batch_jobs`
            WHERE job_id = @job_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            for row in results:
                return {
                    "job_id": row.job_id,
                    "session_id": row.session_id,
                    "filename": row.filename,
                    "total_emails": row.total_emails,
                    "processed_emails": row.processed_emails or 0,
                    "successful_emails": row.successful_emails or 0,
                    "failed_emails": row.failed_emails or 0,
                    "duplicate_emails": row.duplicate_emails or 0,
                    "status": row.status,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "started_at": row.started_at.isoformat() if row.started_at else None,
                    "completed_at": row.completed_at.isoformat() if row.completed_at else None,
                    "last_updated": row.last_updated.isoformat() if row.last_updated else None,
                    "error_message": row.error_message
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting job status: {e}")
            return None

    def update_job_status(self, job_id: str, status: str, **kwargs):
        """Update job status and statistics"""
        try:
            # Build UPDATE query dynamically
            update_fields = [f"status = '{status}'", "last_updated = CURRENT_TIMESTAMP()"]
            
            if 'processed_emails' in kwargs:
                update_fields.append(f"processed_emails = {kwargs['processed_emails']}")
            if 'successful_emails' in kwargs:
                update_fields.append(f"successful_emails = {kwargs['successful_emails']}")
            if 'failed_emails' in kwargs:
                update_fields.append(f"failed_emails = {kwargs['failed_emails']}")
            if 'duplicate_emails' in kwargs:
                update_fields.append(f"duplicate_emails = {kwargs['duplicate_emails']}")
            if 'error_message' in kwargs:
                update_fields.append(f"error_message = '{kwargs['error_message']}'")
            
            if status == 'processing' and 'started_at' not in kwargs:
                update_fields.append("started_at = CURRENT_TIMESTAMP()")
            elif status in ['completed', 'failed'] and 'completed_at' not in kwargs:
                update_fields.append("completed_at = CURRENT_TIMESTAMP()")
            
            query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.batch_jobs`
            SET {', '.join(update_fields)}
            WHERE job_id = @job_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()
            
            logger.info(f"Updated job {job_id} status to {status}")
            
        except Exception as e:
            logger.error(f"Error updating job status: {e}")

    def get_pending_emails(self, job_id: str, limit: int = 10) -> List[Dict]:
        """Get pending emails from queue for processing"""
        try:
            query = f"""
            SELECT queue_id, email, extracted_domain, position
            FROM `{self.project_id}.{self.dataset_id}.email_queue`
            WHERE job_id = @job_id 
            AND status = 'pending'
            ORDER BY position
            LIMIT {limit}
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            return [
                {
                    "queue_id": row.queue_id,
                    "email": row.email,
                    "extracted_domain": row.extracted_domain,
                    "position": row.position
                }
                for row in results
            ]
            
        except Exception as e:
            logger.error(f"Error getting pending emails: {e}")
            return []

    def update_email_status(self, queue_id: str, status: str, error_message: str = None):
        """Update individual email processing status"""
        try:
            update_fields = [f"status = '{status}'"]
            
            if status == 'processing':
                update_fields.append("started_at = CURRENT_TIMESTAMP()")
            elif status in ['completed', 'failed']:
                update_fields.append("completed_at = CURRENT_TIMESTAMP()")
            
            if error_message:
                update_fields.append(f"error_message = '{error_message}'")
                update_fields.append("retry_count = retry_count + 1")
            
            query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.email_queue`
            SET {', '.join(update_fields)}
            WHERE queue_id = @queue_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("queue_id", "STRING", queue_id)]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()
            
        except Exception as e:
            logger.error(f"Error updating email status: {e}")

    def log_progress(self, job_id: str, message: str, percentage: float = None, current_email: str = None):
        """Log progress update"""
        try:
            progress_data = {
                'job_id': [job_id],
                'progress_message': [message],
                'progress_percentage': [percentage],
                'current_email': [current_email],
                'timestamp': [datetime.utcnow()]
            }
            
            df = pd.DataFrame(progress_data)
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            
            job = self.client.load_table_from_dataframe(
                df, self.job_progress_table_ref, job_config=job_config
            )
            job.result()
            
        except Exception as e:
            logger.error(f"Error logging progress: {e}")

    # EXISTING METHODS (keep all the original methods)
    
    def insert_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Insert DataFrame into BigQuery table
        
        Args:
            df (pd.DataFrame): DataFrame with analysis results
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert timestamp columns to proper datetime format
            if 'completed_timestamp' in df.columns:
                df['completed_timestamp'] = pd.to_datetime(df['completed_timestamp'], errors='coerce')
            
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
            
            # Configure job to append data
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                autodetect=False,  # Use predefined schema
                ignore_unknown_values=True
            )
            
            # Load DataFrame to BigQuery
            job = self.client.load_table_from_dataframe(
                df, 
                self.table_ref, 
                job_config=job_config
            )
            
            # Wait for the job to complete
            job.result()
            
            logger.info(f"Successfully inserted {len(df)} rows into BigQuery table {self.table_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting DataFrame into BigQuery: {str(e)}")
            return False
    
    def insert_single_result(self, result_dict: dict) -> bool:
        """
        Insert single analysis result into BigQuery
        
        Args:
            result_dict (dict): Single analysis result
            
        Returns:
            bool: True if successful, False otherwise
        """
        df = pd.DataFrame([result_dict])
        return self.insert_dataframe(df)
    
    def query_domain_results(self, domain: str, limit: int = 10) -> pd.DataFrame:
        """
        Query existing results for a specific domain
        
        Args:
            domain (str): Domain to search for
            limit (int): Maximum number of results to return
            
        Returns:
            pd.DataFrame: Existing analysis results
        """
        try:
            query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE extracted_domain = @domain
            ORDER BY created_at DESC
            LIMIT {limit}
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("domain", "STRING", domain)
                ]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            df = results.to_dataframe()
            logger.info(f"Retrieved {len(df)} existing results for domain {domain}")
            return df
            
        except Exception as e:
            logger.error(f"Error querying domain results: {str(e)}")
            return pd.DataFrame()
    
    def check_domain_exists(self, domain: str, max_age_hours: int = 24) -> bool:
        """
        Check if domain analysis exists and is recent
        
        Args:
            domain (str): Domain to check
            max_age_hours (int): Maximum age in hours for considering result fresh
            
        Returns:
            bool: True if recent analysis exists
        """
        try:
            query = f"""
            SELECT COUNT(*) as count
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE extracted_domain = @domain
            AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {max_age_hours} HOUR)
            AND scraping_status NOT IN ('error', 'failed')
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("domain", "STRING", domain)
                ]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            for row in results:
                return row.count > 0
                
            return False
            
        except Exception as e:
            logger.error(f"Error checking domain existence: {str(e)}")
            return False
    
    def check_multiple_emails_exist(self, emails: List[str], max_age_hours: int = 87600) -> Dict[str, bool]:
        """
        Check if multiple email addresses exist in the database in batch
        
        Args:
            emails: List of email addresses to check
            max_age_hours: Maximum age in hours for considering result fresh
            
        Returns:
            Dict mapping email to boolean (True if exists)
        """
        if not emails:
            return {}
            
        try:
            # Create email list for SQL IN clause
            email_params = []
            query_params = []
            
            for i, email in enumerate(emails):
                param_name = f"email_{i}"
                email_params.append(f"@{param_name}")
                query_params.append(bigquery.ScalarQueryParameter(param_name, "STRING", email))
            
            emails_in_clause = ", ".join(email_params)
            
            query = f"""
            SELECT original_email, COUNT(*) as count
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE original_email IN ({emails_in_clause})
            AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {max_age_hours} HOUR)
            AND scraping_status NOT IN ('error', 'failed')
            GROUP BY original_email
            """
            
            job_config = bigquery.QueryJobConfig(query_parameters=query_params)
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            # Create result dict - all emails default to False
            result_dict = {email: False for email in emails}
            
            # Update with existing emails
            for row in results:
                if row.count > 0:
                    result_dict[row.original_email] = True
                    
            return result_dict
            
        except Exception as e:
            logger.error(f"Error checking multiple emails existence: {str(e)}")
            # Return all False on error
            return {email: False for email in emails}

    def check_email_exists(self, email: str, max_age_hours: int = 87600) -> bool:
        """
        Check if a specific email address exists in the database

        Args:
            email: Email address to check
            max_age_hours: Maximum age in hours for considering result fresh

        Returns:
            bool: True if email exists
        """
        result = self.check_multiple_emails_exist([email], max_age_hours)
        return result.get(email, False)

    def get_analysis_stats(self) -> dict:
        """
        Get basic statistics about the analysis results
        
        Returns:
            dict: Statistics about the analysis results
        """
        try:
            query = f"""
            SELECT 
                COUNT(*) as total_analyses,
                COUNT(DISTINCT extracted_domain) as unique_domains,
                AVG(processing_time_seconds) as avg_processing_time,
                AVG(confidence_score) as avg_confidence_score,
                COUNT(*) FILTER (WHERE scraping_status = 'success') as successful_scrapes,
                COUNT(*) FILTER (WHERE scraping_status != 'success') as failed_scrapes
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            for row in results:
                return {
                    'total_analyses': row.total_analyses,
                    'unique_domains': row.unique_domains,
                    'avg_processing_time': float(row.avg_processing_time) if row.avg_processing_time else 0,
                    'avg_confidence_score': float(row.avg_confidence_score) if row.avg_confidence_score else 0,
                    'successful_scrapes': row.successful_scrapes,
                    'failed_scrapes': row.failed_scrapes
                }
                
        except Exception as e:
            logger.error(f"Error getting analysis stats: {str(e)}")
            return {}


def create_bigquery_client(project_id: str, dataset_id: str = "advanced_csv_analysis", table_id: str = "email_domain_results") -> BigQueryClient:
    """
    Factory function to create BigQuery client
    
    Args:
        project_id (str): Google Cloud project ID
        dataset_id (str): BigQuery dataset ID  
        table_id (str): BigQuery table ID
        
    Returns:
        BigQueryClient: Initialized BigQuery client
    """
    return BigQueryClient(project_id, dataset_id, table_id)