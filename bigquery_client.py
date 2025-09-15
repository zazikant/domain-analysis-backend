"""
BigQuery Client Module
Handles DataFrame to BigQuery integration for domain analysis results
"""

import os
import logging
from typing import Optional, List, Dict
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime

logger = logging.getLogger(__name__)


class BigQueryClient:
    """Client for handling BigQuery operations"""
    
    def __init__(self, project_id: str, dataset_id: str = "domain_intelligence", table_id: str = "domain_analysis"):
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
        
        # Ensure dataset and table exist
        self._ensure_dataset_exists()
        self._ensure_table_exists()
        self._ensure_queue_tables_exist()
    
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
        """Create table if it doesn't exist"""
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

    def _ensure_queue_tables_exist(self):
        """Create processing queue tables if they don't exist"""
        self._create_processing_queue_table()
        self._create_batch_tracking_table()
    
    def _create_processing_queue_table(self):
        """Create email processing queue table"""
        queue_table_ref = self.dataset_ref.table("email_processing_queue")
        
        try:
            self.client.get_table(queue_table_ref)
            logger.info("Processing queue table already exists")
        except NotFound:
            schema = [
                bigquery.SchemaField("queue_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("extracted_domain", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),  # pending, processing, completed, failed
                bigquery.SchemaField("priority", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("retry_count", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("started_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("completed_at", "TIMESTAMP", mode="NULLABLE"),
            ]
            
            table = bigquery.Table(queue_table_ref, schema=schema)
            table.description = "Email processing queue for batch operations"
            
            # Partition by created_at for performance
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at"
            )
            
            # Cluster by batch_id and status for common queries
            table.clustering_fields = ["batch_id", "status", "extracted_domain"]
            
            self.client.create_table(table, timeout=30)
            logger.info("Created email processing queue table")
    
    def _create_batch_tracking_table(self):
        """Create batch tracking table"""
        batch_table_ref = self.dataset_ref.table("processing_batches")
        
        try:
            self.client.get_table(batch_table_ref)
            logger.info("Batch tracking table already exists")
        except NotFound:
            schema = [
                bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("user_session_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("original_filename", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("total_emails", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("processed_emails", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("successful_emails", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("failed_emails", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("duplicate_emails", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),  # pending, processing, completed, failed
                bigquery.SchemaField("progress_percentage", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("estimated_completion_time", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("started_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("completed_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("last_updated", "TIMESTAMP", mode="REQUIRED"),
            ]
            
            table = bigquery.Table(batch_table_ref, schema=schema)
            table.description = "Batch processing tracking and progress monitoring"
            
            # Partition by created_at
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at"
            )
            
            # Cluster for performance
            table.clustering_fields = ["user_session_id", "status"]
            
            self.client.create_table(table, timeout=30)
            logger.info("Created batch tracking table")

    def add_emails_to_queue(self, emails: List[str], batch_id: str, priority: int = 5) -> bool:
        """
        Add emails to processing queue
        
        Args:
            emails: List of email addresses
            batch_id: Unique batch identifier
            priority: Processing priority (1=highest, 10=lowest)
            
        Returns:
            bool: True if successful
        """
        try:
            import uuid
            from datetime import datetime
            
            rows_to_insert = []
            for email in emails:
                queue_id = f"queue_{uuid.uuid4().hex[:8]}"
                extracted_domain = email.split('@')[-1] if '@' in email else None
                
                rows_to_insert.append({
                    "queue_id": queue_id,
                    "batch_id": batch_id,
                    "email": email.strip().lower(),
                    "extracted_domain": extracted_domain,
                    "status": "pending",
                    "priority": priority,
                    "retry_count": 0,
                    "error_message": None,
                    "created_at": datetime.utcnow().isoformat(),
                    "started_at": None,
                    "completed_at": None,
                })
            
            queue_table_ref = self.dataset_ref.table("email_processing_queue")
            errors = self.client.insert_rows_json(queue_table_ref, rows_to_insert)
            
            if not errors:
                logger.info(f"Added {len(rows_to_insert)} emails to processing queue for batch {batch_id}")
                return True
            else:
                logger.error(f"Errors adding emails to queue: {errors}")
                return False
                
        except Exception as e:
            logger.error(f"Error adding emails to queue: {str(e)}")
            return False

    def create_batch_record(self, batch_id: str, total_emails: int, user_session_id: str = None, filename: str = None) -> bool:
        """
        Create batch tracking record
        
        Args:
            batch_id: Unique batch identifier
            total_emails: Total number of emails in batch
            user_session_id: User session identifier
            filename: Original CSV filename
            
        Returns:
            bool: True if successful
        """
        try:
            from datetime import datetime
            
            row = {
                "batch_id": batch_id,
                "user_session_id": user_session_id,
                "original_filename": filename,
                "total_emails": total_emails,
                "processed_emails": 0,
                "successful_emails": 0,
                "failed_emails": 0,
                "duplicate_emails": 0,
                "status": "pending",
                "progress_percentage": 0.0,
                "estimated_completion_time": None,
                "created_at": datetime.utcnow().isoformat(),
                "started_at": None,
                "completed_at": None,
                "last_updated": datetime.utcnow().isoformat(),
            }
            
            batch_table_ref = self.dataset_ref.table("processing_batches")
            errors = self.client.insert_rows_json(batch_table_ref, [row])
            
            if not errors:
                logger.info(f"Created batch record for {batch_id} with {total_emails} emails")
                return True
            else:
                logger.error(f"Errors creating batch record: {errors}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating batch record: {str(e)}")
            return False

    def update_batch_progress(self, batch_id: str, processed: int = None, successful: int = None, 
                            failed: int = None, duplicates: int = None, status: str = None) -> bool:
        """
        Update batch processing progress
        
        Args:
            batch_id: Batch identifier
            processed: Number of processed emails
            successful: Number of successful analyses
            failed: Number of failed analyses
            duplicates: Number of duplicate emails found
            status: Current batch status
            
        Returns:
            bool: True if successful
        """
        try:
            from datetime import datetime
            
            # Build update query dynamically
            updates = []
            query_params = [bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)]
            
            if processed is not None:
                updates.append("processed_emails = @processed")
                query_params.append(bigquery.ScalarQueryParameter("processed", "INTEGER", processed))
            
            if successful is not None:
                updates.append("successful_emails = @successful") 
                query_params.append(bigquery.ScalarQueryParameter("successful", "INTEGER", successful))
                
            if failed is not None:
                updates.append("failed_emails = @failed")
                query_params.append(bigquery.ScalarQueryParameter("failed", "INTEGER", failed))
                
            if duplicates is not None:
                updates.append("duplicate_emails = @duplicates")
                query_params.append(bigquery.ScalarQueryParameter("duplicates", "INTEGER", duplicates))
                
            if status is not None:
                updates.append("status = @status")
                query_params.append(bigquery.ScalarQueryParameter("status", "STRING", status))
                
                if status == "processing" and "started_at" not in [u.split(" = ")[0] for u in updates]:
                    updates.append("started_at = @started_at")
                    query_params.append(bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", datetime.utcnow()))
                elif status in ["completed", "failed"]:
                    updates.append("completed_at = @completed_at")
                    query_params.append(bigquery.ScalarQueryParameter("completed_at", "TIMESTAMP", datetime.utcnow()))
            
            # Always update last_updated and calculate progress
            updates.extend([
                "last_updated = @last_updated",
                "progress_percentage = SAFE_DIVIDE(processed_emails, total_emails) * 100"
            ])
            query_params.append(bigquery.ScalarQueryParameter("last_updated", "TIMESTAMP", datetime.utcnow()))
            
            update_query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.processing_batches`
            SET {', '.join(updates)}
            WHERE batch_id = @batch_id
            """
            
            job_config = bigquery.QueryJobConfig(query_parameters=query_params)
            query_job = self.client.query(update_query, job_config=job_config)
            query_job.result()
            
            logger.info(f"Updated batch progress for {batch_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating batch progress: {str(e)}")
            return False

    def get_batch_status(self, batch_id: str) -> Dict:
        """
        Get current batch processing status
        
        Args:
            batch_id: Batch identifier
            
        Returns:
            Dict with batch status information
        """
        try:
            query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.processing_batches`
            WHERE batch_id = @batch_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            for row in results:
                return {
                    "batch_id": row.batch_id,
                    "total_emails": row.total_emails,
                    "processed_emails": row.processed_emails,
                    "successful_emails": row.successful_emails,
                    "failed_emails": row.failed_emails,
                    "duplicate_emails": row.duplicate_emails,
                    "status": row.status,
                    "progress_percentage": row.progress_percentage,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "started_at": row.started_at.isoformat() if row.started_at else None,
                    "completed_at": row.completed_at.isoformat() if row.completed_at else None,
                    "last_updated": row.last_updated.isoformat() if row.last_updated else None,
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting batch status: {str(e)}")
            return None

    def get_pending_queue_items(self, limit: int = 50, priority_order: bool = True) -> List[Dict]:
        """
        Get pending items from processing queue
        
        Args:
            limit: Maximum number of items to return
            priority_order: Whether to order by priority
            
        Returns:
            List of queue items ready for processing
        """
        try:
            order_clause = "ORDER BY priority ASC, created_at ASC" if priority_order else "ORDER BY created_at ASC"
            
            query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.email_processing_queue`
            WHERE status = 'pending'
            {order_clause}
            LIMIT {limit}
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            items = []
            for row in results:
                items.append({
                    "queue_id": row.queue_id,
                    "batch_id": row.batch_id,
                    "email": row.email,
                    "extracted_domain": row.extracted_domain,
                    "priority": row.priority,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                })
            
            return items
            
        except Exception as e:
            logger.error(f"Error getting pending queue items: {str(e)}")
            return []

    def update_queue_item_status(self, queue_id: str, status: str, error_message: str = None) -> bool:
        """
        Update processing queue item status
        
        Args:
            queue_id: Queue item identifier
            status: New status (processing, completed, failed)
            error_message: Error message if failed
            
        Returns:
            bool: True if successful
        """
        try:
            from datetime import datetime
            
            # Build update fields based on status
            updates = ["status = @status"]
            query_params = [
                bigquery.ScalarQueryParameter("queue_id", "STRING", queue_id),
                bigquery.ScalarQueryParameter("status", "STRING", status)
            ]
            
            if status == "processing":
                updates.append("started_at = @started_at")
                query_params.append(bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", datetime.utcnow()))
            elif status in ["completed", "failed"]:
                updates.append("completed_at = @completed_at")
                query_params.append(bigquery.ScalarQueryParameter("completed_at", "TIMESTAMP", datetime.utcnow()))
                
            if error_message:
                updates.append("error_message = @error_message")
                query_params.append(bigquery.ScalarQueryParameter("error_message", "STRING", error_message))
            
            update_query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.email_processing_queue`
            SET {', '.join(updates)}
            WHERE queue_id = @queue_id
            """
            
            job_config = bigquery.QueryJobConfig(query_parameters=query_params)
            query_job = self.client.query(update_query, job_config=job_config)
            query_job.result()
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating queue item status: {str(e)}")
            return False

    def check_multiple_domains_exist(self, domains: List[str], max_age_hours: int = 87600) -> Dict[str, bool]:
        """
        Check if multiple domains exist in the database in batch
        
        Args:
            domains: List of domains to check
            max_age_hours: Maximum age in hours for considering result fresh
            
        Returns:
            Dict mapping domain to boolean (True if exists)
        """
        if not domains:
            return {}
            
        try:
            # Create domain list for SQL IN clause
            domain_params = []
            query_params = []
            
            for i, domain in enumerate(domains):
                param_name = f"domain_{i}"
                domain_params.append(f"@{param_name}")
                query_params.append(bigquery.ScalarQueryParameter(param_name, "STRING", domain))
            
            domains_in_clause = ", ".join(domain_params)
            
            query = f"""
            SELECT extracted_domain, COUNT(*) as count
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE extracted_domain IN ({domains_in_clause})
            AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {max_age_hours} HOUR)
            AND scraping_status NOT IN ('error', 'failed')
            GROUP BY extracted_domain
            """
            
            job_config = bigquery.QueryJobConfig(query_parameters=query_params)
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            
            # Create result dict - all domains default to False
            result_dict = {domain: False for domain in domains}
            
            # Update with existing domains
            for row in results:
                if row.count > 0:
                    result_dict[row.extracted_domain] = True
                    
            return result_dict
            
        except Exception as e:
            logger.error(f"Error checking multiple domains existence: {str(e)}")
            # Return all False on error
            return {domain: False for domain in domains}
    
    def get_recent_results(self, limit: int = 100) -> pd.DataFrame:
        """
        Get recent analysis results
        
        Args:
            limit (int): Maximum number of results to return
            
        Returns:
            pd.DataFrame: Recent analysis results
        """
        try:
            query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            ORDER BY created_at DESC
            LIMIT {limit}
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            df = results.to_dataframe()
            logger.info(f"Retrieved {len(df)} recent results")
            return df
            
        except Exception as e:
            logger.error(f"Error getting recent results: {str(e)}")
            return pd.DataFrame()
    
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


def create_bigquery_client(project_id: str, dataset_id: str = "domain_intelligence", table_id: str = "domain_analysis") -> BigQueryClient:
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


# Example usage for testing
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # Test with sample data
    project_id = "your-project-id"  # Replace with actual project ID
    
    client = create_bigquery_client(project_id)
    
    # Create sample DataFrame
    sample_data = {
        'original_email': ['test@example.com'],
        'extracted_domain': ['example.com'],
        'selected_url': ['https://example.com'],
        'scraping_status': ['success'],
        'company_summary': ['Example company website'],
        'confidence_score': [0.95],
        'selection_reasoning': ['Official company website'],
        'completed_timestamp': [datetime.now()],
        'processing_time_seconds': [120.5],
        'created_at': [datetime.utcnow()],
        'real_estate': ['Commercial'],
        'infrastructure': ["Can't Say"],
        'industrial': ["Can't Say"],
        'company_type': ['Developer'],
        'company_name': ['Example Company Inc'],
        'base_location': ['New York, USA']
    }
    
    df = pd.DataFrame(sample_data)
    
    # Test insertion
    success = client.insert_dataframe(df)
    print(f"Insertion successful: {success}")
    
    # Test querying
    results = client.query_domain_results('example.com')
    print(f"Found {len(results)} results for example.com")
    
    # Test stats
    stats = client.get_analysis_stats()
    print(f"Analysis stats: {stats}")