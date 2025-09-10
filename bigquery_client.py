"""
BigQuery Client Module
Handles DataFrame to BigQuery integration for domain analysis results
"""

import os
import logging
from typing import Optional
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
                bigquery.SchemaField("website_summary", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("confidence_score", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("selection_reasoning", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("completed_timestamp", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("processing_time_seconds", "FLOAT64", mode="NULLABLE"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                # New sector classification columns
                bigquery.SchemaField("real_estate", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("infrastructure", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("industrial", "STRING", mode="NULLABLE"),
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
        'website_summary': ['Example company website'],
        'confidence_score': [0.95],
        'selection_reasoning': ['Official company website'],
        'completed_timestamp': [datetime.now()],
        'processing_time_seconds': [120.5],
        'created_at': [datetime.utcnow()],
        'real_estate': ['Commercial'],
        'infrastructure': ["Can't Say"],
        'industrial': ["Can't Say"]
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