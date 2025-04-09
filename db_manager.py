import sqlite3
import pandas as pd
import datetime
import os
import boto3
from botocore.exceptions import ClientError

class S3BBCDatabaseManager:
    """
    An extension of BBCDatabaseManager that syncs with S3
    """
    
    def __init__(self, bucket_name, db_name='bbc_articles.db', local_db_path=None):
        """
        Initialize the database connection and set up S3 syncing
        
        Args:
            bucket_name (str): Name of the S3 bucket
            db_name (str): Name of the database file in S3
            local_db_path (str): Path to store the local copy (default: same as db_name)
        """
        self.bucket_name = bucket_name
        self.db_name = db_name
        self.local_db_path = local_db_path or db_name
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3')
        
        # First, try to download the database from S3
        self.download_from_s3()
        
        # Initialize the database connection
        self.conn = self.setup_database()
    
    def setup_database(self):
        """
        Create the database and tables if they don't exist
        """
        conn = sqlite3.connect(self.local_db_path)
        cursor = conn.cursor()
        
        # Create the articles table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL,
                body TEXT,
                is_live BOOLEAN NOT NULL,
                date_scraped TIMESTAMP NOT NULL
            )
        ''')
        
        conn.commit()
        return conn
    
    def download_from_s3(self):
        """
        Download the database from S3 if it exists
        """
        try:
            print(f"Attempting to download database from S3 bucket '{self.bucket_name}'...")
            self.s3_client.download_file(
                self.bucket_name, 
                self.db_name, 
                self.local_db_path
            )
            print(f"Successfully downloaded database from S3 to {self.local_db_path}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"Database file '{self.db_name}' not found in S3 bucket. A new one will be created.")
            else:
                print(f"Error downloading database from S3: {e}")
            return False
    
    def upload_to_s3(self):
        """
        Upload the database to S3
        """
        try:
            print(f"Uploading database to S3 bucket '{self.bucket_name}'...")
            self.s3_client.upload_file(
                self.local_db_path,
                self.bucket_name,
                self.db_name
            )
            print(f"Successfully uploaded database to S3")
            return True
        except ClientError as e:
            print(f"Error uploading database to S3: {e}")
            return False
    
    def close_connection(self):
        """
        Close the database connection and upload to S3
        """
        if self.conn:
            self.conn.close()
            # Upload to S3 when closing
            self.upload_to_s3()
    
    # The following methods are the same as your original BBCDatabaseManager
    
    def article_exists(self, url):
        """
        Check if an article with the given URL already exists in the database
        """
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM articles WHERE url = ?", (url,))
        count = cursor.fetchone()[0]
        return count > 0
    
    def save_article(self, article_data):
        """
        Save an article to the database
        Returns True if the article was saved, False if it already existed
        """
        # Check if we already have this article
        if self.article_exists(article_data['url']):
            return False
        
        cursor = self.conn.cursor()
        
        # Insert the new article
        cursor.execute('''
            INSERT INTO articles (title, url, body, is_live, date_scraped)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            article_data['title'],
            article_data['url'],
            article_data['body'],
            article_data.get('live', False),
            datetime.datetime.now()
        ))
        
        self.conn.commit()
        return True
    
    def get_recent_articles(self, limit=10):
        """
        Get the most recently added articles
        """
        query = f"""
            SELECT title, url, body, is_live, date_scraped 
            FROM articles 
            ORDER BY date_scraped DESC 
            LIMIT {limit}
        """
        return pd.read_sql_query(query, self.conn)
    
    def get_total_count(self):
        """
        Get the total number of articles in the database
        """
        query = "SELECT COUNT(*) as count FROM articles"
        result = pd.read_sql_query(query, self.conn)
        return result['count'].iloc[0]
    
    def search_articles(self, keyword):
        """
        Search for articles containing a keyword in title or body
        """
        query = f"""
            SELECT title, url, body, is_live, date_scraped 
            FROM articles 
            WHERE title LIKE '%{keyword}%' OR body LIKE '%{keyword}%'
            ORDER BY date_scraped DESC
        """
        return pd.read_sql_query(query, self.conn)
    
    def get_all_articles(self):
        """
        Get all articles in the database
        """
        query = """
            SELECT title, url, body, is_live, date_scraped 
            FROM articles 
            ORDER BY date_scraped DESC
        """
        return pd.read_sql_query(query, self.conn)
    
    def delete_article(self, url):
        """
        Delete an article from the database
        """
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM articles WHERE url = ?", (url,))
        self.conn.commit()
        return cursor.rowcount > 0  # Return True if any rows were deleted
