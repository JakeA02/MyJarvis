import sqlite3
import pandas as pd
import datetime

class BBCDatabaseManager:
    """
    A class to manage database operations for BBC articles
    """
    
    def __init__(self, db_name='bbc_articles.db'):
        """
        Initialize the database connection and set up the tables
        """
        self.db_name = db_name
        self.conn = self.setup_database()
    
    def setup_database(self):
        """
        Create the database and tables if they don't exist
        """
        conn = sqlite3.connect(self.db_name)
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
    
    def close_connection(self):
        """
        Close the database connection
        """
        if self.conn:
            self.conn.close()
    
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
