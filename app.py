import os
import boto3
import sqlite3
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from dotenv import load_dotenv
import tempfile
from datetime import datetime

# Load environment variables
load_dotenv()

# AWS S3 Configuration
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
DB_NAME = 'bbc_articles.db'

# Create a temporary directory to store the downloaded database
TEMP_DIR = tempfile.gettempdir()
LOCAL_DB_PATH = os.path.join(TEMP_DIR, DB_NAME)

app = FastAPI(
    title="BBC News API",
    description="API for accessing the latest BBC news articles",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Initialize S3 client
s3_client = boto3.client('s3')

class Article(BaseModel):
    id: int
    title: str
    url: str
    body: str
    is_live: bool
    date_scraped: str

class ArticleList(BaseModel):
    articles: List[Article]
    count: int
    page: int
    total_pages: int
    total_count: int

def download_db():
    """Download the database from S3"""
    try:
        # Create the temp directory if it doesn't exist
        os.makedirs(TEMP_DIR, exist_ok=True)
        
        # Download the database file from S3
        s3_client.download_file(S3_BUCKET_NAME, DB_NAME, LOCAL_DB_PATH)
        return True
    except Exception as e:
        print(f"Error downloading database from S3: {e}")
        return False

def db_connection():
    """Create a connection to the SQLite database"""
    # Ensure we have the latest database
    if not os.path.exists(LOCAL_DB_PATH) or (datetime.now().timestamp() - os.path.getmtime(LOCAL_DB_PATH)) > 3600:
        # If file doesn't exist or is older than 1 hour, download from S3
        if not download_db():
            raise HTTPException(status_code=500, detail="Could not access the news database")
    
    try:
        # Connect to the database
        conn = sqlite3.connect(LOCAL_DB_PATH)
        conn.row_factory = sqlite3.Row  # This enables column access by name
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

@app.get("/", tags=["Status"])
async def root():
    """API root - provides basic information"""
    return {
        "name": "BBC News API", 
        "description": "Access to the latest BBC news articles",
        "status": "operational",
        "documentation": "/docs"
    }

@app.get("/health", tags=["Status"])
async def health_check():
    """Health check endpoint"""
    try:
        # Try to connect to the database
        conn = db_connection()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

@app.get("/articles", response_model=ArticleList, tags=["Articles"])
async def get_articles(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Number of articles per page"),
    keyword: Optional[str] = Query(None, description="Search by keyword in title or body")
):
    """
    Get a list of BBC news articles.
    
    - **page**: Page number (starting from 1)
    - **limit**: Number of articles per page (1-100)
    - **keyword**: Optional keyword to search in title or body
    """
    try:
        conn = db_connection()
        cursor = conn.cursor()
        
        # Base query
        query = "SELECT * FROM articles"
        count_query = "SELECT COUNT(*) as count FROM articles"
        
        # Add search condition if keyword is provided
        params = ()
        if keyword:
            query += " WHERE title LIKE ? OR body LIKE ?"
            count_query += " WHERE title LIKE ? OR body LIKE ?"
            keyword_param = f"%{keyword}%"
            params = (keyword_param, keyword_param)
        
        # Add sorting and pagination
        query += " ORDER BY date_scraped DESC LIMIT ? OFFSET ?"
        offset = (page - 1) * limit
        params = params + (limit, offset)
        
        # Execute the queries
        cursor.execute(query, params)
        articles = [dict(row) for row in cursor.fetchall()]
        
        # Get total count
        if keyword:
            cursor.execute(count_query, (f"%{keyword}%", f"%{keyword}%"))
        else:
            cursor.execute(count_query)
        total_count = cursor.fetchone()["count"]
        
        # Calculate total pages
        total_pages = (total_count + limit - 1) // limit  # Ceiling division
        
        conn.close()
        
        return {
            "articles": articles,
            "count": len(articles),
            "page": page,
            "total_pages": total_pages,
            "total_count": total_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving articles: {str(e)}")

@app.get("/articles/{article_id}", response_model=Article, tags=["Articles"])
async def get_article(article_id: int):
    """
    Get a specific BBC news article by ID
    
    - **article_id**: The numeric ID of the article
    """
    try:
        conn = db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM articles WHERE id = ?", (article_id,))
        article = cursor.fetchone()
        
        conn.close()
        
        if article is None:
            raise HTTPException(status_code=404, detail=f"Article with ID {article_id} not found")
        
        return dict(article)
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving article: {str(e)}")

@app.get("/latest", response_model=List[Article], tags=["Articles"])
async def get_latest_articles(limit: int = Query(5, ge=1, le=20, description="Number of latest articles")):
    """
    Get the latest BBC news articles
    
    - **limit**: Number of articles to return (1-20)
    """
    try:
        conn = db_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT * FROM articles ORDER BY date_scraped DESC LIMIT ?", 
            (limit,)
        )
        articles = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        return articles
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving latest articles: {str(e)}")

if __name__ == "__main__":
    # Run the server
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
