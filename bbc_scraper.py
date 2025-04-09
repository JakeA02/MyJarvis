import os
import requests
from bs4 import BeautifulSoup
import time
import random
from fake_useragent import UserAgent
from dotenv import load_dotenv
from db_manager import S3BBCDatabaseManager

# Load environment variables from .env file if it exists
load_dotenv()

# AWS S3 Configuration
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
DB_NAME = 'bbc_articles.db'
LOCAL_DB_PATH = 'bbc_articles.db' 

def get_bbc_article_links():
    """
    Scrape the BBC News homepage to get only main featured article links
    """
    # BBC News homepage
    url = "https://www.bbc.com/"
    
    # Use rotating user agents to avoid detection
    ua = UserAgent()
    headers = {
        "User-Agent": ua.random,
        "Accept": "text/html,application/xhtml+xml,application/xml",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/"
    }
    
    # Add random delay
    time.sleep(random.uniform(1, 3))
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            print(f"Failed to fetch BBC homepage. Status code: {response.status_code}")
            return []
        
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Find main featured articles
        articles = []
        
        # Target the main stories section
        main_section = soup.select_one('section[data-testid="vermont-section-outer"]')
        if not main_section:
            print("Main section not found. BBC may have changed their HTML structure.")
            return []
            
        # Process regular articles
        featured_cards = main_section.select('div[data-testid="dundee-card"], div[data-testid="manchester-card"]')
        for card in featured_cards:
            try:
                # Find the link element
                link_element = card.select_one('a[data-testid="internal-link"]')
                if not link_element:
                    continue
                
                # Find the headline
                headline = card.select_one('h2[data-testid="card-headline"]')
                if not headline:
                    continue
                    
                title = headline.text.strip()
                link = link_element["href"]
                
                # Ensure the link is absolute
                if not link.startswith("http"):
                    link = f"https://www.bbc.com{link}"
                
                # Check if it's a live page
                is_live = False
                if "/live/" in link:
                    is_live = True
                # Skip non-article links like video pages
                if "/av/" in link or "/videos/" in link:
                    continue
                
                articles.append({
                    "title": title,
                    "url": link,
                    "live": is_live,
                })
            except Exception as e:
                print(f"Error extracting article link: {e}")
        
        # Process live articles (they have a different structure)
        live_cards = main_section.select('div[data-testid="westminster-card"]')
        for card in live_cards:
            try:
                # Live updates have a different link type
                link_element = card.select_one('a[data-testid="external-anchor"]')
                if not link_element:
                    continue
                
                # Find the headline
                headline = card.select_one('h2[data-testid="card-headline"]')
                if not headline:
                    print("Continuing")
                    continue
                    
                title = headline.text.strip()
                link = link_element["href"]
                
                # Ensure the link is absolute
                if not link.startswith("http"):
                    link = f"https://www.bbc.com{link}"
                
                articles.append({
                    "title": title,
                    "url": link,
                    "live": True,
                })
            except Exception as e:
                print(f"Error extracting live article link: {e}")
        
        # Limit to top stories
        return articles[:10]
    
    except Exception as e:
        print(f"Error fetching BBC page: {e}")
        return []

def get_article_content(article_url, headers):
    """
    Scrape a specific BBC article to get its body content
    """
    # Add random delay between requests (be respectful to the server)
    time.sleep(random.uniform(2, 5))
    
    try:
        response = requests.get(article_url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            print(f"Failed to fetch article: {article_url}. Status code: {response.status_code}")
            return None
        
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Get the article title (h1 as you mentioned)
        title_element = soup.find("h1")
        title = title_element.text.strip() if title_element else ""
        
        # Get the article body (looking for <p> tags within <article>)
        article_element = soup.find("article")
        
        if not article_element:
            print(f"No article element found at: {article_url}")
            return None
        
        # Find all paragraph elements in the article
        paragraphs = article_element.find_all("p")
        
        # Combine paragraphs into a single text
        body_text = " ".join([p.text.strip() for p in paragraphs if p.text.strip()])
        body_text = body_text[:600] #get only the first ~100 words. Angle + a few details
        
        return {
            "title": title,
            "body": body_text,
            "url": article_url
        }
    
    except Exception as e:
        print(f"Error processing article {article_url}: {e}")
        return None
    
def get_live_article_content(article_url, headers):
    """
    Scrape a live BBC article to get its body content
    """
    # Add random delay between requests (be respectful to the server)
    time.sleep(random.uniform(2, 5))
    
    try:
        response = requests.get(article_url, headers=headers, timeout=15)
        if response.status_code != 200:
            print(f"Failed to fetch article: {article_url}. Status code: {response.status_code}")
            return None
        
        soup = BeautifulSoup(response.content, "html.parser")
        
        summary_points = soup.select("#summaryPoints ul[role='list'] > li")
        summary_texts = ""

        for item in summary_points:
            # This gets the text from the paragraph inside each list item
            paragraph = item.find('p')
            if paragraph:
                summary_texts += paragraph.getText() + ". "

        return {
            "body": summary_texts,
            "url": article_url
        }
    
    except Exception as e:
        print(f"Error processing article {article_url}: {e}")
        return None

def scrape_bbc_articles_to_db(db_manager, max_articles=10):
    """
    Main function to scrape BBC articles and save to database
    """
    print("Scraping BBC News articles...")
    
    # Get article links from homepage
    article_links = get_bbc_article_links()
    
    if not article_links:
        print("No article links found.")
        return 0
    
    print(f"Found {len(article_links)} article links.")
    
    # Process each article
    articles_saved = 0
    ua = UserAgent()
    
    # Limit to max_articles
    for i, article_info in enumerate(article_links[:max_articles]):
        print(f"Processing article {i+1}/{min(len(article_links), max_articles)}: {article_info['url']}")
        
        # Skip if the article already exists in the database
        if db_manager.article_exists(article_info['url']):
            print(f"Skipping already saved article: {article_info['url']}")
            continue
        
        # Use a different user agent for each request
        headers = {
            "User-Agent": ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml",
            "Accept-Language": "en-US,en;q=0.9"
        }
        
        if article_info["live"]:
            article_data = get_live_article_content(article_info["url"], headers)
            if article_data:
                article_data["title"] = article_info["title"]
                article_data["live"] = True
        else:
            article_data = get_article_content(article_info["url"], headers)
            if article_data:
                article_data["live"] = False
        
        if article_data and db_manager.save_article(article_data):
            articles_saved += 1
            print(f"Successfully saved to database: {article_data['title']}")
    
    return articles_saved

def main():
    """
    Main function to run the scraper and sync with S3
    """
    # Validate S3 configuration
    if not S3_BUCKET_NAME:
        print("Error: S3_BUCKET_NAME environment variable is not set")
        print("Please set the required environment variables:")
        print("  - S3_BUCKET_NAME: Your S3 bucket name")
        print("  - AWS_ACCESS_KEY_ID: Your AWS access key")
        print("  - AWS_SECRET_ACCESS_KEY: Your AWS secret key")
        return
    
    # Check if AWS credentials are set
    if not (os.environ.get('AWS_ACCESS_KEY_ID') and os.environ.get('AWS_SECRET_ACCESS_KEY')):
        print("Warning: AWS credentials not found in environment variables")
        print("Using IAM role or AWS CLI configuration if available")
    
    print(f"Initializing S3 database manager with bucket: {S3_BUCKET_NAME}")
    
    # Initialize the S3 database manager
    db_manager = S3BBCDatabaseManager(
        bucket_name=S3_BUCKET_NAME,
        db_name=DB_NAME,
        local_db_path=LOCAL_DB_PATH
    )
    
    try:
        # Scrape articles and save to database
        print("Starting BBC news scraping process...")
        articles_saved = scrape_bbc_articles_to_db(db_manager, max_articles=10)
        
        if articles_saved > 0:
            print(f"\nSuccessfully saved {articles_saved} new articles to the database.")
            
            # Display recently added articles
            print("\nRecently added articles:")
            recent_articles = db_manager.get_recent_articles(10)
            
            for i, row in recent_articles.iterrows():
                print(f"\n--- Article {i+1} ---")
                print(f"Title: {row['title']}")
                print(f"URL: {row['url']}")
                print(f"Date scraped: {row['date_scraped']}")
            
            # Count total articles in database
            total_count = db_manager.get_total_count()
            print(f"\nTotal articles in database: {total_count}")
        else:
            print("No new articles were saved to the database.")
    
    finally:
        # Always close the connection and upload to S3
        print("Closing database connection and syncing with S3...")
        db_manager.close_connection()
        print("Database sync complete.")

if __name__ == "__main__":
    main()
