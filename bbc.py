import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from fake_useragent import UserAgent

def get_bbc_article_links():
    """
    Scrape the BBC News homepage to get article links
    """
    # BBC News homepage
    url = "https://www.bbc.com/business"
    
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
        
        # Find all h2 elements that contain article titles
        articles = []
        headline_elements = soup.select("h2")
        for headline in headline_elements:
            try:
                # Find the parent link that contains the URL
                link_element = headline.find_parent("a", href=True)
                
                if not link_element:
                    continue
                
                title = headline.text.strip()
                link = link_element["href"]
                
                # Ensure the link is absolute
                if not link.startswith("http"):
                    link = f"https://www.bbc.com{link}"
                
                # Skip non-article links like video/live pages
                if "/live/" in link:
                    articles.append({
                        "title": title, 
                        "url": link,
                        "live": True,
                    })
                    continue
                if "/av/" in link:
                    continue
    
                articles.append({
                    "title": title,
                    "url": link,
                    "live": False,
                })
            except Exception as e:
                print(f"Error extracting article link: {e}")
        
        return articles
    
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


def scrape_bbc_articles(max_articles=10):
    """
    Main function to scrape BBC articles
    """
    print("Scraping BBC News articles...")
    
    # Get article links from homepage
    article_links = get_bbc_article_links()
    
    if not article_links:
        print("No article links found.")
        return pd.DataFrame()
    
    print(f"Found {len(article_links)} article links.")
    
    # Process each article
    articles = []
    ua = UserAgent()
    
    # Limit to max_articles
    for i, article_info in enumerate(article_links[:max_articles]):
        print(f"Processing article {i+1}/{min(len(article_links), max_articles)}: {article_info['url']}")
        
        # Use a different user agent for each request
        headers = {
            "User-Agent": ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml",
            "Accept-Language": "en-US,en;q=0.9"
        }
        if article_info["live"]:
            article_data = get_live_article_content(article_info["url"], headers)
            article_data["title"] = article_info["title"]
            print("LIVE ARTICLE DATA", article_data)
        else:
            article_data = get_article_content(article_info["url"], headers)
        
        if article_data:
            articles.append(article_data)
            print(f"Successfully scraped: {article_data['title']}")
    
    # Convert to DataFrame
    if articles:
        df = pd.DataFrame(articles)
        return df
    else:
        print("No articles successfully scraped.")
        return pd.DataFrame()

if __name__ == "__main__":
    # Scrape up to 10 articles
    articles_df = scrape_bbc_articles(max_articles=10)
    
    if not articles_df.empty:
        print(f"\nSuccessfully scraped {len(articles_df)} articles.")
        
        # Display article titles
        for i, row in articles_df.iterrows():
            print(f"\n--- Article {i+1} ---")
            print(f"Title: {row['title']}")
            print(f"URL: {row['url']}")
            print(f"Body preview: {row['body'][:100]}...")
        
        # Save to CSV
        articles_df.to_csv("bbc_articles.csv", index=False)
        print("\nArticles saved to bbc_articles.csv")
    else:
        print("No articles were successfully scraped. Please check the selectors.")
