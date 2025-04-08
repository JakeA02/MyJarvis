# query_articles.py
from db_manager import BBCDatabaseManager
import sys

def print_help():
    print("\nBBC Articles Database Query Tool")
    print("---------------------------------")
    print("Available commands:")
    print("  recent [n]       - Show n most recent articles (default: 5)")
    print("  count            - Show total count of articles")
    print("  search <keyword> - Search for articles containing keyword")
    print("  all              - Show all articles (can be a lot)")
    print("  help             - Show this help message")
    print("  exit             - Exit the program")

def main():
    db_manager = BBCDatabaseManager()
    
    if len(sys.argv) > 1:
        # Command line mode
        command = sys.argv[1]
        
        if command == "recent":
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            articles = db_manager.get_recent_articles(limit)
            print_articles(articles)
            
        elif command == "count":
            count = db_manager.get_total_count()
            print(f"Total articles in database: {count}")
            
        elif command == "search" and len(sys.argv) > 2:
            keyword = sys.argv[2]
            articles = db_manager.search_articles(keyword)
            print(f"Found {len(articles)} articles matching '{keyword}':")
            print_articles(articles)
            
        elif command == "all":
            articles = db_manager.get_all_articles()
            print(f"All {len(articles)} articles in database:")
            print_articles(articles)
            
        else:
            print_help()
    else:
        # Interactive mode
        print_help()
        while True:
            command = input("\nEnter command (or 'help' for commands): ").strip()
            
            if command == "exit":
                break
                
            elif command == "help":
                print_help()
                
            elif command.startswith("recent"):
                parts = command.split()
                limit = int(parts[1]) if len(parts) > 1 else 5
                articles = db_manager.get_recent_articles(limit)
                print_articles(articles)
                
            elif command == "count":
                count = db_manager.get_total_count()
                print(f"Total articles in database: {count}")
                
            elif command.startswith("search "):
                keyword = command[7:]
                articles = db_manager.search_articles(keyword)
                print(f"Found {len(articles)} articles matching '{keyword}':")
                print_articles(articles)
                
            elif command == "all":
                articles = db_manager.get_all_articles()
                print(f"All {len(articles)} articles in database:")
                print_articles(articles)
                
            else:
                print("Unknown command. Type 'help' for available commands.")
    
    db_manager.close_connection()

def print_articles(articles):
    if len(articles) == 0:
        print("No articles found.")
        return
        
    for i, row in articles.iterrows():
        print(f"\n--- Article {i+1} ---")
        print(f"Title: {row['title']}")
        print(f"URL: {row['url']}")
        print(f"Date scraped: {row['date_scraped']}")
        print(f"Body preview: {row['body'][:100]}..." if len(row['body']) > 100 else row['body'])

if __name__ == "__main__":
    main()
