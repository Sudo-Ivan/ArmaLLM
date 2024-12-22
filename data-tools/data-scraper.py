from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import json
import yaml
from pathlib import Path
import time
from urllib.parse import urljoin, urlparse
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.traceback import install
import logging
from datetime import datetime

install()  # Install rich traceback handler
console = Console()

class ArmaScraper:
    def __init__(self):
        self.setup_logging()
        self.data_dir = Path("dataset/raw")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.data_dir / "raw_wiki_data.json"
        self.visited_urls = set()
        self.load_config()
        # Initialize empty JSON file
        with open(self.output_file, "w") as f:
            f.write("[\n")
        
    def setup_logging(self):
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        self.logger = logging.getLogger("ArmaScraper")
        self.logger.setLevel(logging.INFO)
        
        file_handler = logging.FileHandler(
            log_dir / f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        self.logger.addHandler(file_handler)
    
    def load_config(self):
        try:
            with open("data-tools/scraper.yml", "r") as f:
                self.config = yaml.safe_load(f)
            self.logger.info("Config loaded successfully")
        except Exception as e:
            self.logger.error(f"Failed to load config: {e}")
            console.print("[red bold]Failed to load config file[/]")
            raise
    
    def is_valid_url(self, url):
        parsed = urlparse(url)
        return bool(parsed.netloc) and any(base in url for base in self.config['base_urls'])
    
    def append_data(self, data_item):
        """Append a single data item to the JSON file"""
        with open(self.output_file, "a") as f:
            json.dump(data_item, f, indent=2)
            f.write(",\n")  # Add comma for valid JSON array
    
    def finalize_json(self):
        """Finalize the JSON file by removing the last comma and closing the array"""
        with open(self.output_file, "rb+") as f:
            f.seek(-2, 2)  # Go to 2nd last character
            f.truncate()   # Remove last comma and newline
            f.write(b"\n]")  # Close the array
    
    def scrape_page(self, page, url, depth=0, task_id=None, progress=None):
        if depth > self.config['max_depth'] or url in self.visited_urls:
            return 0  # Return count instead of data
        
        self.visited_urls.add(url)
        self.logger.info(f"Scraping: {url} (depth: {depth})")
        if progress and task_id:
            progress.update(task_id, description=f"Scraping: {url}")
        
        try:
            console.print(f"[yellow]Attempting to load:[/] {url}")
            response = page.goto(url, timeout=30000, wait_until="networkidle")
            
            if not response.ok:
                console.print(f"[red]Failed to load {url} - Status: {response.status}[/]")
                return 0
            
            time.sleep(self.config['rate_limit'])
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            count = 0
            
            # Handle category pages differently
            if "Category:" in url:
                console.print("[cyan]Processing category page...[/]")
                category_links = soup.find_all('div', {'class': 'mw-category-group'})
                for group in category_links:
                    links = group.find_all('a')
                    for link in links:
                        href = link.get('href')
                        if href:
                            full_url = urljoin(url, href)
                            if self.is_valid_url(full_url) and full_url not in self.visited_urls:
                                console.print(f"[blue]Found command link:[/] {full_url}")
                                count += self.scrape_page(page, full_url, depth + 1, task_id, progress)
            else:
                # Handle regular command pages
                content_div = soup.find('div', {'class': 'mw-parser-output'})
                if content_div:
                    title = soup.title.text if soup.title else url
                    console.print(f"[green]Found content for:[/] {title}")
                    data_item = {
                        'title': title,
                        'url': url,
                        'content': content_div.text.strip()
                    }
                    self.append_data(data_item)
                    count += 1
                    if progress and task_id:
                        progress.advance(task_id)
            
            return count
            
        except Exception as e:
            self.logger.error(f"Error scraping {url}: {str(e)}")
            console.print(f"[red]Error scraping {url}:[/] {str(e)}")
            return 0

    def scrape_wiki(self):
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                task_id = progress.add_task("Starting scraper...", total=None)
                
                with sync_playwright() as p:
                    browser = p.chromium.launch(
                        headless=True,
                        timeout=60000,
                    )
                    
                    context = browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                    )
                    
                    page = context.new_page()
                    page.set_default_timeout(30000)
                    
                    console.print("[cyan]Browser launched, starting scraping...[/]")
                    
                    total_count = 0
                    for url in self.config['start_urls']:
                        console.print(f"\n[yellow]Processing start URL:[/] {url}")
                        count = self.scrape_page(page, url, task_id=task_id, progress=progress)
                        total_count += count
                        console.print(f"[cyan]Found {count} entries from {url}[/]")
                    
                    browser.close()
                    
                    # Finalize JSON file
                    self.finalize_json()
                    
                progress.update(task_id, description="[green]Scraping completed!")
                console.print(f"\n[green]Successfully saved {total_count} entries to {self.output_file}[/]")
                self.logger.info(f"Scraping completed. Processed {total_count} pages")
                
        except Exception as e:
            self.logger.critical(f"Fatal error during scraping: {str(e)}")
            console.print_exception()
            raise

if __name__ == "__main__":
    scraper = ArmaScraper()
    scraper.scrape_wiki() 