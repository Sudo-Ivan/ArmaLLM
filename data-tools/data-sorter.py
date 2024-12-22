import json
from pathlib import Path
import pandas as pd
from rich.console import Console
from rich.progress import track
from rich.traceback import install
import logging
from datetime import datetime

install()
console = Console()

class DataSorter:
    def __init__(self):
        self.setup_logging()
        self.raw_dir = Path("dataset/raw")
        self.processed_dir = Path("dataset/processed")
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
    def setup_logging(self):
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        self.logger = logging.getLogger("DataSorter")
        self.logger.setLevel(logging.INFO)
        
        file_handler = logging.FileHandler(
            log_dir / f"sorter_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        self.logger.addHandler(file_handler)
    
    def process_wiki_data(self):
        try:
            input_file = self.raw_dir / "raw_wiki_data.json"
            if not input_file.exists():
                raise FileNotFoundError(f"Input file not found: {input_file}")
            
            with open(input_file, "r") as f:
                data = json.load(f)
            
            self.logger.info(f"Processing {len(data)} entries")
            console.print(f"[cyan]Processing {len(data)} entries...[/]")
            
            processed_data = []
            for entry in track(data, description="Processing entries"):
                try:
                    content = entry['content'].strip()
                    if not content:
                        self.logger.warning(f"Empty content for entry: {entry['title']}")
                        continue
                        
                    processed_data.append({
                        'instruction': f"Explain the Arma command: {entry['title']}",
                        'input': "",
                        'output': content
                    })
                    
                except KeyError as e:
                    self.logger.error(f"Missing key in entry: {e}")
                    console.print(f"[yellow]Skipping entry due to missing {e}[/]")
                except Exception as e:
                    self.logger.error(f"Error processing entry {entry.get('title', 'unknown')}: {e}")
                    console.print(f"[red]Error processing entry:[/] {str(e)}")
            
            output_file = self.processed_dir / "arma_commands.jsonl"
            with open(output_file, "w") as f:
                for item in processed_data:
                    f.write(json.dumps(item) + "\n")
            
            self.logger.info(f"Successfully processed {len(processed_data)} entries")
            console.print(f"[green]Successfully saved {len(processed_data)} entries to {output_file}[/]")
            
        except Exception as e:
            self.logger.critical(f"Fatal error during processing: {str(e)}")
            console.print_exception()
            raise

if __name__ == "__main__":
    sorter = DataSorter()
    sorter.process_wiki_data() 