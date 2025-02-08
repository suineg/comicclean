import json                        # For JSON parsing
import os                          # For file and directory operations
import pathlib                     # For path manipulations
from typing import List, Dict      # For type hints
import difflib                     # For file comparison
import shutil                      # For file operations like delete
from datetime import datetime      # For timestamp operations
import logging                     # For logging operations
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import concurrent.futures
import threading
import sys

# Set up basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class FileEntry:
    def __init__(self, name: str, size: int, tth: str):
        self.name = name
        self.size = size
        self.tth = tth

def parse_json_entry(entry_dict: dict) -> FileEntry:
    """
    Parse a single JSON file entry and return a FileEntry object
    
    Example entry:
    {
        "Name": "Champions 001 (2019) (Digital) (Zone-Empire).cbr",
        "Size": "36904422",
        "TTH": "RVPDAATGGUMOTJWDJCF7VTIA3UNTJA42YIUQW5Y"
    }
    """
    name = entry_dict['Name']
    size = int(entry_dict['Size'])
    tth = entry_dict['TTH']
    
    return FileEntry(name, size, tth)

def load_json_file(json_path: str) -> List[FileEntry]:
    """Load and parse a JSON file containing file entries"""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            entries = []
            
            # Assuming the JSON structure has a root list of file entries
            for entry in data['files']:  # Adjust this based on your JSON structure
                entries.append(parse_json_entry(entry))
            
            return entries
    except json.JSONDecodeError as e:
        logging.error('Failed to parse JSON file %s: %s', json_path, str(e))
        raise
    except FileNotFoundError:
        logging.error('JSON file not found: %s', json_path)
        raise

def compare_json_files(mine_path: str, first_path: str, second_path: str, third_path: str) -> Dict[str, List[FileEntry]]:
    """
    Compare four JSON files and categorize entries based on TTH matches
    """
    # Load all JSON files
    mine_entries = {entry.tth: entry for entry in load_json_file(mine_path)}
    first_entries = {entry.tth: entry for entry in load_json_file(first_path)}
    second_entries = {entry.tth: entry for entry in load_json_file(second_path)}
    third_entries = {entry.tth: entry for entry in load_json_file(third_path)}
    
    result = {
        'unique_to_mine': [],
        'in_first': [],
        'in_second': [],
        'in_third': [],
        'in_all': []
    }
    
    # Compare entries
    for tth, entry in mine_entries.items():
        in_first = tth in first_entries
        in_second = tth in second_entries
        in_third = tth in third_entries
        
        if in_first and in_second and in_third:
            result['in_all'].append(entry)
        elif in_first:
            result['in_first'].append(entry)
        elif in_second:
            result['in_second'].append(entry)
        elif in_third:
            result['in_third'].append(entry)
        else:
            result['unique_to_mine'].append(entry)
    
    logging.info('Found %d unique entries', len(result['unique_to_mine']))
    logging.info('Found %d matches in first file', len(result['in_first']))
    logging.info('Found %d matches in second file', len(result['in_second']))
    logging.info('Found %d matches in third file', len(result['in_third']))
    logging.info('Found %d matches in all files', len(result['in_all']))
    
    return result

def clean_filename(filename: str) -> str:
    """
    Clean filename by:
    1. Removing any existing escapes
    2. Escaping special characters for Linux, avoiding double escapes
    
    Args:
        filename: Original filename from XML
    Returns:
        Cleaned filename safe for Linux systems
    """
    # First remove any existing escapes
    cleaned = filename.replace('\\', '')
    
    # Escape special characters
    special_chars = ' []()!&;\'"`<>?|'
    for char in special_chars:
        cleaned = cleaned.replace(char, '\\' + char)
    
    return cleaned

ROOT_FOLDERS = [
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 01 (1939-1949)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 02 (1950-1959)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 03 (1960-1969)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 04 (1970-1979)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 05 (1980-1989)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 06 (1990-1999)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 07 (2000-2009)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 08 (2010)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 09 (2011)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 10 (2012)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 11 (2013)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 12 (2014)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 13 (2015)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 14 (2016)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 15 (2017)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 16 (2018)/",
    "/library/torrent/comics/Complete Marvel Comics Chronology Part 17 (2019)/",
    "/library/misc/Unsorted/DC++ Mirror/",
    "/library/misc/Unsorted/X-Men/",
    "/library/misc/Unsorted/X-Men Sub/",
    "/library/misc/Unsorted/Spider-Man Collection/"
]

def find_file_path_in_root(filename: str, root: str) -> str | None:
    """
    Search for a file in a single root directory
    """
    # Pre-clean the filename for comparison
    clean_name = filename.replace('\\', '')
    search_root = root.replace('\\', '')
    
    try:
        for dirpath, _, filenames in os.walk(search_root):
            if clean_name in filenames:
                full_path = os.path.join(dirpath, filename)
                return clean_filename(full_path)
    except PermissionError:
        logging.warning(f"Permission denied accessing directory: {search_root}")
    except OSError as e:
        logging.warning(f"Error accessing directory {search_root}: {str(e)}")
    
    return None

def find_file_path(filename: str) -> str | None:
    """
    Search through root folders to find the complete path for a file using multiple threads
    """
    with ThreadPoolExecutor(max_workers=min(len(ROOT_FOLDERS), 8)) as executor:  # Back to 8 threads
        # Create a partial function with the filename
        search_func = partial(find_file_path_in_root, filename)
        
        # Map the search function across all root folders
        results = executor.map(search_func, ROOT_FOLDERS)
        
        # Return the first non-None result
        for result in results:
            if result:
                return result
    
    return None

def write_unique_files_to_delete(unique_entries: List[FileEntry], output_path: str = "todelete.txt"):
    """
    Write the complete paths of unique files to todelete.txt using multiple threads
    """
    try:
        found_count = 0
        processed_count = 0
        total_entries = len(unique_entries)
        last_progress = 0
        stop_event = threading.Event()
        
        def check_quit():
            print("\nPress Enter to quit gracefully...")
            while not stop_event.is_set():
                if input() != None:
                    logging.info('Quit requested. Finishing current operations...')
                    stop_event.set()
                    return
        
        # Start the quit checker in a separate thread
        quit_thread = threading.Thread(target=check_quit)
        quit_thread.daemon = True
        quit_thread.start()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            with ThreadPoolExecutor(max_workers=16) as executor:  # Increased to 16 threads
                future_to_entry = {
                    executor.submit(find_file_path, entry.name): entry 
                    for entry in unique_entries
                }
                
                for future in concurrent.futures.as_completed(future_to_entry):
                    if stop_event.is_set():
                        logging.info('Gracefully stopping...')
                        break
                        
                    processed_count += 1
                    full_path = future.result()
                    if full_path:
                        f.write(f"{full_path}\n")
                        found_count += 1
                    
                    # Show progress for both processed and found files
                    if processed_count % 100 == 0 or found_count % 100 == 0:
                        if processed_count != last_progress:
                            logging.info('Processed %d out of %d entries. Found %d matching files so far...',
                                       processed_count, total_entries, found_count)
                            last_progress = processed_count
        
        logging.info('Successfully wrote %d file paths out of %d processed entries to %s',
                    found_count, total_entries, output_path)
            
    except Exception as e:
        logging.error('Error writing to %s: %s', output_path, str(e))
        raise

def main():
    """Main execution function"""
    mine_path = "mine.json"
    first_path = "bigfirst.json"
    second_path = "bigsecond.json"
    third_path = "bigthird.json"
    
    # Validate input files exist
    for file_path in [mine_path, first_path, second_path, third_path]:
        if not os.path.exists(file_path):
            logging.error('Required input file not found: %s', file_path)
            raise FileNotFoundError(f"Missing required file: {file_path}")
    
    try:
        comparison = compare_json_files(mine_path, first_path, second_path, third_path)
        write_unique_files_to_delete(comparison['unique_to_mine'])
        
    except Exception as e:
        logging.error('Error during execution: %s', str(e))
        raise

if __name__ == "__main__":
    main()
