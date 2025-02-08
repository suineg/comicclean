#!/usr/bin/env python3
"""
Archive Manipulator
Handles ZIP and RAR archives, allowing file renaming and deletion within archives
Requires Python 3.11 or higher
"""

import zipfile
import rarfile
import logging
import os
from pathlib import Path
from typing import Dict, List, Union, Tuple, Optional
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
import threading
from datetime import datetime
import sys
import re

# Verify Python version
if sys.version_info < (3, 11):
    sys.exit("Python 3.11 or higher is required to run this script.")

# Configure rarfile to use the correct unrar executable
rarfile.UNRAR_TOOL = "unrar"  # Adjust this path as needed

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class ArchiveHandler:
    def __init__(self):
        self.supported_formats = {'.zip', '.cbz', '.rar', '.cbr'}
        self.temp_dir = Path(tempfile.mkdtemp())
        # Pattern to match four numbers together after a hyphen, plus, or ampersand
        # e.g., "1213" in "023-1213.jpg" or "12+13" or "12&13"
        self.double_number_pattern = re.compile(r'[-+&](\d{2})(\d{2})\.')
        self.connected_number_pattern = re.compile(r'[-+&](\d{2})[-+&](\d{2})\.')
        logging.info('Initialized temporary directory at: %s', self.temp_dir)

    def __del__(self):
        """Cleanup temporary directory on object destruction"""
        try:
            shutil.rmtree(self.temp_dir)
            logging.info('Cleaned up temporary directory')
        except Exception as e:
            logging.error('Failed to cleanup temporary directory: %s', e)

    def _create_temp_dir(self, archive_path: Path) -> Path:
        """Create a unique temporary directory for this archive"""
        # Create unique directory name using timestamp and archive name
        unique_dir = self.temp_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{archive_path.stem}"
        unique_dir.mkdir(parents=True, exist_ok=True)
        logging.info('Created temporary directory for %s at: %s', archive_path.name, unique_dir)
        return unique_dir

    def _cleanup_temp_dir(self, temp_dir: Path) -> None:
        """Clean up a specific temporary directory"""
        try:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
                logging.info('Cleaned up temporary directory: %s', temp_dir)
        except Exception as e:
            logging.error('Failed to cleanup temporary directory %s: %s', temp_dir, e)

    def is_supported(self, file_path: Path) -> bool:
        """Check if the file format is supported"""
        return file_path.suffix.lower() in self.supported_formats

    def find_double_numbers(self, filename: str) -> Optional[Tuple[str, str, str]]:
        """
        Find instances of four numbers together or two numbers separated by +/& in a filename
        Returns tuple of (prefix, first_number, second_number) if found, None otherwise
        Example: "Green Lantern 023-1213.JPG" -> ("Green Lantern 023-", "12", "13")
                "Green Lantern 023-12+13.JPG" -> ("Green Lantern 023-", "12", "13")
                "Green Lantern 023-12&13.JPG" -> ("Green Lantern 023-", "12", "13")
        """
        # First try to match four digits together
        match = self.double_number_pattern.search(filename)
        if match:
            start = match.start(1)
            prefix = filename[:start-1]  # -1 to include the separator
            first_num = match.group(1)
            second_num = match.group(2)
            return (prefix, first_num, second_num)
        
        # Then try to match numbers separated by +/&
        match = self.connected_number_pattern.search(filename)
        if match:
            start = match.start(1)
            prefix = filename[:start-1]  # -1 to include the separator
            first_num = match.group(1)
            second_num = match.group(2)
            return (prefix, first_num, second_num)
        
        return None

    def suggest_new_name(self, filename: str, number_match: Tuple[str, str, str]) -> str:
        """
        Generate suggested new filename with underscores everywhere except between split numbers
        Example: "Green-Lantern-023-1213.jpg" -> "Green_Lantern_023_12-13.jpg"
                "Green-Lantern-023-12+13.jpg" -> "Green_Lantern_023_12-13.jpg"
                "Green-Lantern-023-12&13.jpg" -> "Green_Lantern_023_12-13.jpg"
        """
        prefix, first_num, second_num = number_match
        suffix = filename[len(prefix) + 1 + len(first_num) + len(second_num):]  # +1 for the separator
        # Convert all hyphens to underscores in the prefix
        modified_prefix = prefix.replace('-', '_')
        # Return with underscore before the split numbers, but hyphen between them
        return f"{modified_prefix}_{first_num}-{second_num}{suffix}"

    def list_archive_contents(self, archive_path: Path) -> Dict[int, str]:
        """
        List contents of an archive with numbered entries
        Returns a dictionary mapping numbers to filenames
        """
        try:
            files_dict = {}
            index = 1
            
            if archive_path.suffix.lower() in {'.zip', '.cbz'}:
                with zipfile.ZipFile(archive_path, 'r') as archive:
                    # Sort filenames to ensure consistent ordering
                    filenames = sorted(archive.namelist())
                    for filename in filenames:
                        if not filename.endswith('/'): # Skip directories
                            files_dict[index] = filename
                            index += 1
                            
            elif archive_path.suffix.lower() in {'.rar', '.cbr'}:
                with rarfile.RarFile(archive_path, 'r') as archive:
                    filenames = sorted(archive.namelist())
                    for filename in filenames:
                        if not archive.getinfo(filename).isdir():
                            files_dict[index] = filename
                            index += 1
            
            return files_dict
            
        except Exception as e:
            logging.error('Failed to list contents of %s: %s', archive_path, e)
            raise

    def process_archive(self, archive_path: Path) -> None:
        """Process a single archive file"""
        if not self.is_supported(archive_path):
            logging.warning('Unsupported file format: %s', archive_path)
            return

        logging.info('Processing archive: %s', archive_path)
        try:
            # List contents with numbers
            files_dict = self.list_archive_contents(archive_path)
            
            if not files_dict:
                logging.warning('No files found in archive: %s', archive_path)
                return
            
            # Display contents and instructions
            print("\n" + "=" * 50)
            print(f"Archive: {archive_path.name}")
            print("=" * 50)
            print("Commands: <number> M to modify, <number> D to delete")
            print("Example: '17 D' to delete file 17, '20 M' to modify file 20")
            print("Type 'A' to accept all suggested changes")
            print("Press Enter without input to finish current archive")
            print("Type 'X' to exit program completely")
            print("-" * 50)
            
            # Track files with suggested changes
            suggested_changes = {}
            for num, filename in files_dict.items():
                number_match = self.find_double_numbers(filename)
                if number_match:
                    new_name = self.suggest_new_name(filename, number_match)
                    suggested_changes[filename] = new_name
                    print(f"{num:3d}. {filename} -> {new_name} (suggested)")
                else:
                    print(f"{num:3d}. {filename}")
            print("-" * 50)
            
            # Process user commands
            files_to_rename = {}
            files_to_delete = set()
            
            while True:
                command = input("\nEnter command (or press Enter to finish): ").strip().upper()
                if not command:
                    break
                
                if command == 'X':
                    logging.info('User requested exit')
                    print("Exiting program...")
                    sys.exit(0)
                
                if command == 'A':
                    if suggested_changes:
                        files_to_rename.update(suggested_changes)
                        print(f"Added {len(suggested_changes)} suggested changes")
                        continue
                    else:
                        print("No suggested changes available")
                        continue
                
                # Parse command
                parts = command.split()
                if len(parts) != 2 or parts[1] not in {'M', 'D'}:
                    print("Invalid command. Use format: '<number> M' or '<number> D'")
                    continue
                
                try:
                    file_num = int(parts[0])
                    action = parts[1]
                    
                    if file_num not in files_dict:
                        print("Invalid file number. Please try again.")
                        continue
                    
                    filename = files_dict[file_num]
                    
                    if action == 'D':
                        # Mark file for deletion
                        files_to_delete.add(filename)
                        print(f"Marked for deletion: {filename}")
                        
                    else:  # action == 'M'
                        number_match = self.find_double_numbers(filename)
                        if number_match:
                            suggested_name = self.suggest_new_name(filename, number_match)
                            print(f"Suggested new name: {suggested_name}")
                            use_suggested = input("Use suggested name? (y/n): ").lower()
                            
                            if use_suggested == 'y':
                                files_to_rename[filename] = suggested_name
                            else:
                                new_name = input("Enter new name: ")
                                if new_name:
                                    files_to_rename[filename] = new_name
                        else:
                            new_name = input("Enter new name: ")
                            if new_name:
                                files_to_rename[filename] = new_name
                
                except ValueError:
                    print("Please enter a valid number.")
            
            # If any changes were requested, process the archive
            if files_to_rename or files_to_delete:
                if archive_path.suffix.lower() in {'.zip', '.cbz'}:
                    self._process_zip(archive_path, files_to_rename, files_to_delete)
                else:
                    self._process_rar(archive_path, files_to_rename, files_to_delete)
            else:
                logging.info('No changes requested for %s', archive_path)
            
        except Exception as e:
            logging.error('Failed to process archive %s: %s', archive_path, e)
            raise

    def _process_zip(self, archive_path: Path, files_to_rename: Dict[str, str], files_to_delete: set) -> None:
        """Process a ZIP archive"""
        archive_temp_dir = None
        try:
            with zipfile.ZipFile(archive_path, 'r') as zip_ref:
                # Create unique temporary directory for this archive
                archive_temp_dir = self._create_temp_dir(archive_path)
                
                # Extract files (except those marked for deletion)
                for filename in zip_ref.namelist():
                    if filename not in files_to_delete:
                        extract_path = archive_temp_dir / filename
                        # Create parent directories if they don't exist
                        extract_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        # Skip directories, only extract files
                        if not filename.endswith('/'):
                            with zip_ref.open(filename) as source, open(extract_path, 'wb') as target:
                                shutil.copyfileobj(source, target)
                
                # Create new archive and replace the original
                with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as new_zip:
                    for root, _, files in os.walk(archive_temp_dir):
                        for file in files:
                            file_path = Path(root) / file
                            arcname = file_path.relative_to(archive_temp_dir)
                            
                            if str(arcname) in files_to_rename:
                                arcname = files_to_rename[str(arcname)]
                            elif files_to_rename:
                                arcname = str(arcname).replace('-', '_')
                            
                            new_zip.write(file_path, str(arcname))
                
                logging.info('Successfully updated archive: %s', archive_path)
                
        except Exception as e:
            logging.error('Failed to process ZIP archive %s: %s', archive_path, e)
            raise
        finally:
            # Cleanup temporary directory for this archive
            if archive_temp_dir:
                self._cleanup_temp_dir(archive_temp_dir)

    def _process_rar(self, archive_path: Path, files_to_rename: Dict[str, str], files_to_delete: set) -> None:
        """Process a RAR archive"""
        archive_temp_dir = None
        try:
            with rarfile.RarFile(archive_path, 'r') as rar_ref:
                # Create unique temporary directory for this archive
                archive_temp_dir = self._create_temp_dir(archive_path)
                
                # Extract files (except those marked for deletion)
                for filename in rar_ref.namelist():
                    if filename not in files_to_delete:
                        extract_path = archive_temp_dir / filename
                        # Create parent directories if they don't exist
                        extract_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        # Skip directories, only extract files
                        if not rar_ref.getinfo(filename).isdir():
                            rar_ref.extract(filename, str(archive_temp_dir))
                
                # Create new CBZ archive and replace the original RAR
                new_path = archive_path.with_suffix('.cbz')
                with zipfile.ZipFile(new_path, 'w', zipfile.ZIP_DEFLATED) as new_zip:
                    for root, _, files in os.walk(archive_temp_dir):
                        for file in files:
                            file_path = Path(root) / file
                            arcname = file_path.relative_to(archive_temp_dir)
                            
                            if str(arcname) in files_to_rename:
                                arcname = files_to_rename[str(arcname)]
                            elif files_to_rename:
                                arcname = str(arcname).replace('-', '_')
                            
                            new_zip.write(file_path, str(arcname))
                
                # Remove the original RAR file after successful conversion
                archive_path.unlink()
                logging.info('Successfully converted and updated archive: %s', new_path)
                
        except rarfile.BadRarFile as e:
            logging.error('Failed to process RAR archive %s (bad RAR file): %s', archive_path, e)
            raise
        except Exception as e:
            logging.error('Failed to process RAR archive %s: %s', archive_path, e)
            raise
        finally:
            # Cleanup temporary directory for this archive
            if archive_temp_dir:
                self._cleanup_temp_dir(archive_temp_dir)

def main():
    """Main execution function"""
    handler = ArchiveHandler()
    
    # Get all CBZ and CBR files in current directory
    current_dir = Path.cwd()
    archive_files = list(current_dir.glob('*.cbz')) + list(current_dir.glob('*.cbr'))
    
    if not archive_files:
        logging.error('No CBZ or CBR files found in current directory')
        return
    
    logging.info('Found %d archive files', len(archive_files))
    
    # Process each archive
    for archive_path in sorted(archive_files):
        try:
            print(f"\nProcessing: {archive_path}")
            handler.process_archive(archive_path)
        except Exception as e:
            logging.error('Failed to process %s: %s', archive_path, e)
            continue

if __name__ == "__main__":
    main()
