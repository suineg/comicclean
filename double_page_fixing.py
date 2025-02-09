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
        # e.g., "1213" in "023-1213.jpg"
        self.double_number_pattern = re.compile(r'[-+&](\d{2})(\d{2})\.(?i:jpe?g|png)', re.IGNORECASE)
        # Pattern to match two 3-digit numbers separated by +/&/-
        # e.g., "033-034" in "GL54-033-034.jpg"
        self.connected_number_pattern = re.compile(r'[-+&](\d{3})[-+&](\d{3})\.(?i:jpe?g|png)', re.IGNORECASE)
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
        Find instances of four numbers together or numbers separated by +/& in a filename
        Returns tuple of (prefix, first_number, second_number) if found, None otherwise
        Example: "GL54-033-034.jpg" -> ("GL54", "033", "034")
                "GL57-020+021.jpg" -> ("GL57", "020", "021")
                "GL51-006-007.jpg" -> ("GL51", "006", "007")
        """
        # Try to match three-digit numbers separated by +/&/-
        match = self.connected_number_pattern.search(filename)
        if match:
            # Get the full string up to the first number
            start = match.start()
            prefix = filename[:start]
            # Extract the numbers
            first_num = match.group(1)
            second_num = match.group(2)
            return (prefix, first_num, second_num)
        
        # Then try to match four digits together
        match = self.double_number_pattern.search(filename)
        if match:
            start = match.start()
            prefix = filename[:start]
            first_num = match.group(1)
            second_num = match.group(2)
            return (prefix, first_num, second_num)
        
        return None

    def suggest_new_name(self, filename: str, number_match: Tuple[str, str, str]) -> str:
        """
        Generate suggested new filename with underscores everywhere except between split numbers
        Example: "GL54-033-034.jpg" -> "GL54_033-034.jpg"
                "GL57-020+021.JPG" -> "GL57_020-021.JPG"
                "Green Lantern 031-0809.JPG" -> "Green Lantern 031_08-09.JPG"
        """
        prefix, first_num, second_num = number_match
        # Find the extension with original case
        ext_match = re.search(r'\.(?i:jpe?g|png)$', filename)
        original_ext = ext_match.group(0) if ext_match else ''
        # Convert all hyphens to underscores in the prefix
        modified_prefix = prefix.replace('-', '_')
        # Return with underscore before the split numbers, but hyphen between them
        return f"{modified_prefix}_{first_num}-{second_num}{original_ext}"

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

    def process_archive(self, archive_path: Path, auto_mode: bool = False, dry_run: bool = False, changes_log: List[str] = None) -> None:
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
            
            # Track files with suggested changes
            suggested_changes = {}
            
            if dry_run:
                print("\n" + "=" * 50)
                print(f"Archive: {archive_path}")
                print("=" * 50)
                print("The following changes would be made:")
                print("-" * 50)
            elif not auto_mode:
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
            
            for num, filename in files_dict.items():
                number_match = self.find_double_numbers(filename)
                if number_match:
                    new_name = self.suggest_new_name(filename, number_match)
                    suggested_changes[filename] = new_name
                    if dry_run:
                        change_msg = f"Would rename: {filename}\n        to: {new_name}"
                        print(change_msg)
                        if changes_log is not None:
                            changes_log.append(change_msg)
                    elif not auto_mode:
                        print(f"{num:3d}. {filename} -> {new_name} (suggested)")
                elif not auto_mode and not dry_run:
                    print(f"{num:3d}. {filename}")
            
            if dry_run:
                if not suggested_changes:
                    print("No changes would be made to this archive.")
                print("-" * 50)
                return
            elif not auto_mode:
                print("-" * 50)
            
            # Process user commands or auto-accept changes
            files_to_rename = {}
            files_to_delete = set()
            
            if auto_mode:
                if suggested_changes:
                    files_to_rename.update(suggested_changes)
                    logging.info('Auto-accepted %d suggested changes for %s', 
                               len(suggested_changes), archive_path.name)
            else:
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
            
            # Capture changes for this archive
            if changes_log:
                changes_log.append(f"\n{archive_path}:")
                for filename, new_name in files_to_rename.items():
                    changes_log.append(f"{filename} -> {new_name}")
                for filename in files_to_delete:
                    changes_log.append(f"Marked for deletion: {filename}")
            
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
    
    # Check for flags
    auto_mode = '-a' in sys.argv
    dry_run = '-d' in sys.argv
    recursive = '-r' in sys.argv
    output_file = None
    
    # Remove flags from args
    if auto_mode:
        sys.argv.remove('-a')
    if dry_run:
        sys.argv.remove('-d')
    if recursive:
        sys.argv.remove('-r')
    
    # Check for output file
    if '-o' in sys.argv:
        output_idx = sys.argv.index('-o')
        if output_idx + 1 >= len(sys.argv):
            logging.error('No output file specified after -o')
            return
        output_file = Path(sys.argv[output_idx + 1])
        # Remove -o and its argument
        sys.argv.pop(output_idx)  # Remove -o
        sys.argv.pop(output_idx)  # Remove file path
    
    if auto_mode and dry_run:
        logging.error('Cannot use both auto mode (-a) and dry run (-d) at the same time')
        return
    
    changes_log = []  # Store all changes for output file
    
    def process_directory(directory: Path):
        """Helper function to process a directory"""
        if recursive:
            # Use rglob for recursive search
            archive_files = list(directory.rglob('*.cbz')) + list(directory.rglob('*.cbr'))
        else:
            # Use glob for single directory
            archive_files = list(directory.glob('*.cbz')) + list(directory.glob('*.cbr'))
        
        if not archive_files:
            logging.error('No CBZ or CBR files found in directory: %s', directory)
            return
        
        logging.info('Found %d archive files in %s', len(archive_files), directory)
        
        # Process each archive
        for archive_path in sorted(archive_files):
            try:
                if not dry_run:
                    print(f"\nProcessing: {archive_path}")
                
                # Capture changes for this archive
                if output_file:
                    changes_log.append(f"\n{archive_path}:")
                
                handler.process_archive(archive_path, auto_mode, dry_run, changes_log)
            except Exception as e:
                logging.error('Failed to process %s: %s', archive_path, e)
                continue
    
    # Get path from command line or use current directory
    if len(sys.argv) > 1:
        path = Path(sys.argv[1])
        if not path.exists():
            logging.error('Path not found: %s', path)
            return
        
        if path.is_file():
            # Process single file
            if not handler.is_supported(path):
                logging.error('Unsupported file format: %s', path)
                return
            
            try:
                if output_file:
                    changes_log.append(f"\n{path}:")
                handler.process_archive(path, auto_mode, dry_run, changes_log)
            except Exception as e:
                logging.error('Failed to process archive: %s', e)
                sys.exit(1)
        
        elif path.is_dir():
            # Process directory
            process_directory(path)
        
        else:
            logging.error('Path is neither a file nor a directory: %s', path)
            return
            
    else:
        # No argument provided, use current directory
        process_directory(Path.cwd())
    
    # Write changes to output file if specified
    if output_file and changes_log:
        try:
            with open(output_file, 'w') as f:
                f.write('\n'.join(changes_log))
            logging.info('Changes written to: %s', output_file)
        except Exception as e:
            logging.error('Failed to write changes to output file: %s', e)

if __name__ == "__main__":
    main()

