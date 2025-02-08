import xml.etree.ElementTree as ET
import json
import logging
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def convert_xml_to_json(xml_path: str, json_path: str):
    """
    Convert XML file to JSON format
    
    Args:
        xml_path: Path to source XML file
        json_path: Path to output JSON file
    """
    try:
        logging.info('Converting %s to JSON...', xml_path)
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Create JSON structure
        json_data = {
            "files": []
        }
        
        # Convert each File element
        file_count = 0
        for file_elem in root.findall('.//File'):
            file_data = {
                "Name": file_elem.get('Name'),
                "Size": file_elem.get('Size'),
                "TTH": file_elem.get('TTH')
            }
            json_data["files"].append(file_data)
            file_count += 1
            
            # Log progress for large files
            if file_count % 10000 == 0:
                logging.info('Processed %d files...', file_count)
        
        # Write JSON file
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f)
        
        logging.info('Successfully converted %s to %s (%d files)', 
                    xml_path, json_path, file_count)
        
    except ET.ParseError as e:
        logging.error('Failed to parse XML file %s: %s', xml_path, str(e))
        raise
    except Exception as e:
        logging.error('Error converting %s to JSON: %s', xml_path, str(e))
        raise

def main():
    """Convert all XML files to JSON"""
    files_to_convert = [
        ("mine.xml", "mine.json"),
        ("bigfirst.xml", "bigfirst.json"),
        ("bigsecond.xml", "bigsecond.json"),
        ("bigthird.xml", "bigthird.json")
    ]
    
    for xml_file, json_file in files_to_convert:
        if not os.path.exists(xml_file):
            logging.error('XML file not found: %s', xml_file)
            continue
            
        convert_xml_to_json(xml_file, json_file)

if __name__ == "__main__":
    main() 
