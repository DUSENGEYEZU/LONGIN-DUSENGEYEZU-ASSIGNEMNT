#!/usr/bin/env python3
"""
JSONL to CSV Converter
Converts Amazon Fashion JSONL data to CSV format for efficient processing
"""

import json
import pandas as pd
import gzip
import argparse
import logging
from pathlib import Path
from typing import Iterator, Dict, Any
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('conversion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def read_jsonl_file(file_path: str) -> Iterator[Dict[str, Any]]:
    """
    Read JSONL file line by line, handling both regular and gzipped files
    
    Args:
        file_path: Path to the JSONL file
        
    Yields:
        Dict containing parsed JSON data from each line
    """
    file_path = Path(file_path)
    
    # Determine if file is gzipped
    if file_path.suffix == '.gz':
        open_func = gzip.open
        mode = 'rt'
    else:
        open_func = open
        mode = 'r'
    
    logger.info(f"Reading JSONL file: {file_path}")
    
    try:
        with open_func(file_path, mode, encoding='utf-8') as file:
            for line_num, line in enumerate(file, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping malformed JSON at line {line_num}: {e}")
                    continue
                    
                if line_num % 100000 == 0:
                    logger.info(f"Processed {line_num:,} lines")
                    
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        raise

def clean_and_flatten_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean and flatten a JSON record for CSV conversion
    
    Args:
        record: Raw JSON record
        
    Returns:
        Cleaned and flattened record
    """
    # Initialize cleaned record
    cleaned = {}
    
    # Extract basic fields
    basic_fields = [
        'overall', 'verified', 'reviewTime', 'reviewerID', 
        'asin', 'style', 'reviewerName', 'reviewText', 
        'summary', 'unixReviewTime'
    ]
    
    for field in basic_fields:
        cleaned[field] = record.get(field, '')
    
    # Handle nested images data
    images = record.get('images', {})
    if isinstance(images, dict):
        # Extract image URLs if present
        cleaned['images'] = json.dumps(images) if images else ''
    else:
        cleaned['images'] = str(images) if images else ''
    
    # Handle vote data
    vote = record.get('vote', '')
    cleaned['vote'] = str(vote) if vote else ''
    
    # Clean text fields - remove newlines and extra spaces
    text_fields = ['reviewText', 'summary', 'reviewerName']
    for field in text_fields:
        if field in cleaned and cleaned[field]:
            cleaned[field] = str(cleaned[field]).replace('\n', ' ').replace('\r', ' ')
            cleaned[field] = ' '.join(cleaned[field].split())  # Remove extra spaces
    
    # Convert numeric fields
    try:
        cleaned['overall'] = float(cleaned['overall']) if cleaned['overall'] else 0.0
    except (ValueError, TypeError):
        cleaned['overall'] = 0.0
    
    try:
        cleaned['unixReviewTime'] = int(cleaned['unixReviewTime']) if cleaned['unixReviewTime'] else 0
    except (ValueError, TypeError):
        cleaned['unixReviewTime'] = 0
    
    # Convert verified to boolean
    cleaned['verified'] = str(cleaned['verified']).lower() in ['true', '1', 'yes']
    
    return cleaned

def convert_jsonl_to_csv(jsonl_file: str, csv_file: str, chunk_size: int = 100000):
    """
    Convert JSONL file to CSV format in chunks to handle large files
    
    Args:
        jsonl_file: Path to input JSONL file
        csv_file: Path to output CSV file
        chunk_size: Number of records to process in each chunk
    """
    start_time = time.time()
    total_records = 0
    chunk_count = 0
    
    logger.info(f"Starting conversion: {jsonl_file} -> {csv_file}")
    logger.info(f"Chunk size: {chunk_size:,} records")
    
    try:
        # Read JSONL file and convert to CSV in chunks
        chunk_data = []
        
        for record in read_jsonl_file(jsonl_file):
            # Clean and flatten the record
            cleaned_record = clean_and_flatten_record(record)
            chunk_data.append(cleaned_record)
            
            # Process chunk when it reaches the specified size
            if len(chunk_data) >= chunk_size:
                chunk_count += 1
                chunk_start_time = time.time()
                
                # Convert chunk to DataFrame and append to CSV
                df_chunk = pd.DataFrame(chunk_data)
                
                # Write header only for the first chunk
                write_header = chunk_count == 1
                df_chunk.to_csv(
                    csv_file, 
                    mode='a' if chunk_count > 1 else 'w',
                    header=write_header,
                    index=False,
                    encoding='utf-8'
                )
                
                total_records += len(chunk_data)
                chunk_time = time.time() - chunk_start_time
                
                logger.info(f"Chunk {chunk_count}: {len(chunk_data):,} records processed in {chunk_time:.2f}s")
                logger.info(f"Total progress: {total_records:,} records")
                
                # Clear chunk data for next iteration
                chunk_data = []
        
        # Process remaining records in the last chunk
        if chunk_data:
            chunk_count += 1
            df_chunk = pd.DataFrame(chunk_data)
            
            # Write remaining data
            write_header = chunk_count == 1
            df_chunk.to_csv(
                csv_file, 
                mode='a' if chunk_count > 1 else 'w',
                header=write_header,
                index=False,
                encoding='utf-8'
            )
            
            total_records += len(chunk_data)
            logger.info(f"Final chunk: {len(chunk_data):,} records processed")
        
        total_time = time.time() - start_time
        logger.info(f"Conversion completed successfully!")
        logger.info(f"Total records converted: {total_records:,}")
        logger.info(f"Total time: {total_time:.2f} seconds")
        logger.info(f"Average speed: {total_records/total_time:.0f} records/second")
        
        return total_records
        
    except Exception as e:
        logger.error(f"Error during conversion: {e}")
        raise

def main():
    """Main function to handle command line arguments and execute conversion"""
    parser = argparse.ArgumentParser(
        description="Convert Amazon Fashion JSONL data to CSV format"
    )
    parser.add_argument(
        "input_file", 
        help="Path to input JSONL file (supports .jsonl and .jsonl.gz)"
    )
    parser.add_argument(
        "output_file", 
        help="Path to output CSV file"
    )
    parser.add_argument(
        "--chunk-size", 
        type=int, 
        default=100000,
        help="Number of records to process in each chunk (default: 100000)"
    )
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not Path(args.input_file).exists():
        logger.error(f"Input file not found: {args.input_file}")
        return 1
    
    # Create output directory if it doesn't exist
    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Perform conversion
        total_records = convert_jsonl_to_csv(
            args.input_file, 
            args.output_file, 
            args.chunk_size
        )
        
        logger.info(f"Successfully converted {total_records:,} records to {args.output_file}")
        return 0
        
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
