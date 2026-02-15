#!/usr/bin/env python3

"""

Removes files within /tophat_data/ older than two days.

"""


import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

# How many series of fetched_records or new_records to maintain, and where?
DEFAULT_KEEP_COUNT = 2
OUTPUT_DIR = "tophat_data"

logger = logging.getLogger(__name__)







class FileCleanup:
    
    def __init__(self, output_dir: str = OUTPUT_DIR, keep_count: int = DEFAULT_KEEP_COUNT):
        self.output_dir = Path(output_dir)
        self.keep_count = keep_count



    def get_timestamped_files(self, pattern: str) -> List[Tuple[Path, datetime]]:

        files_with_timestamps = []
        
        if not self.output_dir.exists():
            return files_with_timestamps
        
        for filepath in self.output_dir.glob(pattern):





            # Extract timestamp from filename


            try:
                filename = filepath.stem

                parts = filename.split('_')
                if len(parts) >= 3:
                    date_part = parts[-2]  
                    time_part = parts[-1]
                    timestamp_str = f"{date_part}_{time_part}"
                    timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                    files_with_timestamps.append((filepath, timestamp))
            except (ValueError, IndexError) as e:
                logger.warning(f"Could not parse timestamp from {filepath.name}: {e}")
                continue


        files_with_timestamps.sort(key=lambda x: x[1], reverse=True)
        
        return files_with_timestamps
    
    def cleanup_old_files(self, pattern: str, dry_run: bool = False) -> Tuple[int, int]:




        """
        Remove files

        """



        files_with_timestamps = self.get_timestamped_files(pattern)
        
        if not files_with_timestamps:
            logger.info(f"No files found matching {pattern}")
            return (0, 0)
        
        total_files = len(files_with_timestamps)
        files_to_keep = files_with_timestamps[:self.keep_count]
        files_to_delete = files_with_timestamps[self.keep_count:]

        logger.info(f"Found {total_files} files matching {pattern}")
        logger.info(f"Keeping {len(files_to_keep)} most recent files")
        logger.info(f"Will delete {len(files_to_delete)} old files")






        deleted_count = 0
        for filepath, timestamp in files_to_delete:
            if dry_run:
                logger.info(f"[DRY RUN] Would delete: {filepath.name} ({timestamp})")
            else:
                try:
                    filepath.unlink()
                    logger.info(f"Deleted: {filepath.name} ({timestamp})")
                    deleted_count += 1
                except Exception as e:
                    logger.error(f"Error deleting {filepath.name}: {e}")
        
        return (len(files_to_keep), deleted_count if not dry_run else len(files_to_delete))
    
    def cleanup_all(self, dry_run: bool = False) -> dict:




        results = {}
        
        patterns = {
            'fetched_csv': 'fetched_records_*.csv',
            'fetched_json': 'fetched_records_*.json',
            'new_csv': 'new_records_*.csv',
            'new_json': 'new_records_*.json',
        }
        
        for file_type, pattern in patterns.items():
            logger.info(f"\nCleaning up {file_type} files...")
            kept, deleted = self.cleanup_old_files(pattern, dry_run=dry_run)
            results[file_type] = {'kept': kept, 'deleted': deleted}
        
        return results





def main():
    parser = argparse.ArgumentParser(
        description='Delete files'
    )
    parser.add_argument(
        '--output-dir',
        default=OUTPUT_DIR,
        help=f'Directory with data files (default: {OUTPUT_DIR})'
    )
    parser.add_argument(
        '--keep',
        type=int,
        default=DEFAULT_KEEP_COUNT,
        help=f'Number of most recent files to keep (default: {DEFAULT_KEEP_COUNT})'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )
    parser.add_argument(
        '--pattern',
        help='Clean files matching pattern -- "fetched_records_*.csv"'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    args = parser.parse_args()
    





    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Create cleanup manager
    cleanup = FileCleanup(
        output_dir=args.output_dir,
        keep_count=args.keep
    )
    



    if args.pattern:
        kept, deleted = cleanup.cleanup_old_files(args.pattern, dry_run=args.dry_run)
        print(f"\n{'='*60}")
        print(f"SUMMARY for {args.pattern}")
        print(f"{'='*60}")
        print(f"Files kept: {kept}")
        print(f"Files deleted: {deleted}")
        print(f"{'='*60}\n")
    else:
        results = cleanup.cleanup_all(dry_run=args.dry_run)
        
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        total_kept = sum(r['kept'] for r in results.values())
        total_deleted = sum(r['deleted'] for r in results.values())
        
        for file_type, stats in results.items():
            print(f"{file_type:20s} - Kept: {stats['kept']:3d}, Deleted: {stats['deleted']:3d}")
        
        print(f"{'-'*60}")
        print(f"{'TOTAL':20s} - Kept: {total_kept:3d}, Deleted: {total_deleted:3d}")
        print(f"{'='*60}\n")
    
    if args.dry_run:
        print("DRY RUN. No files deleted.")
        print("Run without --dry-run to perform actual cleanup.\n")
    
    return 0






if __name__ == '__main__':
    sys.exit(main())
