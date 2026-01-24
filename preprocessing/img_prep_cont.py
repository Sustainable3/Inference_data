"""
resumed processing
splits orthophotos into small chunks
equal spatial size
equal px size (scaling)

I 26

from genAI adapted by MD
"""

import os
import re
import time
import logging
from multiprocessing import Pool, cpu_count
from PIL import Image


os.environ['OMP_NUM_THREADS'] = '1'
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'
os.environ['VECLIB_MAXIMUM_THREADS'] = '1'
os.environ['NUMEXPR_NUM_THREADS'] = '1'


INPUT_FOLDER = './imagery125'
OUTPUT_FOLDER = './imagery125_640px'
LOG_FILE_PATH = './slurm-21237230.out'
# OUTPUT_FOLDER = './imagery125_640px_2'
# LOG_FILE_PATH = './slurm-21237263.out'
PREFIXES_SMALL = ('2019', '2020', '2021', '2023') 
SIZE_SMALL = 640
SIZE_LARGE = 3200
TARGET_OUTPUT_SIZE = 640
MAX_WORKERS = max(1, cpu_count() - 8)
QA = 92


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(processName)-15s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH), # Append logs to the file
        logging.StreamHandler()             # Also print to console
    ]
)


def get_split_size(filename):
    if filename.startswith(PREFIXES_SMALL):
        return SIZE_SMALL
    return SIZE_LARGE


def get_completed_files(log_path):
    """
    Parses the log file to find filenames that were successfully processed ([DONE]).
    Returns a set of filenames.
    """
    if not os.path.exists(log_path):
        return set()

    completed_files = set()
    
    # Regex to capture the filename after [DONE]
    # Matches: ... [DONE] filename.tif ...
    regex_pattern = r"\[DONE\]\s+([a-zA-Z0-9_\-\.]+\.tif[f]?)"

    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            for line in f:
                if "[DONE]" in line:
                    match = re.search(regex_pattern, line)
                    if match:
                        completed_files.add(match.group(1).strip())
    except Exception as e:
        print(f"Warning: Could not read log file: {e}")
        
    return completed_files


def process_single_image(file_path):
    """
    Worker function to process a single image.
    """
    filename = os.path.basename(file_path)
    name_no_ext = os.path.splitext(filename)[0]
    
    try:
        crop_size = get_split_size(filename)
        Image.MAX_IMAGE_PIXELS = None 
        
        start_job = time.time()
        
        with Image.open(file_path) as img:
            width, height = img.size
            tile_count = 0
            
            logging.info(f"STARTING: {filename}")

            for y in range(0, height, crop_size):
                for x in range(0, width, crop_size):
                    
                    box = (x, y, min(x + crop_size, width), min(y + crop_size, height))
                    tile = img.crop(box)
                    
                    if crop_size == SIZE_LARGE:
                        tile = tile.resize((TARGET_OUTPUT_SIZE, TARGET_OUTPUT_SIZE), Image.Resampling.LANCZOS)
                    
                    if tile.mode in ('RGBA', 'P', 'LA'):
                        tile = tile.convert('RGB')
                        
                    out_name = f"{name_no_ext}_{tile_count}.jpg"
                    out_path = os.path.join(OUTPUT_FOLDER, out_name)
                    
                    tile.save(out_path, format="JPEG", quality=QA)
                    tile_count += 1

            elapsed = time.time() - start_job
            return f"[DONE] {filename} | Size: {width}x{height} | Mode: {crop_size}px->{TARGET_OUTPUT_SIZE}px | Tiles: {tile_count} | Time: {elapsed:.2f}s"

    except Exception as e:
        return f"[ERROR] {filename} | {str(e)}"


def main():
    all_files = [f for f in os.listdir(INPUT_FOLDER) if f.lower().endswith(('.tif', '.tiff'))]
    
    print(f"Reading log file to identify completed jobs: {LOG_FILE_PATH}")
    completed_files = get_completed_files(LOG_FILE_PATH)
    print(f"Found {len(completed_files)} already completed files.")

    files_to_process = []
    for f in all_files:
        if f not in completed_files:
            files_to_process.append(os.path.join(INPUT_FOLDER, f))

    total_files = len(files_to_process)
    
    if total_files == 0:
        print("All files in the directory are already marked as [DONE] in the log.")
        return

    print(f"Starting processing on remaining {total_files} files using {MAX_WORKERS} cores...")
    print("-" * 80)

    start_time = time.time()

    try:
        with Pool(processes=MAX_WORKERS, maxtasksperchild=3) as pool:   
            for i, result in enumerate(pool.imap_unordered(process_single_image, files_to_process), 1):
                logging.info(f"({i}/{total_files}) {result}")
                
    except Exception as e:
        logging.error(f"Critical System Error: {e}")

    duration = time.time() - start_time
    print("-" * 80)
    print(f"Batch run completed in {duration:.2f} seconds.")

if __name__ == '__main__':
    main()
