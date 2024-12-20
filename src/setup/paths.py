import os 
from pathlib import Path 

from tqdm import tqdm 


PARENT_DIR = Path("_file_").parent.resolve()
IMAGES_DIR = PARENT_DIR/"images"

DATA_DIR = PARENT_DIR/"data"
RAW_DATA_DIR = DATA_DIR/"raw"

MODELS_DIR = PARENT_DIR/"models"
LOCAL_SAVE_DIR = MODELS_DIR/"locally_created"
COMET_SAVE_DIR = MODELS_DIR/"comet_downloads"

CLEANED_DATA = DATA_DIR/"cleaned"
TRANSFORMED_DATA = DATA_DIR/"transformed"
GEOGRAPHICAL_DATA = DATA_DIR/"geographical"

TIME_SERIES_DATA = TRANSFORMED_DATA/"time_series"
TRAINING_DATA = TRANSFORMED_DATA/"training_data"
INFERENCE_DATA = TRANSFORMED_DATA/"inference"



def make_needed_directories() -> None:
    
    from src.setup.config import cities  # Forgive the odd placement. I'm avoiding circular import errors

    major_paths = [
        DATA_DIR, CLEANED_DATA, RAW_DATA_DIR, GEOGRAPHICAL_DATA, TRANSFORMED_DATA, TIME_SERIES_DATA, 
        IMAGES_DIR, TRAINING_DATA, INFERENCE_DATA, MODELS_DIR, LOCAL_SAVE_DIR, COMET_SAVE_DIR 
        
    ]

    for path in tqdm(iterable=major_paths, desc="Creating data directories..."):

        if not Path(path).exists():
            os.mkdir(path)
        
        # The directories that holds geographical data will have a different structure.
        for city in cities:  
            # These directories don't need subdirectories for each city
            if path not in [PARENT_DIR, DATA_DIR, TRANSFORMED_DATA, MODELS_DIR]:
                if not Path(path/city).exists():
                    os.mkdir(path/city)

        if path == GEOGRAPHICAL_DATA:
            for city in cities:
                for indexer_name in ["mixed_indexer", "rounding_indexer"]:
                    indexer_path_for_city = path/city/indexer_name
                    if not Path(indexer_path_for_city).exists():
                        os.mkdir(indexer_path_for_city)

