import os
import io
import logging
import zipfile
import requests

logger = logging.getLogger(__name__)

def download_raw_file(url: str, target_path: str) -> str:
    """
    Downloads a file directly from a URL in streaming mode to handle large files.
    
    Args:
        url (str): The direct URL of the file.
        target_path (str): The full path where the file should be saved.
        
    Returns:
        str: The path to the downloaded file.
    """
    logger.info(f"Starting direct download from: {url}")
    
    # Ensuring the target directory exists before saving
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    
    # Using stream=True to avoid loading massive files entirely into memory
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(target_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192): 
                f.write(chunk)
                
    logger.info(f"File successfully downloaded to: {target_path}")
    return target_path


def download_and_extract_zip(url: str, extract_dir: str) -> list:
    """
    Downloads a ZIP file in memory and extracts its contents directly to a directory.
    
    Args:
        url (str): The direct URL of the ZIP file.
        extract_dir (str): The directory where the contents should be extracted.
        
    Returns:
        list: A list of paths for the extracted files.
    """
    logger.info(f"Downloading and extracting ZIP from: {url}")
    
    os.makedirs(extract_dir, exist_ok=True)
    extracted_files = []
    
    # Downloading the ZIP file into memory
    response = requests.get(url)
    response.raise_for_status()
    
    # Reading the bytes in memory and extracting
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        z.extractall(path=extract_dir)
        
        # Mapping the absolute paths of the extracted files to return them
        for file_name in z.namelist():
            extracted_files.append(os.path.join(extract_dir, file_name))
            
    logger.info(f"Successfully extracted {len(extracted_files)} files to {extract_dir}")
    return extracted_files