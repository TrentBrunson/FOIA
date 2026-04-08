# ================================================
# OPTIMIZED SEC FOIA Logs Downloader (Fast Version)
# ================================================

import requests
from bs4 import BeautifulSoup
from pathlib import Path
import time
from tqdm import tqdm
import concurrent.futures
import sys
from functools import partial

def download_file(url: str, dest_folder: Path, headers: dict, max_retries=3):
    filename = Path(url).name
    filepath = dest_folder / filename
    
    if filepath.exists() and filepath.stat().st_size > 1000:  # Skip if already exists and not empty
        return f"⏭️  Skipped (exists): {filename}"
    
    for attempt in range(max_retries):
        try:
            with requests.get(url, headers=headers, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(filepath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=32*1024):
                        f.write(chunk)
            return f"✅ Downloaded: {filename}"
        except Exception as e:
            if attempt == max_retries - 1:
                return f"❌ Failed {filename} after {max_retries} tries: {e}"
            time.sleep(1 * (attempt + 1))  # Backoff

def download_all_foia_logs(destination_folder: str, max_workers=8):
    dest = Path(destination_folder).expanduser().resolve()
    dest.mkdir(parents=True, exist_ok=True)
    
    print(f"✅ Saving to: {dest}")
    print(f"🔄 Using {max_workers} parallel downloads\n")
    
    url = "https://www.sec.gov/foia-services/frequently-requested-documents/foia-logs"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")
    
    download_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ("foia-log" in href.lower() and (href.endswith(".csv") or href.endswith(".zip"))):
            full_url = "https://www.sec.gov" + href if href.startswith("/") else href
            download_links.append(full_url)
    
    print(f"Found {len(download_links)} files. Starting parallel download...\n")
    
    download_func = partial(download_file, dest_folder=dest, headers=headers)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(tqdm(
            executor.map(download_func, download_links),
            total=len(download_links),
            desc="Downloading"
        ))
    
    print("\n🎉 Download session complete!")
    for result in results:
        if "Failed" in result or "Downloaded" in result:
            print(result)

if __name__ == "__main__":
    folder = input("Enter destination folder (default: ./sec_foia_logs): ") or "./sec_foia_logs"
    workers = int(input("Max parallel downloads (recommended 4-12): ") or "8")
    download_all_foia_logs(folder, max_workers=workers)
