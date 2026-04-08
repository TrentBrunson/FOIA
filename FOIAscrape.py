# ================================================
# SCRIPT 1: SEC FOIA Logs Downloader
# ================================================
# Run this first to download ALL logs from 2018 to present.
# It automatically scrapes the official SEC page and downloads every foia-log CSV/ZIP.

import requests
from bs4 import BeautifulSoup
from pathlib import Path
import time
from tqdm import tqdm
import sys

def download_all_foia_logs(destination_folder: str):
    dest = Path(destination_folder).expanduser().resolve()
    dest.mkdir(parents=True, exist_ok=True)
    
    print(f"✅ Saving files to: {dest}")
    
    # Main SEC FOIA logs page
    url = "https://www.sec.gov/foia-services/frequently-requested-documents/foia-logs"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to load SEC page: {e}")
        sys.exit(1)
    
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Find every link that looks like a FOIA log file
    download_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ("foia-log" in href.lower() and 
            (href.endswith(".csv") or href.endswith(".zip"))):
            # Make absolute URL
            if href.startswith("/"):
                full_url = "https://www.sec.gov" + href
            else:
                full_url = href
            download_links.append(full_url)
    
    print(f"Found {len(download_links)} log files to download...")
    
    for link in tqdm(download_links, desc="Downloading SEC FOIA logs"):
        filename = Path(link).name
        filepath = dest / filename
        
        if filepath.exists():
            print(f"⏭️  Skipping {filename} (already exists)")
            continue
        
        try:
            r = requests.get(link, headers=headers, stream=True, timeout=60)
            r.raise_for_status()
            
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"✅ Downloaded: {filename}")
            
        except Exception as e:
            print(f"❌ Failed {filename}: {e}")
        
        time.sleep(0.5)  # Be polite to SEC servers
    
    print("\n🎉 All downloads complete!")

if __name__ == "__main__":
    folder = input("Enter destination folder (or press Enter for ./sec_foia_logs): ") or "./sec_foia_logs"
    download_all_foia_logs(folder)
