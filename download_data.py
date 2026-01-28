import time
import requests
from pathlib import Path

# 1. Setup paths
data_dir = Path("data")
data_dir.mkdir(parents=True, exist_ok=True)

# 2. Configuration
# Date range from 2018-01 to 2025-09
years = range(2018, 2026)
taxi_types = ["green", "yellow"]
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Headers to look like a real browser (prevents some 403 errors/blocks)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def download_file(taxi_type, yearmonth):
    filename = f"{taxi_type}_tripdata_{yearmonth}.parquet"
    url = f"{base_url}/{filename}"
    file_path = data_dir / filename

    # Skip if file already exists and is not empty
    if file_path.exists() and file_path.stat().st_size > 0:
        print(f"⏭️  Skipping {filename}, already exists.")
        return

    # Retry logic (up to 3 attempts)
    for attempt in range(3):
        try:
            print(f"📥 Downloading {filename} (Attempt {attempt + 1})...")
            # Using stream=True is better for large files
            with requests.get(url, headers=HEADERS, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            
            print(f"✅ Saved: {filename}")
            # Short sleep (0.5s) to avoid overwhelming the server
            time.sleep(0.5)
            return

        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [403, 404]:
                print(f"❌ Data not available for {taxi_type} {yearmonth} (404/403)")
                break # Don't retry for missing files
        except Exception as e:
            print(f"⚠️  Error downloading {filename}: {e}")
            if attempt < 2:
                time.sleep(2) # Wait longer before retrying
            else:
                print(f"🔥 Failed after 3 attempts: {filename}")
                if file_path.exists():
                    file_path.unlink() # Clean up partial downloads

# 3. Main Loop
print(f"🚀 Starting download process...")

for year in years:
    for month in range(1, 13):
        if year == 2025 and month > 9:
            break
        
        yearmonth = f"{year}-{month:02d}"
        
        for t_type in taxi_types:
            download_file(t_type, yearmonth)

print("✨ All downloads finished.")