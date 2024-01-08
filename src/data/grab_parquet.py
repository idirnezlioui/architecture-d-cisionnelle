#The local directory to save the files, will be created if it doesn't exist
local_dir = Path("d:\documents\cours epsi\Architecture Décisionnelle\TP Datamart/taxi_data/raw")
local_dir.mkdir(parents=True, exist_ok=True)
#Define the base URL for downloading the datasets
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
def main():
    grab_data()
def grab_data() -> None:
    """Grab the data from New York Yellow Taxi
    This method download x files of the New York Yellow Taxi. 
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
 
    # Define the start and end years
    year = 2023
    for month in range(1, 13):
        if year == datetime.now().year and month > datetime.now().month:
            break
        try:
            download_data(year, month)
        except Exception as e:
            print(f"Could not download data for {year}-{month}: {e}")
            #Define a function to download the data for a specific year and month
def download_data(year, month):
    month = f"{month:02d}"
    url = f"{BASE_URL}{year}-{month}.parquet"
    local_file = local_dir / f"yellowtripdata{year}-{month}.parquet"
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"Downloaded {local_file}")