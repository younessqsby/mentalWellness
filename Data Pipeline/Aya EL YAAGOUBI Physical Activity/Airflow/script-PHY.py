mport os
from kaggle.api.kaggle_api_extended import KaggleApi

def scrape_data():
    """
    Scrape datasets listed in 'datasets.txt' located in the same directory as t>

    Returns:
        list: List of paths where datasets were downloaded.
    """
    dataset_list_file = "/home/toto/physical.txt"  # Known file name in the sam>
    download_base_path = "/home/toto/physical"  # Base directory for storing al>
    os.makedirs(download_base_path, exist_ok=True)

    # Authenticate with Kaggle
    api = KaggleApi()
    api.authenticate()

    downloaded_paths = []

    # Read dataset names/URLs from the file
    with open(dataset_list_file, "r") as file:
        datasets = file.readlines()

    # Loop through the dataset list
    for dataset in datasets:
        dataset = dataset.strip()  # Remove extra whitespace or newline charact>
        if not dataset:
            continue  # Skip empty lines
        dataset_name = dataset.split("/")[-1]  # Extract the dataset name
        download_path = os.path.join(download_base_path, dataset_name)
        os.makedirs(download_path, exist_ok=True)

        # Download and extract the dataset
        print(f"Downloading dataset: {dataset}...")
        api.dataset_download_files(dataset, path=download_path, unzip=True)
        print(f"Dataset downloaded and extracted to: {download_path}")
        downloaded_paths.append(download_path)

    return downloaded_paths


if __name__ == "__main__":
    # Step 1: Scrape the datasets
    downloaded_data_dirs = scrape_data()
    print("Downloaded datasets:")
    for path in downloaded_data_dirs:
            print(f"- {path}")
