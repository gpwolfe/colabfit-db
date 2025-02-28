import os
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


def download_bz2_files(url, target_directory):
    # Ensure the target directory exists
    os.makedirs(target_directory, exist_ok=True)

    try:
        # Fetch the webpage content
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch URL: {e}")
        sys.exit(1)

    # Parse the webpage to find links
    soup = BeautifulSoup(response.text, "html.parser")
    links = soup.find_all("a", href=True)

    # Filter links ending with ".bz2"
    bz2_links = [
        urljoin(url, link["href"]) for link in links if link["href"].endswith(".bz2")
    ]

    if not bz2_links:
        print("No .bz2 files found on the given URL.")
        return

    # Download each file
    for file_url in bz2_links:
        file_name = os.path.basename(file_url)
        target_path = os.path.join(target_directory, file_name)

        try:
            print(f"Downloading {file_url}...")

            file_response = requests.get(file_url, stream=True)
            file_response.raise_for_status()

            # Save the file
            with open(target_path, "wb") as file:
                for chunk in file_response.iter_content(chunk_size=8192):
                    file.write(chunk)

            print(f"Saved: {target_path}")
        except requests.RequestException as e:
            print(f"Failed to download {file_url}: {e}")


if __name__ == "__main__":
    # Check for correct number of arguments
    if len(sys.argv) != 3:
        print("Usage: python download_bz2.py <url> <target_directory>")
        sys.exit(1)

    # Get arguments
    input_url = sys.argv[1]
    target_dir = sys.argv[2]

    # Run the downloader
    download_bz2_files(input_url, target_dir)
