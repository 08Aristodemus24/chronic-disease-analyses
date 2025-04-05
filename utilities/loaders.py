import requests
import os
import re

from concurrent.futures import ThreadPoolExecutor


def download_dataset(meta_data: list[tuple] | set, data_dir="data"):
    

    # if directory already exists do nothing
    os.makedirs(f"./{data_dir}", exist_ok=True)

    def helper(meta_datum: tuple[str, str]):
        url, state = meta_datum
        file_name = url.split('/')[-1]
        file_dir = "/".join(url.split('/')[:-1])
        years = re.search(r"\d+-\d+", file_dir)[0]
        extension = re.search(r".[A-Za-z]+$", file_name)[0]
        new_file_name = f"{state}_{years}{extension}"
        print(new_file_name)

        # response = requests.get(url, stream=True)
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
        }
        response = requests.get(url, stream=True, headers=headers)

        # download the file given the urls
        with open(f"./{data_dir}/{new_file_name}", mode="wb") as file:
            for chunk in response.iter_content(chunk_size=10 * 1024):
                file.write(chunk)

    # concurrently download the files given url
    with ThreadPoolExecutor(max_workers=5) as exe:
        exe.map(helper, meta_data)