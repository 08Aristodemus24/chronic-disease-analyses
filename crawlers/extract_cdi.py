from argparse import ArgumentParser

import os
import requests
import zipfile


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-L", type=str, help="represents the url/hyperlink that points to the file to be downloaded")
    args = parser.parse_args()

    # extract cdi
    link = args.L
    response = requests.get(link, stream=True)

    DATA_DIR = "../data/chronic-disease-data"
    os.makedirs(DATA_DIR, exist_ok=True)

    # save the raw extracted files to data directory 
    FILE_NAME = f"U.S._Chronic_Disease_Indicators__CDI___2023_Release.zip"
    FILE_PATH = os.path.join(DATA_DIR, FILE_NAME)

    # download the file as a binary zip file given the urls
    print("downloading chronic disease indicators data...")
    with open(FILE_PATH, mode="wb") as file:
        for chunk in response.iter_content(chunk_size=10 * 1024):
            file.write(chunk)
        file.close()
            
    # read the binary zip file and extract its contents
    with zipfile.ZipFile(FILE_PATH, mode="r") as zip_ref:
        zip_ref.extractall(DATA_DIR)

    # once contents are extracted remove the file
    os.remove(FILE_PATH)
