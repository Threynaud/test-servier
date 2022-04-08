import os

import pandas as pd
from dotenv import load_dotenv

from data_formating import normalize_date, preprocess_title

load_dotenv()

PUBMED_CSV_INPUT_PATH = os.environ.get("PUBMED_CSV_INPUT_PATH")
BASE_PUBMED_CSV_PATH = os.environ.get("BASE_PUBMED_CSV_PATH")


def base_pubmed_csv_processing(pubmed_file_path):
    """ """
    pubmed_df = pd.read_csv(pubmed_file_path)

    # Normalize date
    pubmed_df["date"] = pubmed_df["date"].apply(lambda x: normalize_date(x))

    # Preprocess title
    pubmed_df["title_preprocessed"] = pubmed_df["title"].apply(lambda x: preprocess_title(x))

    return pubmed_df


def main():
    """ """
    base_pubmed_df = base_pubmed_csv_processing(PUBMED_CSV_INPUT_PATH)
    base_pubmed_df.to_csv(BASE_PUBMED_CSV_PATH, index=False)


if __name__ == "__main__":
    main()
