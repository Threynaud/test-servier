"""
Step: pubmed.csv -> base_pubmed_csv.csv
"""

import os

import pandas as pd
from dotenv import load_dotenv

from staging_steps.data_formatting import normalize_date, preprocess_title

load_dotenv()

PUBMED_CSV_INPUT_PATH = os.environ.get("PUBMED_CSV_INPUT_PATH")
BASE_PUBMED_CSV_PATH = os.environ.get("BASE_PUBMED_CSV_PATH")


def base_pubmed_csv_processing(pubmed_file_path: str) -> pd.DataFrame:
    """Process the PubMed CSV file by normalizing the date and adding a 'title_preprocessed' column for easier
    drugs recovery.

    Args:
        pubmed_file_path: Path of a raw PubMed CSV file.

    Returns:
        Pandas Dataframe storing the processed CSV file.

    """
    print("\n--- Step: pubmed.csv -> base_pubmed_csv.csv ---")
    pubmed_df = pd.read_csv(pubmed_file_path)

    # Normalize date
    print("Normalizing dates..")
    pubmed_df["date"] = pubmed_df["date"].apply(lambda x: normalize_date(x))

    # Preprocess title
    print("Preprocessing title..")
    pubmed_df["title_preprocessed"] = pubmed_df["title"].apply(lambda x: preprocess_title(x))

    return pubmed_df


def main():
    """Process the PubMed CSV file and write the result in a base CSV file.

    Args:
        None

    Returns:
        None
    """
    base_pubmed_df = base_pubmed_csv_processing(PUBMED_CSV_INPUT_PATH)
    print("Saving to csv..")
    base_pubmed_df.to_csv(BASE_PUBMED_CSV_PATH, index=False)


if __name__ == "__main__":
    main()
