"""
Step: [base_pubmed_csv.csv, base_pubmed_json.csv] -> stg_pubmed.csv
"""

import os

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BASE_PUBMED_CSV_PATH = os.environ.get("BASE_PUBMED_CSV_PATH")
BASE_PUBMED_JSON_PATH = os.environ.get("BASE_PUBMED_JSON_PATH")
STG_PUBMED_PATH = os.environ.get("STG_PUBMED_PATH")


def stg_pubmed_processing(pubmed_csv_path: str, pubmed_json_path: str) -> pd.DataFrame:
    """Process the base PubMed files by concatenating them together.

    Args:
        pubmed_csv_path: Path of the base pubmed_csv file.
        pubmed_json_path: Path of the base pubmed_json file.

    Returns:
        Pandas Dataframe containing the content of both files.

    """
    # NOTE: In order to reduce useless boilerplate code in favor of readability at this stage I voluntarily chose
    # to 'hardcode' the concatenation of the 2 files like so. In a case where we have more than 2 source files,
    # iterating over these files will of course be the solution.
    print("\n--- Step: [base_pubmed_csv.csv, base_pubmed_json.csv] -> stg_pubmed.csv ---")
    base_pubmed_csv_df = pd.read_csv(pubmed_csv_path)
    base_pubmed_json_df = pd.read_csv(pubmed_json_path)
    stg_pubmed_df = pd.concat([base_pubmed_csv_df, base_pubmed_json_df])

    return stg_pubmed_df


def main():
    """Process the PubMed base files and write the data into a single staging CSV file.

    Args:
        None

    Returns:
        None
    """
    stg_pubmed_df = stg_pubmed_processing(BASE_PUBMED_CSV_PATH, BASE_PUBMED_JSON_PATH)
    print("Saving to csv..")
    stg_pubmed_df.to_csv(STG_PUBMED_PATH, index=False)


if __name__ == "__main__":
    main()
