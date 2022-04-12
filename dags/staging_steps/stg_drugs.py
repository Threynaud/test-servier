"""
Step: drugs.csv -> stg_drugs.csv
"""

import os

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DRUGS_INPUT_PATH = os.environ.get("DRUGS_INPUT_PATH")
STG_DRUGS_PATH = os.environ.get("STG_DRUGS_PATH")


def stg_drugs_processing(drugs_file_path: str) -> pd.DataFrame:
    """Process the source drugs file by preprocessing the drug name for easier drugs recovery in titles.

    Args:
        drugs_file_path: Path of the source drugs CSV file.

    Returns:
        Pandas Dataframe containing the processed drugs data.

    """

    print("\n--- Step: drugs.csv -> stg_drugs.csv ---")
    drugs_df = pd.read_csv(drugs_file_path)

    # Preprocess drug name
    print("Preprocessing name..")
    drugs_df["drug_preprocessed"] = drugs_df["drug"].str.lower()

    return drugs_df


def main():
    """Process the drugs source and write the data into a single staging CSV file.

    Args:
        None

    Returns:
        None
    """
    stg_drugs_df = stg_drugs_processing(DRUGS_INPUT_PATH)
    print("Saving to csv..")
    stg_drugs_df.to_csv(STG_DRUGS_PATH, index=False)


if __name__ == "__main__":
    main()
