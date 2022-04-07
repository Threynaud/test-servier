import os

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DRUGS_INPUT_PATH = os.environ.get("DRUGS_INPUT_PATH")
STG_DRUGS_PATH = os.environ.get("STG_DRUGS_PATH")


def stg_drugs_processing(drugs_file_path):
    """ """
    drugs_df = pd.read_csv(drugs_file_path)
    drugs_df["drug_lowercase"] = drugs_df["drug"].str.lower()  # Keep raw drug name for restitution
    return drugs_df


def main():
    """ """
    stg_drugs_df = stg_drugs_processing(DRUGS_INPUT_PATH)
    stg_drugs_df.to_csv(STG_DRUGS_PATH, index=False)


if __name__ == "__main__":
    main()
