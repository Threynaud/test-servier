import os

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

PUBMED_CSV_INPUT_PATH = os.environ.get("PUBMED_CSV_INPUT_PATH")
BASE_PUBMED_CSV_PATH = os.environ.get("BASE_PUBMED_CSV_PATH")


def base_pubmed_csv_processing(pubmed_file_path):
    """ """
    pubmed_df = pd.read_csv(pubmed_file_path)
    pubmed_df["title_lowercase"] = pubmed_df["title"].str.lower()  # Keep raw title for restitution

    # NOTE: There should be a dedicated step/pipeline for data quality but we're missing context.
    # I'm not putting too much reflexion on what do to with missing values as we should understand the business logic
    # behind this: why do we have missing ids? What do we need them for? What's the impact?
    # TODO: Refactor this into a separate data quality file
    pubmed_df["id"] = pubmed_df["id"].replace("", -1)
    pubmed_df["id"] = pubmed_df["id"].fillna(-1)
    pubmed_df["id"] = pubmed_df["id"].astype(int)

    return pubmed_df


def main():
    """ """
    base_pubmed_df = base_pubmed_csv_processing(PUBMED_CSV_INPUT_PATH)
    base_pubmed_df.to_csv(BASE_PUBMED_CSV_PATH, index=False)


if __name__ == "__main__":
    main()
