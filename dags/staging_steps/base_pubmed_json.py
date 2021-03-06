"""
Step: pubmed.json -> base_pubmed_json.csv
"""

import os

import pandas as pd
from dotenv import load_dotenv

from staging_steps.data_formatting import DATE_FORMAT, preprocess_title

load_dotenv()

PUBMED_JSON_INPUT_PATH = os.environ.get("PUBMED_JSON_INPUT_PATH")
BASE_PUBMED_JSON_PATH = os.environ.get("BASE_PUBMED_JSON_PATH")


def base_pubmed_json_processing(pubmed_file_path: str) -> pd.DataFrame:
    """Process the PubMed JSON file by normalizing the date and adding a 'title_preprocessed' column for easier
    drugs recovery. Also, fill missing ids with -1.

    Args:
        pubmed_file_path: Path of a raw PubMed JSON file.

    Returns:
        Pandas Dataframe storing the processed JSON file.

    """
    print("\n--- Step: pubmed.json -> base_pubmed_json.csv ---")
    pubmed_df = pd.read_json(pubmed_file_path)

    # Normalize date
    print("Normalizing dates..")
    pubmed_df["date"] = pubmed_df["date"].dt.strftime(DATE_FORMAT)

    # I'm not putting too much reflexion on what do to with missing values as we should understand the business logic
    # behind this: why do we have missing ids? What do we need them for? What's the impact?
    print("Filling missing ids..")
    pubmed_df["id"] = pubmed_df["id"].replace("", -1)
    pubmed_df["id"] = pubmed_df["id"].fillna(-1)
    pubmed_df["id"] = pubmed_df["id"].astype(int)

    # # Preprocess title
    print("Preprocessing title..")
    pubmed_df["title_preprocessed"] = pubmed_df["title"].apply(lambda x: preprocess_title(x))

    return pubmed_df


def main():
    """Process the PubMed JSON file and write the result in a base CSV file.

    Args:
        None

    Returns:
        None
    """
    base_pubmed_df = base_pubmed_json_processing(PUBMED_JSON_INPUT_PATH)
    print("Saving to csv..")
    base_pubmed_df.to_csv(BASE_PUBMED_JSON_PATH, index=False)


if __name__ == "__main__":
    main()
