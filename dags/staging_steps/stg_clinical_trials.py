"""
Step: clinical_trials.csv -> stg_clinical_trials.csv
"""

import os

import pandas as pd
from dotenv import load_dotenv

from staging_steps.data_formatting import normalize_date, remove_bytes, preprocess_title

load_dotenv()

CLINICAL_TRIALS_INPUT_PATH = os.environ.get("CLINICAL_TRIALS_INPUT_PATH")
STG_CLINICAL_TRIALS_PATH = os.environ.get("STG_CLINICAL_TRIALS_PATH")


def stg_clinical_trials_processing(clinical_trials_file_path: str) -> pd.DataFrame:
    """Process the source clinical_trials file.
    Clean the title from bytes like characters, normalize the dates, concatenate duplicates and preprocess the title.

    Args:
        clinical_trials_file_path: Path of the source clinical_trials CSV file.

    Returns:
        Pandas Dataframe containing the processed clinical_trials data.

    """

    print("\n--- Step: clinical_trials.csv -> stg_clinical_trials.csv ---")
    clinical_trials_df = pd.read_csv(clinical_trials_file_path)

    # Clean strings from bytes like characters
    print("Removing bytes like characters from title..")
    clinical_trials_df["scientific_title"] = clinical_trials_df["scientific_title"].apply(lambda x: remove_bytes(x))
    clinical_trials_df["journal"] = clinical_trials_df["journal"].astype(str).apply(lambda x: remove_bytes(x))

    # Normalize date
    print("Normalizing dates..")
    clinical_trials_df["date"] = clinical_trials_df["date"].apply(lambda x: normalize_date(x))

    # Merge lines if same title and date
    # WARNING: This is an ad_hoc step, this should not be handled this way in prod!! (This is very ugly)
    # See README for data quality handling!
    print("Aggregating duplicate lines..")
    clinical_trials_df = (
        clinical_trials_df.groupby(["scientific_title", "date"])
        .agg(
            {
                "id": lambda x: [id for id in x if not pd.isna(id)][0],
                "journal": lambda x: [journal for journal in x if journal != "nan"][0],
            }
        )
        .reset_index()
    )
    clinical_trials_df = clinical_trials_df[["id", "scientific_title", "date", "journal"]]

    # Preprocess title
    print("Preprocessing title..")
    clinical_trials_df["scientific_title_preprocessed"] = clinical_trials_df["scientific_title"].apply(
        lambda x: preprocess_title(x)
    )

    return clinical_trials_df


def main():
    """Process the clinical trials source files and write the data into a staging CSV file.

    Args:
        None

    Returns:
        None
    """

    stg_clinical_trials_df = stg_clinical_trials_processing(CLINICAL_TRIALS_INPUT_PATH)
    print("Saving to csv..")
    stg_clinical_trials_df.to_csv(STG_CLINICAL_TRIALS_PATH, index=False)


if __name__ == "__main__":
    main()
