import os

import pandas as pd
from dotenv import load_dotenv

from data_formating import normalize_date, remove_bytes

load_dotenv()

CLINICAL_TRIALS_INPUT_PATH = os.environ.get("CLINICAL_TRIALS_INPUT_PATH")
STG_CLINICAL_TRIALS_PATH = os.environ.get("STG_CLINICAL_TRIALS_PATH")


def stg_clinical_trials_processing(clinical_trials_file_path):
    """ """
    clinical_trials_df = pd.read_csv(clinical_trials_file_path)

    # Clean strings
    clinical_trials_df["scientific_title"] = clinical_trials_df["scientific_title"].apply(lambda x: remove_bytes(x))
    clinical_trials_df["journal"] = clinical_trials_df["journal"].astype(str).apply(lambda x: remove_bytes(x))

    # Normalize date
    clinical_trials_df["date"] = clinical_trials_df["date"].apply(lambda x: normalize_date(x))

    # Merge lines if same title and date
    # WARNING: This is an ad_hoc step, this should not be handled this way in prod!! (This is very ugly)
    # See README for data quality handling!
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
    print(clinical_trials_df["journal"].isna())
    print(clinical_trials_df)

    return clinical_trials_df


def main():
    """ """
    stg_clinical_trials_df = stg_clinical_trials_processing(CLINICAL_TRIALS_INPUT_PATH)
    stg_clinical_trials_df.to_csv(STG_CLINICAL_TRIALS_PATH, index=False)


if __name__ == "__main__":
    main()
