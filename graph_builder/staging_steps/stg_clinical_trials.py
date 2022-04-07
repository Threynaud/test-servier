import os
import re

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

CLINICAL_TRIALS_INPUT_PATH = os.environ.get("CLINICAL_TRIALS_INPUT_PATH")
STG_CLINICAL_TRIALS_PATH = os.environ.get("STG_CLINICAL_TRIALS_PATH")


def remove_bytes(my_str):
    my_str = re.sub(r"(\s*(\\x)\w+\s*)+", " ", my_str)
    my_str = my_str.strip()  # Remove potential space at the end of the string introduced by the operation above.
    return my_str


def stg_clinical_trials_processing(clinical_trials_file_path):
    """ """
    clinical_trials_df = pd.read_csv(clinical_trials_file_path)

    # Clean strings
    clinical_trials_df["scientific_title"] = clinical_trials_df["scientific_title"].apply(lambda x: remove_bytes(x))
    clinical_trials_df["journal"] = clinical_trials_df["journal"].astype(str).apply(lambda x: remove_bytes(x))

    # Keep raw strings for restitution
    clinical_trials_df["scientific_title_lowercase"] = clinical_trials_df["scientific_title"].str.lower()
    clinical_trials_df["journal_lowercase"] = clinical_trials_df["journal"].str.lower()

    return clinical_trials_df


def main():
    """ """
    stg_clinical_trials_df = stg_clinical_trials_processing(CLINICAL_TRIALS_INPUT_PATH)
    stg_clinical_trials_df.to_csv(STG_CLINICAL_TRIALS_PATH, index=False)


if __name__ == "__main__":
    main()
