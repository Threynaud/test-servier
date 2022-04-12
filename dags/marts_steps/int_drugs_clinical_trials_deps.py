"""
Step: [stg_drugs.csv, stg_clinical_trials.csv] -> int_drugs_clinical_trials_deps.csv
"""

import os


import pandas as pd
from dotenv import load_dotenv

from marts_steps.utils import load_drugs_lookup, find_drugs_in_title, dependency_formatter

load_dotenv()

STG_DRUGS_PATH = os.environ.get("STG_DRUGS_PATH")
STG_CLINICAL_TRIALS_PATH = os.environ.get("STG_CLINICAL_TRIALS_PATH")
INT_DRUGS_CLINICAL_TRIALS_DEPS_PATH = os.environ.get("INT_DRUGS_CLINICAL_TRIALS_DEPS_PATH")


# I voluntarily did not refactor the two intermediate steps in a big and unreadable function handling both pubmed
# and clinical trials as the business logic behind one or the other might change and complicate unnecessarily the code.
def list_dependencies(stg_drugs_file_path: str, stg_clinical_trials_file_path: str) -> pd.DataFrame:
    """List dependencies between drugs and clinical trials and store them in a dataframe.

    Args:
        stg_drugs_file_path: Path of the staging drugs CSV file.
        stg_clinical_trials_file_path: Path of the staging clinical trials CSV file.

    Returns:
        Pandas Dataframe containing the relationship between drugs and clinical trials.

    """

    print("\n--- Step: [stg_drugs.csv, stg_clinical_trials.csv] -> int_drugs_clinical_trials_deps.csv ---")
    dependencies = []

    print("Loading drugs..")
    drugs_lookup = load_drugs_lookup(stg_drugs_file_path)
    print("Loading clinical trials..")
    stg_clinical_trials_df = pd.read_csv(stg_clinical_trials_file_path)

    print("Finding drugs in title..")
    for _, clinical_trial in stg_clinical_trials_df.iterrows():
        drugs_found = find_drugs_in_title(clinical_trial["scientific_title_preprocessed"], drugs_lookup)
        if drugs_found:
            for drug in drugs_found:
                dependency = dependency_formatter("clinical_trial", drug, clinical_trial)
                dependencies.append(dependency)

    int_drugs_clinical_trials_deps_df = pd.DataFrame(dependencies)
    return int_drugs_clinical_trials_deps_df


def main():
    """List dependencies between drugs and clinical trials and write the result in an intermediate CSV file.

    Args:
        None

    Returns:
        None
    """
    int_drugs_clinical_trials_deps_df = list_dependencies(STG_DRUGS_PATH, STG_CLINICAL_TRIALS_PATH)
    print("Saving dependencies to csv..")
    int_drugs_clinical_trials_deps_df.to_csv(INT_DRUGS_CLINICAL_TRIALS_DEPS_PATH, index=False)


if __name__ == "__main__":
    main()
