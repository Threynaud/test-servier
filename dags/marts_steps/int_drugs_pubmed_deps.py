"""
Step: [stg_drugs.csv, stg_pubmed_csv.csv] -> int_drugs_pubmed_deps.csv
"""

import os

import pandas as pd
from dotenv import load_dotenv

from marts_steps.utils import load_drugs_lookup, find_drugs_in_title, dependency_formatter

load_dotenv()

STG_DRUGS_PATH = os.environ.get("STG_DRUGS_PATH")
STG_PUBMED_PATH = os.environ.get("STG_PUBMED_PATH")
INT_DRUGS_PUBMED_DEPS_PATH = os.environ.get("INT_DRUGS_PUBMED_DEPS_PATH")


# I voluntarily did not refactor the two intermediate steps in a big and unreadable function handling both pubmed
# and clinical trials as the business logic behind one or the other might change and complicate unnecessarily the code.
def list_dependencies(stg_drugs_file_path: str, stg_pubmed_file_path: str) -> pd.DataFrame:
    """List dependencies between drugs and PubMed and store them in a dataframe.

    Args:
        stg_drugs_file_path: Path of the staging drugs CSV file.
        stg_pubmed_file: Path of the staging pubmed CSV file.

    Returns:
        Pandas Dataframe containing the relationship between drugs and PubMed.

    """

    print("\n--- Step: [stg_drugs.csv, stg_pubmed_csv.csv] -> int_drugs_pubmed_deps.csv ---")
    dependencies = []

    print("Loading drugs..")
    drugs_lookup = load_drugs_lookup(stg_drugs_file_path)
    print("Loading PubMed..")
    stg_pubmed_df = pd.read_csv(stg_pubmed_file_path)

    print("Finding drugs in title..")
    for _, pubmed in stg_pubmed_df.iterrows():
        drugs_found = find_drugs_in_title(pubmed["title_preprocessed"], drugs_lookup)
        if drugs_found:
            for drug in drugs_found:
                dependency = dependency_formatter("pubmed", drug, pubmed)
                dependencies.append(dependency)

    int_drugs_pubmed_deps_df = pd.DataFrame(dependencies)
    return int_drugs_pubmed_deps_df


def main():
    """List dependencies between drugs and clinical trials and write the result in an intermediate CSV file.

    Args:
        None

    Returns:
        None
    """
    int_drugs_pubmed_deps_df = list_dependencies(STG_DRUGS_PATH, STG_PUBMED_PATH)
    print("Saving dependencies to csv..")
    int_drugs_pubmed_deps_df.to_csv(INT_DRUGS_PUBMED_DEPS_PATH, index=False)


if __name__ == "__main__":
    main()
