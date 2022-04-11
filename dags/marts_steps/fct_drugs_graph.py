"""
Step: [int_drugs_clinical_trials_deps.csv, int_drugs_pubmed_deps.csv] -> fct_drugs_graph.csv
"""

import os

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

INT_DRUGS_CLINICAL_TRIALS_DEPS_PATH = os.environ.get("INT_DRUGS_CLINICAL_TRIALS_DEPS_PATH")
INT_DRUGS_PUBMED_DEPS_PATH = os.environ.get("INT_DRUGS_PUBMED_DEPS_PATH")
FCT_DRUGS_DEPS_PATH = os.environ.get("FCT_DRUGS_DEPS_PATH")


def fct_drugs_graph_building(int_drugs_clinical_trials_deps_path: str, int_drugs_pubmed_deps_path: str) -> pd.DataFrame:
    """Process the intermediate drugs relationships files and concatenate them into a dataframe.

    Args:
        int_drugs_clinical_trials_deps_path: Path of the intermediate drugs <-> clinical trials CSV file.
        int_drugs_pubmed_deps_path: Path of the intermediate drugs <-> pubmed CSV file.

    Returns:
        Pandas Dataframe containing the content of both files.

    """

    int_drugs_clinical_trials_deps_df = pd.read_csv(int_drugs_clinical_trials_deps_path)
    int_drugs_pubmed_deps_df = pd.read_csv(int_drugs_pubmed_deps_path)
    fct_drugs_graph_df = pd.concat([int_drugs_clinical_trials_deps_df, int_drugs_pubmed_deps_df], ignore_index=True)

    return fct_drugs_graph_df


def main():
    """Process the intermediate drugs relationships files and concatenate them into a da single fact CSV file.

    Args:
        None

    Returns:
        None
    """
    fct_drugs_graph_df = fct_drugs_graph_building(INT_DRUGS_CLINICAL_TRIALS_DEPS_PATH, INT_DRUGS_PUBMED_DEPS_PATH)
    fct_drugs_graph_df.to_csv(FCT_DRUGS_DEPS_PATH, index=True, index_label="dependency_id")


if __name__ == "__main__":
    main()
