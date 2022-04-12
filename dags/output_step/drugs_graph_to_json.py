"""
Step: fct_drugs_graph.csv -> output_drugs_graph.json
"""

import os

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

FCT_DRUGS_DEPS_PATH = os.environ.get("FCT_DRUGS_DEPS_PATH")
OUTPUT_DRUGS_GRAPH_JSON_PATH = os.environ.get("OUTPUT_DRUGS_GRAPH_JSON_PATH")


def output_processing(fct_drugs_deps_path: str) -> pd.DataFrame:
    """Process the fact drugs relationships CSV.

    Args:
        fct_drugs_deps_path: Path of the fact data file containing the relationships between drugs and publications.
    Returns:
        Pandas Dataframe containing the content of the fact file.

    """

    print("\n--- Step: fct_drugs_graph.csv -> output_drugs_graph.json ---")
    fct_drugs_deps_df = pd.read_csv(fct_drugs_deps_path)
    return fct_drugs_deps_df


def main():
    """Process the fact drugs relationships CSV file and store it in the wanted JSON file.

    Args:
        None

    Returns:
        None
    """
    fct_drugs_deps_df = output_processing(FCT_DRUGS_DEPS_PATH)
    print("Saving graph to JSON..")
    fct_drugs_deps_df.to_json(OUTPUT_DRUGS_GRAPH_JSON_PATH, orient="records", indent=2, force_ascii=False)


if __name__ == "__main__":
    main()
