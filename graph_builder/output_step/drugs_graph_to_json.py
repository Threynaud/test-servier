import os

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

FCT_DRUGS_DEPS_PATH = os.environ.get("FCT_DRUGS_DEPS_PATH")
OUTPUT_DRUGS_GRAPH_JSON_PATH = os.environ.get("OUTPUT_DRUGS_GRAPH_JSON_PATH")


def output_processing(fct_drugs_deps_path):
    """ """
    fct_drugs_deps_df = pd.read_csv(fct_drugs_deps_path)
    return fct_drugs_deps_df


def main():
    """ """
    fct_drugs_deps_df = output_processing(FCT_DRUGS_DEPS_PATH)
    fct_drugs_deps_df.to_json(
        OUTPUT_DRUGS_GRAPH_JSON_PATH, orient="records", indent=2, force_ascii=False
    )


if __name__ == "__main__":
    main()
