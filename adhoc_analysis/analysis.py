import os

import pandas as pd

from dotenv import load_dotenv

load_dotenv()

OUTPUT_DRUGS_GRAPH_JSON_PATH = os.environ.get("OUTPUT_DRUGS_GRAPH_JSON_PATH")


def get_top_journals(output_drugs_graph_json_path):
    df = pd.read_json(output_drugs_graph_json_path, convert_dates=False)
    journal_counts_df = (
        df.groupby("journal")["drug_name"].nunique().reset_index(name="count")
    )

    top_journals_df = journal_counts_df.loc[
        journal_counts_df["count"] == journal_counts_df["count"].max()
    ]
    top_journals_df.reset_index(drop=True, inplace=True)

    return top_journals_df


if __name__ == "__main__":
    top_journals_df = get_top_journals(OUTPUT_DRUGS_GRAPH_JSON_PATH)
    max_count = top_journals_df["count"].max()
    journals = ", ".join(top_journals_df["journal"])

    if len(top_journals_df["journal"]) > 1:
        print(f"The journals mentioning the biggest number of drugs are: {journals}.")
        print(f"They mention {max_count} different drugs each.")
    else:
        print(f"The journal mentioning the biggest number of drugs is: {journals}.")
        print(f"It mentions {max_count} different drugs.")
