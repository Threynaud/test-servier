"""
Answer to question 4: Return the name of the journal which mentions the largest number of different drugs.
"""

import os

import pandas as pd

from dotenv import load_dotenv

load_dotenv()

OUTPUT_DRUGS_GRAPH_JSON_PATH = os.environ.get("OUTPUT_DRUGS_GRAPH_JSON_PATH")


def get_top_journals(output_drugs_graph_json_path: str) -> pd.DataFrame:
    """Get the journals mentionning the largest number of different drugs from the output JSON.

    Args:
        output_drugs_graph_json_path: Path of the fact output JSON file containing the relationships
        between drugs and publications.
    Returns:
        Pandas Dataframe containing the top journals and the count of drugs.

    """
    df = pd.read_json(output_drugs_graph_json_path, convert_dates=False)
    journal_counts_df = df.groupby("journal")["drug_name"].nunique().reset_index(name="count")

    # For each journal, count the number of unique drugs and only keep the ones with the max count.
    top_journals_df = journal_counts_df.loc[journal_counts_df["count"] == journal_counts_df["count"].max()]
    top_journals_df.reset_index(drop=True, inplace=True)

    return top_journals_df


if __name__ == "__main__":
    top_journals_df = get_top_journals(OUTPUT_DRUGS_GRAPH_JSON_PATH)  # type: ignore
    max_count = top_journals_df["count"].max()
    journals = ", ".join(top_journals_df["journal"])

    if len(top_journals_df["journal"]) > 1:
        print(f"The journals mentioning the biggest number of drugs are: {journals}.")
        print(f"They mention {max_count} different drugs each.")
    else:
        print(f"The journal mentioning the biggest number of drugs is: {journals}.")
        print(f"It mentions {max_count} different drugs.")
