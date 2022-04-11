"""
Helpers to find relationships between a drug name in a title.
"""

from typing import Dict, List

import pandas as pd


def load_drugs_lookup(stg_drugs_file_path: str) -> Dict[str, Dict[str, str]]:
    """
    Build a dict of drugs to improve the time complexity of the drug name search.

    Args:
        stg_drugs_file_path: Path of the staging drugs CSV file.

    Returns:
        Dictionary in the form {'diphenhydramine': {'atccode': 'A04AD', 'drug': 'DIPHENHYDRAMINE'}, ...}.
    """
    stg_drugs_df = pd.read_csv(stg_drugs_file_path)
    drugs_lookup = stg_drugs_df.set_index("drug_preprocessed").to_dict("index")
    return drugs_lookup


def find_drugs_in_title(title: str, drugs_lookup: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Find all drugs mentionned in a string.

    Args:
        title: Title of the publication, preferably preprocessed.
        drugs_lookup: Dictionary of a drug {'diphenhydramine': {'atccode': 'A04AD', 'drug': 'DIPHENHYDRAMINE'}, ...}.

    Returns:
        List of drugs found in the title in the form [{'atccode': 'A04AD', 'drug': 'DIPHENHYDRAMINE'}, ...].
    """

    if pd.isna(title):
        return []

    drugs_found = []
    words = title.split()
    for word in words:
        if word in drugs_lookup:
            drugs_found.append(drugs_lookup[word])

    # Remove potential duplicates if a drug is named twice or more in the same title
    # https://stackoverflow.com/questions/9427163/remove-duplicate-dict-in-list-in-python
    drugs_found = [dict(t) for t in {tuple(d.items()) for d in drugs_found}]

    return drugs_found


def dependency_formatter(ref_type: str, drug: Dict[str, str], ref: pd.Series):
    """
    Format an edge of the output graph with the wanted metadata.

    Args:
        ref_type: Type of the reference at hand, either 'pubmed' or 'clinical_trial'
        drug: Dictionary of a drug {'atccode': 'A04AD', 'drug': 'DIPHENHYDRAMINE'}.
        ref: Row of a pd.Dataframe containing the data of a reference, either a PubMed or a clinical trial.

    Returns:
        A dependency dict representing an edge of the output graph with its metadata.
    """
    dependency = {
        "drug_atcode": drug["atccode"],
        "drug_name": drug["drug"],
        "ref_type": ref_type,
        "ref_id": ref["id"],
        "ref_title": ref["scientific_title"] if ref_type == "clinical_trial" else ref["title"],  # TODO: raise exception
        "date": ref["date"],
        "journal": ref["journal"],
    }

    return dependency
