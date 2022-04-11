import pandas as pd


RELATION_ATTRIBUTES = [
    "drug_atcode",
    "drug_name",
    "relation_type",
    "ref_id",
    "date",
    "journal",
]


def load_drugs_lookup(stg_drugs_file):
    """
    Return a dict in the form: {'diphenhydramine': {'atccode': 'A04AD', 'drug': 'DIPHENHYDRAMINE'}, ...}
    which improves the time complexity of the drug name search.
    """
    stg_drugs_df = pd.read_csv(stg_drugs_file)
    drugs_lookup = stg_drugs_df.set_index("drug_preprocessed").to_dict("index")
    return drugs_lookup


def find_drugs_in_title(title, drugs_lookup):
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


def dependency_formatter(ref_type, drug, ref):
    dependency = {
        "drug_atcode": drug["atccode"],
        "drug_name": drug["drug"],
        "ref_type": ref_type,
        "ref_id": ref["id"],
        "ref_title": ref["scientific_title"]
        if ref_type == "clinical_trial"
        else ref["title"],
        "date": ref["date"],
        "journal": ref["journal"],
    }

    return dependency
