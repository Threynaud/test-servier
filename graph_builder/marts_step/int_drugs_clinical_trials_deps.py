import os

import pandas as pd
from dotenv import load_dotenv

from utils import load_drugs_lookup, find_drugs_in_title, dependency_formatter

load_dotenv()

STG_DRUGS_PATH = os.environ.get("STG_DRUGS_PATH")
STG_CLINICAL_TRIALS_PATH = os.environ.get("STG_CLINICAL_TRIALS_PATH")
INT_DRUGS_CLINICAL_TRIALS_DEPS_PATH = os.environ.get("INT_DRUGS_CLINICAL_TRIALS_DEPS_PATH")


def list_dependencies(stg_drugs_file, stg_clinical_trials_file):
    dependencies = []

    drugs_lookup = load_drugs_lookup(stg_drugs_file)
    stg_clinical_trials_df = pd.read_csv(stg_clinical_trials_file)

    for _, clinical_trial in stg_clinical_trials_df.iterrows():
        drugs_found = find_drugs_in_title(clinical_trial["scientific_title_preprocessed"], drugs_lookup)
        if drugs_found:
            for drug in drugs_found:
                dependency = dependency_formatter("clinical_trial", drug, clinical_trial)
                dependencies.append(dependency)

    int_drugs_clinical_trials_deps_df = pd.DataFrame(dependencies)
    return int_drugs_clinical_trials_deps_df


def main():
    int_drugs_clinical_trials_deps_df = list_dependencies(STG_DRUGS_PATH, STG_CLINICAL_TRIALS_PATH)
    int_drugs_clinical_trials_deps_df.to_csv(INT_DRUGS_CLINICAL_TRIALS_DEPS_PATH, index=False)


if __name__ == "__main__":
    main()
