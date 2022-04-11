"""
Note:
Since I have troubles running Airflow locally, I added this small python script to run all the steps of the pipeline
at once. This is not production quality code, neither a 'pipeline' per se, only a utility!
"""
# TODO: Make a CLI of this

from marts_steps import (
    fct_drugs_graph,
    int_drugs_clinical_trials_deps,
    int_drugs_pubmed_deps,
)
from output_step import drugs_graph_to_json
from staging_steps import (
    base_pubmed_csv,
    base_pubmed_json,
    stg_clinical_trials,
    stg_drugs,
    stg_pubmed,
)


def run_backup_pipeline():
    stg_drugs.main()
    stg_clinical_trials.main()
    base_pubmed_csv.main()
    base_pubmed_json.main()
    stg_pubmed.main()
    int_drugs_clinical_trials_deps.main()
    int_drugs_pubmed_deps.main()
    fct_drugs_graph.main()
    drugs_graph_to_json.main()


if __name__ == "__main__":
    run_backup_pipeline()
