from i2b2wrappers import *


def gen_concept_modifier_dim(
    folder_path=OUTPUT_TABLES, metadata_filename="METADATA.csv"
):
    """
    Build two tables: i2b2's CONCEPT_DIMENSION and MODIFIER_DIMENSION that stores the concepts and modifiers codes as well as the full paths used in the ontology.
    These informations are needed to join CRC tables to ontology tables.
    """
    df = pd.read_csv(folder_path + metadata_filename)
    concept_df = pd.DataFrame(columns=COLUMNS["CONCEPT_DIMENSION"])
    modifier_df = pd.DataFrame(columns=COLUMNS["MODIFIER_DIMENSION"])

    concept_df[["CONCEPT_PATH", "CONCEPT_CD", "NAME_CHAR"]] = df.loc[
        df["C_TABLENAME"] == "CONCEPT_DIMENSION", ["C_FULLNAME", "C_BASECODE", "C_NAME"]
    ]
    modifier_df[["MODIFIER_PATH", "MODIFIER_CD", "NAME_CHAR"]] = df.loc[
        df["C_TABLENAME"] == "MODIFIER_DIMENSION",
        ["C_FULLNAME", "C_BASECODE", "C_NAME"],
    ]

    concept_df.fillna("").to_csv(folder_path + "CONCEPT_DIMENSION.csv", index=False)
    modifier_df.fillna("").to_csv(folder_path + "MODIFIER_DIMENSION.csv", index=False)


def gen_patient_mapping():
    pass


def gen_patient_dim():
    pass


def gen_encounter_dim():
    pass


def gen_encounter_mapping():
    pass


def gen_provider_dim():
    pass


def gen_table_access(path=OUTPUT_TABLES + "TABLE_ACCESS.csv"):
    table_access = pd.DataFrame(columns=COLUMNS["TABLE_ACCESS"])
    if len(ROOT_URIS)!=len(ROOT_PATHS) or len(ROOT_URIS)!=len(ONTOLOGY_NAMES):
        raise Exception("Please provide as many root URIs as root i2b2 paths as ontology names in the config files")
    for idx in range(len(ROOT_URIS)):
        table_access = table_access.append(
            {
                "C_TABLE_CD": "sphn",
                "C_TABLE_NAME": "sphn",
                "C_PROTECTED_ACCESS": "N",
                "C_HLEVEL": 0,
                "C_FULLNAME": ROOT_PATHS[idx],
                "C_NAME": ONTOLOGY_NAMES[idx],
                "C_SYNONYM_CD": "N",
                "C_VISUALATTRIBUTES": "CA",
                "C_FACTTABLECOLUMN": "CONCEPT_CD",
                "C_DIMTABLENAME": "CONCEPT_DIMENSION",
                "C_COLUMNNAME": "CONCEPT_PATH",
                "C_COLUMNDATATYPE": "T",
                "C_OPERATOR": "LIKE",
                "C_DIMCODE": ROOT_PATHS[idx],
            },
            ignore_index=True,
        )
    table_access.fillna("").to_csv(path, index=False)
