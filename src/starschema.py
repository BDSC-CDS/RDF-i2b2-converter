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

def fill_obs_missing_values():
    """
    Replace the None values in "ENCOUNTER_NUM" by -1,
    Replace the None values in "START_DATE" by the default date specified in the config file.
    """
    pass

def gen_patient_dim():
    """
    Query the RDF graph about patient information and store the details in a dedicated table.
    """
    pass

def gen_patient_mapping():
    """
    Replace the non-integer (or too large) fields of "PATIENT_NUM" (both in observation and patient_dimension) by an integer. Store the index mapping in a dedicated table.
    """
    pass


def gen_encounter_dim():
    """
    Query the RDF graph about encounter information and store the details in a dedicated table.
    """
    pass


def gen_encounter_mapping():
    pass


def gen_provider_dim():
    pass


def gen_table_access(folder_path=OUTPUT_TABLES, metadata_filenames=["METADATA.csv"]):
    dfs=[pd.read_csv(folder_path + fname) for fname in metadata_filenames]
    df = pd.concat(dfs) if len(dfs)>1 else dfs[0]
    table_access = pd.DataFrame(columns=COLUMNS["TABLE_ACCESS"])
    inter = table_access.columns.intersection(df.columns)
    table_access[inter] = df.loc[(df["C_HLEVEL"]==0) & (df["C_FACTTABLECOLUMN"]=="CONCEPT_CD"), inter]
    table_access["C_TABLE_CD"]=table_access["C_NAME"]
    table_access["C_TABLE_NAME"]="sphn"
    table_access.fillna("").to_csv(folder_path+"TABLE_ACCESS.csv", index=False)
