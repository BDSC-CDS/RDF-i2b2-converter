from i2b2wrappers import *


def gen_concept_modifier_dim(metadata_path=METADATA_PATH):
    """
    Build two tables: i2b2's CONCEPT_DIMENSION and MODIFIER_DIMENSION that stores the concepts and modifiers codes as well as the full paths used in the ontology.
    These informations are needed to join CRC tables to ontology tables.
    """
    df = pd.read_csv(metadata_path)
    concept_df = pd.DataFrame(columns=["CONCEPT_PATH", "CONCEPT_CD", "NAME_CHAR", "CONCEPT_BLOB", "UPDATE_DATE", "DOWNLOAD_DATE", "IMPORT_DATE", "SOURCESYSTEM_CD", "UPLOAD_ID"])
    modifier_df = pd.DataFrame(columns=["MODIFIER_PATH", "MODIFIER_CD", "NAME_CHAR", "MODIFIER_BLOB", "UPDATE_DATE", "DOWNLOAD_DATE", "IMPORT_DATE", "SOURCESYSTEM_CD", "UPLOAD_ID"])

    concept_df[["CONCEPT_PATH", "CONCEPT_CD", "NAME_CHAR"]] =  df.loc[df["C_TABLENAME"]=="CONCEPT_DIMENSION", ["C_FULLNAME", "C_BASECODE", "C_NAME"]]
    modifier_df[["CONCEPT_PATH", "CONCEPT_CD", "NAME_CHAR"]] =  df.loc[df["C_TABLENAME"]=="MODIFIER_DIMENSION", ["C_FULLNAME", "C_BASECODE", "C_NAME"]]

    concept_df.fillna("").to_csv(os.path.dirname(metadata_path)+"CONCEPT_DIMENSION")
    modifier_df.fillna("").to_csv(os.path.dirname(metadata_path)+"MODIFIER_DIMENSION")

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

def gen_table_access():
    pass