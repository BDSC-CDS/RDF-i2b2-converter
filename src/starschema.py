from i2b2wrappers import *


def gen_concept_modifier_dim(folder_path=os.path.dirname(METADATA_PATH), metadata_filename="METADATA"):
    """
    Build two tables: i2b2's CONCEPT_DIMENSION and MODIFIER_DIMENSION that stores the concepts and modifiers codes as well as the full paths used in the ontology.
    These informations are needed to join CRC tables to ontology tables.
    """
    df = pd.read_csv(folder_path+metadata_filename)
    concept_df = pd.DataFrame(columns=["CONCEPT_PATH", "CONCEPT_CD", "NAME_CHAR", "CONCEPT_BLOB", "UPDATE_DATE", "DOWNLOAD_DATE", "IMPORT_DATE", "SOURCESYSTEM_CD", "UPLOAD_ID"])
    modifier_df = pd.DataFrame(columns=["MODIFIER_PATH", "MODIFIER_CD", "NAME_CHAR", "MODIFIER_BLOB", "UPDATE_DATE", "DOWNLOAD_DATE", "IMPORT_DATE", "SOURCESYSTEM_CD", "UPLOAD_ID"])

    concept_df[["CONCEPT_PATH", "CONCEPT_CD", "NAME_CHAR"]] =  df.loc[df["C_TABLENAME"]=="CONCEPT_DIMENSION", ["C_FULLNAME", "C_BASECODE", "C_NAME"]]
    modifier_df[["CONCEPT_PATH", "CONCEPT_CD", "NAME_CHAR"]] =  df.loc[df["C_TABLENAME"]=="MODIFIER_DIMENSION", ["C_FULLNAME", "C_BASECODE", "C_NAME"]]

    concept_df.fillna("").to_csv(folder_path+"CONCEPT_DIMENSION")
    modifier_df.fillna("").to_csv(folder_path+"MODIFIER_DIMENSION")

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

def gen_table_access(path=os.path.dirname(METADATA_PATH)+"TABLE_ACCESS"):
    table_access = pd.DataFrame(columns=["C_TABLE_CD", "C_TABLE_NAME", "C_PROTECTED_ACCESS", "C_HLEVEL", "C_FULLNAME", "C_NAME", "C_SYNONYM_CD", "C_VISUALATTRIBUTES", "C_TOTALNUM", "C_BASECODE", "C_METADATAXML", "C_FACTTABLECOLUMN", "C_DIMTABLENAME", "C_COLUMNNAME", "C_COLUMNDATATYPE", "C_OPERATOR", "C_DIMCODE", "C_COMMENT", "C_TOOLTIP", "C_ENTRY_DATE", "C_CHANGE_DATE", "C_STATUS_CD", "VALUETYPE_CD"])
    table_access = table_access.append({"C_TABLE_CD":"test" ,"C_TABLE_NAME":"test" ,"C_PROTECTED_ACCESS":"N","C_HLEVEL":0, "C_FULLNAME":ROOT_PATH, "C_NAME": ONTOLOGY_NAME,"C_SYNONYM_CD":"N", "C_VISUALATTRIBUTES":"CA", "C_FACTTABLECOLUMN":"CONCEPT_CD", "C_DIMTABLENAME":"CONCEPT_DIMENSION", "C_COLUMNNAME":"CONCEPT_PATH","C_COLUMNDATATYPE":"T", "C_OPERATOR":"LIKE", "C_DIMCODE":ROOT_PATH})			
    table_access.to_csv(path)