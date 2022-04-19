from i2b2wrappers import *

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath)
OBS_TABLE = myPath+"/../files/output_tables/OBSERVATION_FACT.csv"

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

def gen_patient_dim():
    """
    Gather the minimal information for the patient dimension table and write it.
    If more details are needed, this should also query the RDF graph about patient information and store the details in the same table.

    The patient numbers are the ones AFTER reindexing.
    """
    df = pd.read_csv(OBS_TABLE)
    patdf = pd.DataFrame(columns=COLUMNS["PATIENT_DIMENSION"])
    patdf["PATIENT_NUM"] = df["PATIENT_NUM"]
    patdf.to_csv(OUTPUT_TABLES+"PATIENT_DIMENSION.csv", index=False)    

def gen_patient_mapping(lookup):
    """
    Replace the non-integer (or too large) fields of "PATIENT_NUM" (both in observation and patient_dimension) by an integer. Store the index mapping in a dedicated table.
    """
    pm = lookup.dropna(subset="PATIENT_NUM")
    pmdf = pd.DataFrame(columns=COLUMNS["PATIENT_MAPPING"])
    pmdf["PATIENT_NUM"] = pm["PATIENT_NUM"].index.values
    pmdf["PATIENT_IDE"] = pm["PATIENT_NUM"]
    pmdf["PATIENT_IDE_SOURCE"] = pm["ENCOUNTER_NUM"]
    pmdf.to_csv(OUTPUT_TABLES+"PATIENT_MAPPING.csv", index=False) 

def gen_visit_dim():
    """
    TODO: currently incorrect table since only one encounter with num -1 is kept... Fix this in the obs_tools file and remove the drop_duplicates call here.
    Query the RDF graph about encounter information and store the details in a dedicated table.
    """
    df = pd.read_csv(OBS_TABLE).drop_duplicates(subset="ENCOUNTER_NUM", keep="first")
    encdf = pd.DataFrame(columns=COLUMNS["VISIT_DIMENSION"])
    encdf["PATIENT_NUM"] = df["PATIENT_NUM"]
    encdf["ENCOUNTER_NUM"] = df["ENCOUNTER_NUM"]
    encdf.to_csv(OUTPUT_TABLES+"VISIT_DIMENSION.csv", index=False)    


def gen_encounter_mapping(lookup):
    em = lookup.dropna(subset="ENCOUNTER_NUM")
    emdf = pd.DataFrame(columns=COLUMNS["ENCOUNTER_MAPPING"])
    emdf["ENCOUNTER_NUM"] = em["ENCOUNTER_NUM"].index.values
    emdf["ENCOUNTER_IDE"] = em["ENCOUNTER_NUM"]
    emdf["ENCOUNTER_IDE_SOURCE"] = em["ENCOUNTER_NUM"]
    emdf.to_csv(OUTPUT_TABLES+"ENCOUNTER_MAPPING.csv", index=False)   


def gen_provider_dim(graph_parser):
    provider_class = rdflib.URIRef(ONTOLOGY_DROP_DIC["PROVIDER_INFO"])
    graph = graph_parser.graph
    res = graph.query("""
        SELECT ?c ?n
        where {
            ?k rdf:type ?dpiclass .
            ?k ?_ ?s .
            ?s ?codepred ?c .
            ?s ?codeid ?n
        }
        """, initBindings={"dpiclass":provider_class, 
            "codepred":rdflib.URIRef("https://biomedit.ch/rdf/sphn-ontology/sphn#hasCodeName"), 
            "codeid":rdflib.URIRef("https://biomedit.ch/rdf/sphn-ontology/sphn#hasIdentifier")})
    pdb.set_trace()

def fill_star_schema(mappings=None, graph_parser=None):
    """
    Generate the observation-based star schema tables. 
    If a mapping is passed as parameter, generate also the encouter_mapping and patient_mapping tables.
    """
    gen_visit_dim()
    gen_patient_dim()
    gen_provider_dim(graph_parser)    

    if mappings is not None:
        gen_encounter_mapping(mappings)
        gen_patient_mapping(mappings)

def gen_table_access(folder_path=OUTPUT_TABLES, metadata_filenames=["METADATA.csv"]):
    dfs=[pd.read_csv(folder_path + fname) for fname in metadata_filenames]
    df = pd.concat(dfs) if len(dfs)>1 else dfs[0]
    table_access = pd.DataFrame(columns=COLUMNS["TABLE_ACCESS"])
    inter = table_access.columns.intersection(df.columns)
    table_access[inter] = df.loc[(df["C_HLEVEL"]==0) & (df["C_FACTTABLECOLUMN"]=="CONCEPT_CD"), inter]
    table_access["C_TABLE_CD"]=table_access["C_NAME"]
    table_access["C_TABLE_NAME"]="sphn"
    table_access.fillna("").to_csv(folder_path+"TABLE_ACCESS.csv", index=False)
