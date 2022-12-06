from i2b2wrappers import *
from data_loader import *

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath)
OBS_TABLE = OUTPUT_TABLES_LOCATION + "OBSERVATION_FACT.csv"


def gen_concept_modifier_dim(
    folder_path=OUTPUT_TABLES_LOCATION, metadata_filename="METADATA.csv"
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
    suffix = "_VERBOSE" if DEBUG else ""
    concept_df.fillna("").to_csv(
        folder_path + "CONCEPT_DIMENSION" + suffix + ".csv", index=False
    )
    modifier_df.fillna("").to_csv(
        folder_path + "MODIFIER_DIMENSION" + suffix + ".csv", index=False
    )


def init_patient_dim():
    """
    Initialize PATIENT_DIMENSION.
    """
    patdf = pd.DataFrame(columns=COLUMNS["PATIENT_DIMENSION"])
    patdf.to_csv(OUTPUT_TABLES_LOCATION + "PATIENT_DIMENSION.csv", index=False)


def init_patient_mapping():
    """
    Initialize PATIENT_MAPPING
    """
    pmdf = pd.DataFrame(columns=COLUMNS["PATIENT_MAPPING"])
    pmdf.to_csv(OUTPUT_TABLES_LOCATION + "PATIENT_MAPPING.csv", index=False)


def init_visit_dim():
    """
    Initialize VISIT_DIMENSION.
    """
    encdf = pd.DataFrame(columns=COLUMNS["VISIT_DIMENSION"])
    encdf.to_csv(OUTPUT_TABLES_LOCATION + "VISIT_DIMENSION.csv", index=False)


def init_encounter_mapping():
    emdf = pd.DataFrame(columns=COLUMNS["ENCOUNTER_MAPPING"])
    emdf.to_csv(OUTPUT_TABLES_LOCATION + "ENCOUNTER_MAPPING.csv", index=False)


def query_providers(graph_parser):
    provider_class = PROVIDER_CLASS_URI
    graph = graph_parser.graph
    res = graph.query(
        """
        SELECT ?c ?n
        where {
            ?k rdf:type ?dpiclass .
            ?k ?_ ?s .
            ?s ?codepred ?c .
            ?s ?codeid ?n
        }
        """,
        initBindings={
            "dpiclass": provider_class,
            "codepred": rdflib.URIRef(
                COLUMNS_MAPPING["CONTEXT"][provider_class.toPython()]["verbose_value"][
                    -1
                ]
            ),
            "codeid": rdflib.URIRef(
                COLUMNS_MAPPING["CONTEXT"][provider_class.toPython()]["pred_to_value"][
                    -1
                ]
            ),
        },
    )
    return [el for el in res]


def gen_provider_dim(providers_sparqlres):
    res = providers_sparqlres
    prov_df = pd.DataFrame(columns=COLUMNS["PROVIDER_DIMENSION"])
    kdic = {"PROVIDER_ID": [], "PROVIDER_PATH": []}
    for el in res:
        kdic["PROVIDER_ID"].append(el[1].toPython())
        kdic["PROVIDER_PATH"].append(el[0].toPython())
    pdf = pd.DataFrame.from_dict(kdic)
    prov_df = pd.concat([prov_df, pdf], axis=0)
    prov_df.to_csv(OUTPUT_TABLES_LOCATION + "PROVIDER_DIMENSION.csv", index=False)


def init_star_schema(providers=None):
    """
    Generate the observation-based star schema tables.
    If a mapping is passed as parameter, generate also the encouter_mapping and patient_mapping tables.
    """
    init_visit_dim()
    init_patient_dim()
    gen_provider_dim(providers)
    init_encounter_mapping()
    init_patient_mapping()


def gen_table_access(
    folder_path=OUTPUT_TABLES_LOCATION, metadata_filenames=["METADATA.csv"]
):
    dfs = [pd.read_csv(folder_path + fname) for fname in metadata_filenames]
    df = pd.concat(dfs) if len(dfs) > 1 else dfs[0]
    table_access = pd.DataFrame(columns=COLUMNS["TABLE_ACCESS"])
    inter = table_access.columns.intersection(df.columns)
    table_access[inter] = df.loc[
        (df["C_HLEVEL"] == 0) & (df["C_FACTTABLECOLUMN"] == "CONCEPT_CD"), inter
    ]
    table_access["C_TABLE_CD"] = "sphn"
    table_access["C_TABLE_NAME"] = "sphn"
    table_access["C_DIMTABLENAME"] = "CONCEPT_DIMENSION"
    table_access["C_PROTECTED_ACCESS"] = "N"

    table_access.fillna("").to_csv(folder_path + "TABLE_ACCESS.csv", index=False)
