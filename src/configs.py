import json

(
    ONTOLOGY_GRAPH_LOCATION,
    TERMINOLOGIES_LOCATION,
    ONTOLOGY_NAME,
    DATA_GRAPHS_LOCATION,
    OUTPUT_TABLES,
    USE_DUMMY_DATESt,
    ALWAYS_DEEP,
    RDF_FORMAT,
    PREF_LANGUAGE,
    ROOT_URI,
    PROJECT_RDF_NAMESPACE,
    SUBCLASS_PRED_URI,
    ENTRY_CONCEPTS,
    BLACKLIST,
    TERMINOLOGY_MARKER_URI,
    VALUESET_MARKER_URI,
) = [
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
]
(
    ENCOUNTER_NUM,
    OBSERVATION_PRED,
    DATE_DESCRIPTOR,
    UNIT_DESCRIPTOR,
    MAX_BASECODE_LENGTH,
    START_SEARCH_INDEX,
    DEFAULT_DATE,
    ROOT_PATH,
    UNITS,
    METADATA_PATH,
) = [None, None, None, None, None, None, None, None, None, None]
ONTOLOGY_DROP_DIC = {}
MERGE_DIC = {}
with open("files/ontology_config.json") as ff:
    config = json.load(ff)
for key, val in config["parameters"].items():
    globals()[key] = val
for key, val in config["uris"].items():
    globals()[key] = val

with open("files/i2b2_rdf_mapping.json") as ff:
    config = json.load(ff)
for key, val in config.items():
    globals()[key] = val
