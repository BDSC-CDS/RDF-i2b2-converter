import json
import rdflib
import os
import sys

# TODO: remove all the initialisations to None, were there only to avoid getting red underlining in VScode
(
    ONTOLOGY_GRAPH_LOCATION,
    TERMINOLOGIES_LOCATION,
    ONTOLOGY_NAMES,
    DATA_GRAPHS_LOCATION,
    OUTPUT_TABLES,
    USE_DUMMY_DATES,
    ALWAYS_DEEP,
    RDF_FORMAT,
    PREF_LANGUAGE,
    ROOT_URIS,
    PROJECT_RDF_NAMESPACE,
    SUBCLASS_PRED_URI,
    ENTRY_CONCEPTS,
    BLACKLIST,
    TERMINOLOGY_MARKER_URI,
    VALUESET_MARKER_URI,
    TYPE_PREDICATE_URI,
    DATATYPE_PROP_URI,
    OBJECT_PROP_URI,
    COMMENT_URI,
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
    ROOT_PATHS,
    UNITS,
    METADATA_PATH,
    XML_PATTERN,
) = [None, None, None, None, None, None, None, None, None, None, None]
ONTOLOGY_DROP_DIC = {}
COLUMNS = {}
DATA_LEAVES = {}
TERMINOLOGIES_GRAPHS = {}
TERMINOLOGIES_FILES = {}
UNDROP_LEAVES = {}
EQUIVALENCES = {}
cur_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")
with open(cur_path + "files/graph_config.json") as ff:
    config = json.load(ff)
for key, val in config["parameters"].items():
    globals()[key] = val
for key, val in config["uris"].items():
    globals()[key] = (
        rdflib.URIRef(val) if type(val) == str else [rdflib.URIRef(k) for k in val]
    )

with open(cur_path + "files/i2b2_rdf_mapping.json") as ff:
    config = json.load(ff)
for key, val in config.items():
    globals()[key] = val
