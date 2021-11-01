import os
import sys

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + "/../src/")
from rdfwrappers import *

ONTOLOGY_GRAPH = rdflib.Graph()
ONTOLOGY_GRAPH.parse(ONTOLOGY_GRAPH_LOCATION, format="turtle")
for file in os.listdir(TERMINOLOGIES_LOCATION):
    print("Adding " + file + " to the graph, from i2b2 wrappers test file")
    if "snomed" in file or "loinc" in file:
        continue
    ONTOLOGY_GRAPH.parse(TERMINOLOGIES_LOCATION + file, format="turtle")
RDFS = ""
RDF = ""
SPHN = ""
ns = [e for e in ONTOLOGY_GRAPH.namespace_manager.namespaces()]
for tupp in ns:
    key, val = tupp
    globals()[key.upper()] = rdflib.Namespace(val)


def give_entry_concepts():
    return [ONTOLOGY_GRAPH.resource(e) for e in ENTRY_CONCEPTS]


TEST_URI = SPHN.FOPHDiagnosis
CONCEPT_LIST = [Concept(k) for k in give_entry_concepts()]
