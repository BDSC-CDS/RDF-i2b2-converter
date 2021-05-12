import os, sys, pytest

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + "/../src/")
from ontology_converter import *
from load_data import *

ONTOLOGY_GRAPH = rdflib.Graph()
ONTOLOGY_GRAPH.parse(ONTOLOGY_GRAPH_LOCATION, format="turtle")

def test_componentclass():
    root_resource = ONTOLOGY_GRAPH.resource(ROOT_URI)
    root_component = Concept(root_resource)
