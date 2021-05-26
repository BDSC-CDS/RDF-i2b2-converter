import os
import sys
import pytest
import random

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + "/../src/")
from ontology_converter import *
from load_data import *

ONTOLOGY_GRAPH = rdflib.Graph()
ONTOLOGY_GRAPH.parse(ONTOLOGY_GRAPH_LOCATION, format="turtle")

def give_concepts():
    root_resource = ONTOLOGY_GRAPH.resource(ROOT_URI)
    root_component = Concept(root_resource)
    return root_resource.subject(RDFS.subClassOf)

CONCEPT_LIST= give_concepts()

def test_list_properties():

    example_concept = Concept(random.choice(CONCEPT_LIST))
    properties = example_concept.list_unique_properties()
    graph = example_concept.component.resource.graph

    # Other way around:
    other_props = example_concept.component.resource.subject(RDFS.domain)
    clean_props = []
    for candidate in other_props:
        add = True
        for child in other_props:
            if (child, RDFS.subPropertyOf*OneOrMore, candidate) in graph:
                add = False
                break
        if add:
            # No children found for this candidate so it's the most specific
            clean_props.append(candidate)
    assert len(clean_props) == len(properties)
            



