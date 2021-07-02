import os
import sys
import pytest
import random

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + "/../src/")
from rdf_base import *

ONTOLOGY_GRAPH = rdflib.Graph()
ONTOLOGY_GRAPH.parse(ONTOLOGY_GRAPH_LOCATION, format="turtle")
for file in os.listdir(TERMINOLOGIES_LOCATION):
    if "snomed" in file:
        continue
    print("Adding "+file+" to the graph")
    ONTOLOGY_GRAPH.parse(TERMINOLOGIES_LOCATION+file, format="turtle")
pdb.set_trace()

def give_entry_concepts():
    return [ONTOLOGY_GRAPH.resource(e) for e in ENTRY_CONCEPTS]

CONCEPT_LIST= give_entry_concepts()

def test_list_properties():

    example_concept = Concept(random.choice(CONCEPT_LIST))
    properties = example_concept.list_unique_properties()
    graph = example_concept.component.resource.graph

    # Other way around:
    other_props = example_concept.component.resource.subjects(RDFS.domain)
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
            
def test_unique_properties_specific():
    pass

def test_explore_children():
    concept = Concept(CONCEPT_LIST[0])
    concept.explore_children()
    pdb.set_trace()
    return concept


def test_extract_range_type_bnode():
    res = ONTOLOGY_GRAPH.resource(rdflib.URIRef("https://biomedit.ch/rdf/sphn-ontology/sphn#hasCareHandlingTypeCode"))
    pro = Property(res)
    rnges = pro.extract_range_type()
    assert len(rnges)>1


def test_extract_range_type_plain():
    res = ONTOLOGY_GRAPH.resource(rdflib.URIRef("https://biomedit.ch/rdf/sphn-ontology/sphn#hasBiosample"))
    pro = Property(res)
    rnges = pro.extract_range_type()
    assert len(rnges)==1


def test_mute_sameterminology():
    res1 = ONTOLOGY_GRAPH.resource(rdflib.URIRef("https://biomedit.ch/rdf/sphn-ontology/sphn#hasAdministrativeGenderCode")
    res2 = ONTOLOGY_GRAPH.resource(rdflib.URIRef("https://biomedit.ch/rdf/sphn-ontology/sphn#hasDiagnosticRadiologicExaminationCode")
    prop1 = Property(res1)
    prop2 = Property(res2)
    filter_d = PropertyFilter(None)
    filter_d.filter_properties([prop1, prop2])
    assert all(rnn.subconcepts == [] for rnn in prop1.ranges+prop2.ranges)

def test_nomute_diffterminologies():
    "https://biomedit.ch/rdf/sphn-ontology/sphn#hasSubstanceCode"

def test_mute_sameterm_differentfiles():
    # use <http://snomed.info/id/105590001> (extracted from the sphn ttl) vs any other snomed node from the snomed file
    pass
