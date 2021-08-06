import os
import sys
import pytest
import random

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + "/../src/")
from rdfwrappers import *

ONTOLOGY_GRAPH = rdflib.Graph()
ONTOLOGY_GRAPH.parse(ONTOLOGY_GRAPH_LOCATION, format="turtle")
for file in os.listdir(TERMINOLOGIES_LOCATION):
    print("Adding " + file + " to the graph")
    if "snomed" in file:
        continue
    ONTOLOGY_GRAPH.parse(TERMINOLOGIES_LOCATION + file, format="turtle")
ns = [e for e in ONTOLOGY_GRAPH.namespace_manager.namespaces()]
for tupp in ns:
    key, val = tupp
    globals()[key.upper()] = rdflib.Namespace(val)

def give_entry_concepts():
    return [ONTOLOGY_GRAPH.resource(e) for e in ENTRY_CONCEPTS]

TEST_URI = SPHN.FOPHDiagnosis
CONCEPT_LIST = give_entry_concepts()

def list_sparql_bnode_domains(uri=TEST_URI):
    """
    This should return all predicates for which the requested uri is a domain listed among other domains (in a blank node bag using owl:unionOf) 
    """
    resp=ONTOLOGY_GRAPH.query(
        """
        SELECT ?p WHERE
                {
                ?p rdfs:domain [ a owl:Class ;
                                    owl:unionOf [ rdf:rest*/rdf:first ?self ]
                                    ]
                }
        """, initBindings={"self":uri}
    )
    rows = [ONTOLOGY_GRAPH.resource(e[0]) for e in resp]
    return rows

def test_list_properties():

    example_concept = Concept(random.choice(CONCEPT_LIST))
    fil = PropertyFilter(example_concept)
    fil.fetch_unique_properties()
    properties = fil.resources
    graph = example_concept.resource.graph

    # Other way around:
    other_props = [e for e in example_concept.resource.subjects(RDFS.domain)]+list_sparql_bnode_domains(example_concept.resource.identifier)
    clean_props = []
    for candidate in other_props:
        add = True
        for child in other_props:
            if (child.identifier, RDFS.subPropertyOf * rdflib.paths.OneOrMore, candidate.identifier) in graph:
                add = False
                break
        if add:
            # No children found for this candidate so it's the most specific
            clean_props.append(candidate)
    assert len(clean_props) == len(properties)


def test_unique_properties_specific():
    """
    Check if a concept has a "Code" property and a "XX Code" property (descendant of Code), only the latter is written in the concept class attribute.
    """
    res1 = ONTOLOGY_GRAPH.resource(
        "https://biomedit.ch/rdf/sphn-ontology/sphn#FOPHDiagnosis"
    )
    test_concept = Concept(res1)
    test_concept.explore_children()
    assert len(test_concept.properties) == 6


def test_explore_children():
    concept = Concept(CONCEPT_LIST[0])
    concept.explore_children()
    assert concept.subconcepts != [] or concept.properties != []


def test_extract_range_type_bnode():
    res = ONTOLOGY_GRAPH.resource(
        rdflib.URIRef(
            "https://biomedit.ch/rdf/sphn-ontology/sphn#hasCareHandlingTypeCode"
        )
    )
    handler = RangeFilter(res)
    reachable = handler.extract_range_type()
    assert len(reachable) > 1


def test_extract_range_type_plain():
    res = ONTOLOGY_GRAPH.resource(
        rdflib.URIRef("https://biomedit.ch/rdf/sphn-ontology/sphn#hasBiosample")
    )
    pro = RangeFilter(res)
    rnges = pro.extract_range_type()
    assert len(rnges) == 1


def nonblrng_props(reslist):
    """
    Return a list of instantiated Resources with their ranges filtered as non-blacklisted, from a list of resource uris
    """
    prop = PropertyFilter(None)
    prop.resources = reslist
    ranges = prop.filter_ranges()
    if len(ranges) != len(self.resources):
        raise Exception("Bad property-range matching")
    return [Resource(self.resources[i], ranges[i]) for i in range(self.resources)]


def test_mute_sameterminology():
    res1 = ONTOLOGY_GRAPH.resource(
        rdflib.URIRef(
            "https://biomedit.ch/rdf/sphn-ontology/sphn#hasAdministrativeGenderCode"
        )
    )
    res2 = ONTOLOGY_GRAPH.resource(
        rdflib.URIRef(
            "https://biomedit.ch/rdf/sphn-ontology/sphn#hasDiagnosticRadiologicExaminationCode"
        )
    )
    props = nonblrng_props([res1, res2])
    rnns = []
    for prop in props:
        prop.explore_ranges()
        rnns.extends(prop.ranges)
    assert all([rnn.subconcepts == [] for rnn in rnns])


def test_nomute_diffterminologies():
    res1 = ONTOLOGY_GRAPH.resource(
        rdflib.URIRef("https://biomedit.ch/rdf/sphn-ontology/sphn#hasSubstanceCode")
    )
    prop1 = nonblrng_props(res1)
    prop1.explore_ranges()
    assert all([len(rnn.subconcepts) > 0 for rnn in prop1.ranges])


def test_mute_sameterm_differentfiles():
    res1 = ONTOLOGY_GRAPH.resource(rdflib.URIRef("http://snomed.info/id/105590001"))
    res2 = ONTOLOGY_GRAPH.resource(rdflib.URIRef("http://snomed.info/id/118169006"))
    prop = Property(ONTOLOGY_GRAPH.resource(RDF.toto), [res1, res2])
    prop.explore_ranges()
    assert all([len(rnn.subconcepts) == [] for rnn in prop.ranges])
    # use <http://snomed.info/id/105590001> (extracted from the sphn ttl) vs any other snomed node from the snomed file
