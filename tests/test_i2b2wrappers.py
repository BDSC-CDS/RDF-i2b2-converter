import os
import sys
import pytest
import random

from utils import from_csv

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath)

from initsts import *
from i2b2wrappers import *

global_db = []

def construct_property(uri):
    pf = PropertyFilter(None)
    pf.resources = [ONTOLOGY_GRAPH.resource(rdflib.URIRef(uri))]
    properties = pf.get_properties()
    for k in properties:
        k.digin_ranges()
    return properties

def test_converterclass():
    root_concept = Concept(
        ONTOLOGY_GRAPH.resource(
            "https://biomedit.ch/rdf/sphn-ontology/sphn#SPHNConcept"
        )
    )
    converter = I2B2Converter(root_concept)
    all_concepts = converter.i2b2concepts
    assert [k.modifiers == [] for k in all_concepts]

def test_modifiers():
    prop = construct_property("https://biomedit.ch/rdf/sphn-ontology/sphn#hasInhaledOxygenConcentrationDrugAdministrationEvent")
    i2b2mod = I2B2Modifier(prop[0], parent=None, applied_path=None)
    modlist = i2b2mod.walk_mtree()
    assert len(modlist)>1

def test_i2b2ontelem():
    inter_conc = CONCEPT_LIST[0]
    test_c = I2B2Concept(inter_conc, parent=None)
    test_c.extract_modelems()
    mod_mod = test_c.modifiers
    pdb.set_trace()


def test_interface():

    # Step 1: generate python objects from the entry concept list specified in the config file
    entry_concept_resources = CONCEPT_LIST

    # Step 2: For each concept, create an I2B2 converter and extract info from it.
    # This might be suboptimal in terms of memory usage when an entry concept is in fact a directory having a lot of subconcepts
    for concept_res in entry_concept_resources:
        concept = Concept(concept_res)
        concept.explore_subclasses()

        # Initialize the converter using the list of objects
        converter = I2B2Converter(concept)
        # Get the i2b2 db lines related to this concept
        buffer = converter.get_batch()
        while buffer:
            converter.write(METADATA_PATH)

def test_duplicate_paths():
    """
    Check all paths and basecodes are unique.
    """
    col_names=["c_fullname", "c_basecode"]
    db = from_csv(METADATA_PATH, usecols=col_names)
    if len(db)>0:
        assert not any([len(set(db[key]))<len(db[key]) for key in col_names])






