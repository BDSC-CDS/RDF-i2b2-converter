import os
import sys
import pytest
import random

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath)

from initsts import *
from i2b2wrappers import *



def test_converterclass():
    root_concept = Concept(
        ONTOLOGY_GRAPH.resource(
            "https://biomedit.ch/rdf/sphn-ontology/sphn#SPHNConcept"
        )
    )
    converter = I2B2Converter(root_concept)
    all_concepts = converter.i2b2concepts
    assert [k.modifiers == [] for k in all_concepts]
    pdb.set_trace()

def test_i2b2ontelem():
    inter_conc = Concept(CONCEPT_LIST[0])
    test_c = I2B2Concept(inter_conc, parent=None)
    modifiers = test_c.extract_modelems()
    mod_mod = [el for el in modifiers.walk_mtree()]
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
