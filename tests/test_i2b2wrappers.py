import os
import sys
import pytest
import random

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath)

from initsts import *
from utils import from_csv
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
    root_rdfconcept = Concept(
        ONTOLOGY_GRAPH.resource(
            "https://biomedit.ch/rdf/sphn-ontology/sphn#SPHNConcept"
        )
    )
    converter = I2B2Converter(root_rdfconcept)
    all_concepts = converter.i2b2concepts
    assert len(all_concepts)>0 and not all([conc.level == all_concepts[0].level for conc in all_concepts])


def test_modifiers():
    prop = construct_property(
        "https://biomedit.ch/rdf/sphn-ontology/sphn#hasInhaledOxygenConcentrationDrugAdministrationEvent"
    )
    conc = I2B2Concept(Concept(ONTOLOGY_GRAPH.resource(TEST_URI)))
    i2b2mod = I2B2Modifier(prop[0], parent=conc, applied_path=conc.path)
    modlist = i2b2mod.walk_mtree()
    assert len(modlist) > 1 and i2b2mod.visual=="RA" and all([k.visual=="RA" for k in modlist if "VALUETYPE_CD" in k.line_updates.keys()])


def test_i2b2ontelem():
    inter_conc = CONCEPT_LIST[0]
    test_c = I2B2Concept(inter_conc, parent=None)
    test_c.extract_modelems()
    mod_mod = test_c.modifiers
    assert all([type(k)==I2B2Modifier for k in mod_mod])


def test_interface():

    # Step 1: generate python objects from the entry concept list specified in the config file
    entry_concepts = CONCEPT_LIST
    # Step 2: For each concept, create an I2B2 converter and extract info from it.
    # This might be suboptimal in terms of memory usage when an entry concept is in fact a directory having a lot of subconcepts
    for concept_i in entry_concepts:
        concept = Concept(concept_i.resource)

        # Initialize the converter using the list of objects
        converter = I2B2Converter(concept)
        # Get the i2b2 db lines related to this concept
        buffer = converter.get_batch()
        while buffer:
            converter.write(METADATA_PATH)


def test_basecode():
    prop = construct_property(
        "https://biomedit.ch/rdf/sphn-ontology/sphn#hasFOPHDiagnosisCode"
    )
    conc = I2B2Concept(Concept(ONTOLOGY_GRAPH.resource(TEST_URI)))
    i2b2mod = I2B2Modifier(prop[0], parent=conc, applied_path=conc.path)
    modlist = i2b2mod.walk_mtree()
    assert [len(k.code) == 50 for k in modlist]

def test_e2e():
    pass

def test_duplicate_paths():
    """
    Check all paths and basecodes are unique.
    """
    col_names = ["c_fullname", "c_basecode"]
    db = from_csv(METADATA_PATH, usecols=col_names)
    assert not any([len(set(db[key])) < len(db[key]) for key in col_names])


def test_levels():
    """
    Check ontology elements all have exactly one parent in the tree, and their levels are consistent.
    """
    df = pd.read_csv(METADATA_PATH, usecols=["C_HLEVEL", "C_FULLNAME"])
    samples = df.samples(n=10)
    res=[]
    for row in samples:
        if row["C_HLEVEL"]>0:
            parent_path = row["C_FULLNAME"].rfind("\\", 0, -1)
            parlev = df.loc[df["C_FULLNAME"]==parent_path]
            if len(parlev) !=1:
                res.append(False)
            else:
                res.append(parlev["C_HLEVEL"]==row["C_HLEVEL"]-1)
    assert all(res)