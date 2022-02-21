from importlib.metadata import entry_points
import os
import sys
import pytest
import random

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath)

from initsts import *
from utils import from_csv
from data_loader import *

TEST_CLASS_URI = "https://biomedit.ch/rdf/sphn-ontology/sphn#LabResult"

TEST_INDIVIDUALS = [""]
TEST_XSD_ENDS = [""]
TEST_TERMINOLOGY_END =[]
TEST_NONENDS_OBJECTS =[]
TEST_PREDICATES=[]

PARSER = GraphParser(paths=[DATA_GRAPHS_LOCATION])
PARSER.define_namespaces()

def give_shuffled_dataloader():
    entry_points = PARSER.get_entrypoints(ENTRY_DATA_CONCEPTS)
    dl = DataLoader(PARSER, entry_points)
    dl.entry_class_resources=random.shuffle(dl.entry_class_resources)
    return dl

def test_parser():
    """
    Check the parser correctly populates a graph. 
    """
    parser=PARSER
    assert len(parser.graph)>0

def test_entrypoints():
    entryclasses = PARSER.get_entrypoints(ENTRY_DATA_CONCEPTS)
    assert len(entryclasses)>0

def test_loaderobject():
    dl = give_shuffled_dataloader()
    fi=len(dl.entry_class_resources)
    first_inss = dl.get_next_class_instances()
    found=len(first_inss)
    if found:
        print(str(found)+" instances found for class" + first_inss[0].value(TYPE_PREDICATE_URI))
    assert len(dl.entry_class_resources) == fi-1

def test_informationtree():
    dl = give_shuffled_dataloader()
    fi=len(dl.entry_class_resources)
    first_inss = dl.get_next_class_instances(TEST_CLASS_URI)
    itree = InformationTree(first_inss)
    itree.explore_tree_master()
    recs = itree.main_register.records
    assert len(recs)>0

def test_ispathend_ends():
    endtree = InformationTree(TEST_TERMINOLOGY_END+TEST_XSD_ENDS)
    assert all([endtree.is_pathend(k) for k in endtree.observations])

def test_ispathend_individuals():
    indtree = InformationTree(TEST_INDIVIDUALS)
    assert all([indtree.is_pathend(k) for k in indtree.observations])

def test_ispathend_nonends():
    nonendtree = InformationTree(TEST_NONENDS_OBJECTS)
    assert all([not nonendtree.is_pathend(k) for k in nonendtree.observations])

