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


PARSER = GraphParser(paths=[DATA_GRAPHS_LOCATION])
PARSER.define_namespaces()

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
    entry_points = PARSER.get_entrypoints(ENTRY_DATA_CONCEPTS)
    dl = DataLoader(PARSER, entry_points)
    dl.entry_class_resources=random.shuffle(dl.entry_class_resources)
    fi=len(dl.entry_class_resources)
    first_inss = dl.get_next_class_instances()
    found=len(first_inss)
    if found:
        print(str(found)+" instances found for class" + first_inss[0].value(RDF.type))
    assert len(dl.entry_class_resources) == fi-1

def test_