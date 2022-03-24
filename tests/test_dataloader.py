import os
import sys
import pytest
import random
import string

herePath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, herePath)

from initsts import *
from utils import from_csv
from data_loader import *

TEST_CLASS_URI = "https://biomedit.ch/rdf/sphn-ontology/sphn#LabResult"

TEST_INDIVIDUALS = [""]
TEST_TERMINOLOGY_END = ["https://biomedit.ch/rdf/sphn-resource/" + k for k in 
    ["Code-LOINC-14732-2", "Code-LOINC-14933-6", "Code-SNOMED-CT-703118005", "Code-SNOMED-CT-125681006", "Code-ICD-10-G55.0"] ]
TEST_NONENDS_OBJECTS =["https://biomedit.ch/rdf/sphn-resource/" + k for k in 
    []]
TEST_PREDICATES=[]


class test_dataloaderclass:
    def __init__(self):
        self.entry_points=[]
        self.db = []
        self.parser = GraphParser(paths=[DATA_GRAPHS_LOCATION])
        self.parser.define_namespaces()
        
    def give_shuffled_dataloader(self):
        entry_points = self.parser.get_entrypoints(ENTRY_DATA_CONCEPTS)
        dl = DataLoader(self.parser, entry_points)
        dl.entry_class_resources=random.shuffle(dl.entry_class_resources)
        self.dataloader = dl
        self.entry_points=entry_points

    def test_parser(self):
        """
        Check the parser correctly populates a graph. 
        """
        assert len(self.parser.graph)>0

    def test_entrypoints(self):
        entryclasses = self.parser.get_entrypoints(ENTRY_DATA_CONCEPTS)
        assert len(entryclasses)>0

    def test_loaderobject(self):
        dl = self.dataloader
        fi=len(dl.entry_class_resources)
        first_inss = dl.get_next_class_instances()
        self.instances = first_inss
        found=len(first_inss)
        if found:
            print(str(found)+" instances found for class" + first_inss[0].value(TYPE_PREDICATE_URI))
        assert len(dl.entry_class_resources) == fi-1

    def test_informationtree(self):
        self.itree = InformationTree(self.instances)
        self.itree.explore_tree_master()
        recs = self.itree.main_register.records
        assert len(recs)>0
    

    def test_context_register(self):
        pass

    def test_obsregister(self):
        pass

    def test_ispathend_ends():
        endtree = InformationTree(TEST_TERMINOLOGY_END+TEST_XSD_ENDS)
        assert all([endtree.is_pathend(k) for k in endtree.observations])

    def test_ispathend_individuals():
        indtree = InformationTree(TEST_INDIVIDUALS)
        assert all([indtree.is_pathend(k) for k in indtree.observations])

    def test_ispathend_nonends():
        nonendtree = InformationTree(TEST_NONENDS_OBJECTS)
        assert all([not nonendtree.is_pathend(k) for k in nonendtree.observations])

    def test_valuereplacement():
        pass
