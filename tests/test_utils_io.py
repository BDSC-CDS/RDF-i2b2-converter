# pylint:disable=missing-function-docstring
"""
Test file for I/O operations and other routines defined in utils.py
"""

import unittest
from rdflib import URIRef
from tests.constant_test import MANDATORY_GRAPH_CONFIG_KEYS
from src.constant import GRAPH_CONFIG_FILE, I2B2_MAPPING_FILE
from src.utils import read_config


class IOTester(unittest.TestCase):
    """
    Test class for basic I/O operations such as:
    - parsing JSON files,
    - reading and writing CSV files.
    """

    def __init__(self, methodName="runTest"):
        super().__init__(methodName)
        self.graph_dic = read_config(GRAPH_CONFIG_FILE)
        self.i2b2_dic = read_config(I2B2_MAPPING_FILE)

    def test_read_graph_config(self):
        for valuri in self.graph_dic["uris"].values():
            with self.subTest():
                if isinstance(valuri, list):
                    for elem in valuri:
                        with self.subTest():
                            self.assertIsInstance(elem, URIRef)
                else:
                    self.assertIsInstance(valuri, URIRef)

    def test_keys_uris(self):
        for key in self.graph_dic["uris"].keys():
            with self.subTest():
                self.assertIn(key, MANDATORY_GRAPH_CONFIG_KEYS["uris"])

    def test_keys_params(self):
        for key in self.graph_dic["parameters"].keys():
            with self.subTest():
                self.assertIn(key, MANDATORY_GRAPH_CONFIG_KEYS["parameters"])

    def test_wipe(self):
        pass

    def test_to_csv(self):
        pass

    def test_from_csv(self):
        pass


# class GraphTester:
#     """
#     Test class for basic graph operations such as:
#     - GraphParser methods
#     - Loading graph in memory
#     - Deleting graph
#     - Detecting terminology elements
#     - etc
#     """

#     def __init__(self) -> None:
#         pass
#     def test_free_main_graph(self):
#         """
#         Check the memory for the main ontology graphs is correctly freed.
#         """
#         pass

#     def test_free_terminologies(self):
#         """
#         Check the memory for terminologies graphs is correctly freed.
#         """
#         pass


# class CSVPayloadTester:
#     """
#     Test class for i2b2-specific content editing operations such as:
#     - Elements merging
#     - XML field editing
#     -
#     """

#     def __init__(self) -> None:
#         pass
#     def test_units_injection():
#         pass
#     def


# class BasecodeTester:
#     """
#     Test class for the I2B2Basecode methods
#     """

#     def __init__(self) -> None:
#         pass

# class
