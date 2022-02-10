import os
import sys
import pytest
import random

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath)

from initsts import *
from utils import from_csv
from data_loader import *

def test_parser():
    """
    Check the parser correctly populates a graph. 
    """
    pass

def test_entrypoints():
    pass

