from utils import *

class Component:
    def __init__(self, resource):
        self.resource = resource
    self.path = None
    self.parent = None

class Concept(Component):
    pass

class Property(Component):    
    pass

class I2B2Concept(Concept):
    self.basecode = None

class I2B2Modifier(Property):
    self.applied_concept = None
    self.basecode = None
    
