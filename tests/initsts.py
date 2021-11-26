import os
import sys

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + "/../src/")
from rdfwrappers import *


gp = GraphParser([ONTOLOGY_GRAPH_LOCATION, TERMINOLOGIES_LOCATION])
ns = gp.define_namespaces()
ONTOLOGY_GRAPH=gp.graph
for tupp in ns:
    key, val = tupp
    globals()[key.upper()] = rdflib.Namespace(val)

def give_entry_concepts():
    return [ONTOLOGY_GRAPH.resource(e) for e in ENTRY_CONCEPTS]


TEST_URI = SPHN.FOPHDiagnosis
CONCEPT_LIST = [Concept(k) for k in give_entry_concepts()]

"""

            "https://biomedit.ch/rdf/sphn-ontology/sphn#LabResult",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#DrugAdministrationEvent",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#DrugPrescription",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#Allergy",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#Biobanksample",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#ICDODiagnosis",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#NursingDiagnosis",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#OncologyTreatmentAssessment",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#ProblemCondition",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#RadiotherapyProcedure",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#TNMClassification",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#TumorGrade",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#TumorStage",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#AdverseEvent",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#AllergyEpisode",
            "https://biomedit.ch/rdf/sphn-ontology/sphn#TumorSpecimen",
            """