from os import write
from rdflib.graph import ModificationException
from rdf_base import *
from i2b2wrappers import I2B2BasecodeHandler


class DataLoader:
    def __init__(self, class_resources, filename=OUTPUT_TABLES+"OBSERVATION_FACT"):
        """
        Take a list of class resources.
        """
        self.graph = class_resources[0].graph
        self.class_resources = class_resources
        self.filename = filename
        self.init = True

    def write_db(self):
        nonempty = True
        counter=0
        while nonempty:
            nonempty = self.write_batch()
            counter = counter+1
        print("Batches written: "+str(counter))

    def write_batch(self):
        """
        Trigger data conversion and write the db lines in the csv file for the current batch.
        """
        db = self.convert_data()
        mode = "w" if self.init else "a"
        res = db_to_csv(db, self.filename, mode=mode)
        self.init = False
        return res!=[]

    def convert_data(self):
        """
        Operate the batch retrieval and observation lines
        """
        database_batch = []
        observations = self.get_next_class_instances()
        for obs in observations:
            info = ObservationRegister(obs)
            children_info = self.explore_subtree(obs, upper_info = info)
            database_batch.append([info]+children_info)
        return database_batch


    def get_next_class_instances(self):
        if self.class_resources==[]:
            return []
        cur = self.class_resources.pop()
        obs = self.graph.query("""
                select ?obs
                where {
                    ?obs rdf:type ?class
                }
            """, initBindings={"class":cur.identifier})
    
    def explore_subtree(self, resource, upper_info):

        for pred, obj in resource.predicate_objects():
            pass



class ObservationRegister:
    def __init__(self):
        self.default = {
            "ENCOUNTER_NUM": "", "PATIENT_NUM": "", "CONCEPT_CD": "", "PROVIDER_ID": "", "START_DATE": "", "MODIFIER_CD": "", "INSTANCE_NUM": "", "VALTYPE_CD": "", "TVAL_CHAR": "", "NVAL_NUM": "", "VALUEFLAG_CD": "", "QUANTITY_NUM": "", "UNITS_CD": "", "END_DATE": "", "LOCATION_CD": "", "OBSERVATION_BLOB": "", "CONFIDENCE_NUM": "", "UPDATE_DATE": "", "DOWNLOAD_DATE": "", "IMPORT_DATE": "", "SOURCESYSTEM_CD": "", "UPLOAD_ID": "", "TEXT_SEARCH_INDEX": ""
        }

    def fill_info(self, ):
        return self.default.copy().update()