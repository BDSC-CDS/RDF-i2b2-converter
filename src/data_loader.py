from rdf_base import *
from i2b2wrappers import I2B2BasecodeHandler


class DataLoader:
    """
    Manage data conversion from an observation graph. 
    Observations register is performed by batches defined by the type (class) of observations.
    """
    def __init__(self, class_resources, filename, reset_file=True):
        """
        Take a list of class resources.
        """
        self.graph = class_resources[0].graph
        self.class_resources = class_resources
        self.filename = filename
        self.init = reset_file

    def write_db(self):
        """
        Trigger sequential writing of batched observation database lines.
        """
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
        Iterate through the batch instances and launch exploration of the RDF observation graph.
        Return the database lines for the current batch.
        """
        database_batch = []
        observations = self.get_next_class_instances()
        information_tree = InformationTree(observations)
        database_batch = information_tree.get_info_dics()
        return database_batch

    def get_next_class_instances(self):
        """
        Extract the next observations batch from the RDF graph.
        """
        if self.class_resources==[]:
            return []
        cur = self.class_resources.pop()
        obs = self.graph.query("""
                select ?obs
                where {
                    ?obs rdf:type ?class
                }
            """, initBindings={"class":cur.identifier})
    
class ObservationRegister:
    """
    Create a proper database line as dict and manages the overwritings of local dict (typically extracted from a FeatureExtractor with other dicts.
    """
    def __init__(self):
        self.default = {
            "ENCOUNTER_NUM": "", "PATIENT_NUM": "", "CONCEPT_CD": "", "PROVIDER_ID": "", "START_DATE": "", "MODIFIER_CD": "", "INSTANCE_NUM": "", "VALTYPE_CD": "", "TVAL_CHAR": "", "NVAL_NUM": "", "VALUEFLAG_CD": "", "QUANTITY_NUM": "", "UNITS_CD": "", "END_DATE": "", "LOCATION_CD": "", "OBSERVATION_BLOB": "", "CONFIDENCE_NUM": "", "UPDATE_DATE": "", "DOWNLOAD_DATE": "", "IMPORT_DATE": "", "SOURCESYSTEM_CD": "", "UPLOAD_ID": "", "TEXT_SEARCH_INDEX": ""
        }

    def merge_dics(self, local_info, weaker_info, stronger_info):
        base_dic = self.default.copy()
        agnostic_dic = base_dic.update(local_info)
        fmerge = weaker_info.copy().update(agnostic_dic)
        smerge = fmerge.update(stronger_info)
        return smerge

class FeatureExtractor:
    """
    Extract the relevant information from a RDF resource regarding a set of predefined features.
    """
    def __init__(self, resource):
        pass
    def extract_features(self):
        pass

class InformationTree:
    """
    This class is in charge of the graph exploration given a set of entrypoints. It will trigger the collection of observation informations.
    The tool classes ObservationRegister and FeatureExtractor are called from there.
    """
    def __init__(self, resources_list):
        self.observations = resources_list

    def get_info_dics(self):
        pass

    def go_fish(self):
        for obs in observations:
            extractor = FeatureExtractor(obs)
            info = ObservationRegister(obs) #TODO: rewrite all this block
            info.merge_dics(extractor.extract_features())
            children_info = self.explore_subtree(obs, upper_info = info)
            return [info]+children_info

    def explore_subtree(self, resource, upper_info):
        for pred, obj in resource.predicate_objects():
            pass

# TODO: fill the dicts. fill the other dimensions. write unit tests.