from rdf_base import *
from i2b2wrappers import I2B2BasecodeHandler

cur_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")
with open(cur_path + "files/data_loader_config.json") as ff:
    config = json.load(ff)
for key, val in config["TO_IGNORE"].items():
    globals()[key] = (
        rdflib.URIRef(val) if type(val) == str else [rdflib.URIRef(k) for k in val]
    )


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
        counter = 0
        while nonempty:
            nonempty = self.write_batch()
            counter = counter + 1
        print("Batches written: " + str(counter))

    def write_batch(self):
        """
        Trigger data conversion and write the db lines in the csv file for the current batch.
        """
        db = self.convert_data()
        mode = "w" if self.init else "a"
        res = db_to_csv(db, self.filename, mode=mode)
        self.init = False
        return res != []

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
        if self.class_resources == []:
            return []
        cur = self.class_resources.pop()
        obs = self.graph.query(
            """
                select ?obs
                where {
                    ?obs rdf:type ?class
                }
            """,
            initBindings={"class": cur.identifier},
        )


class ObservationRegister:
    """
    Create a proper database line as dict and manages the overwritings of local dict (typically extracted from a FeatureExtractor with other dicts.
    """

    def __init__(self):
        self.default = COLUMNS["OBSERVATION_FACT"]

    def merge_dics(self, local_info, weaker_info, stronger_info):
        base_dic = self.default.copy()
        agnostic_dic = base_dic.update(local_info)
        fmerge = weaker_info.copy().update(agnostic_dic)
        smerge = fmerge.update(stronger_info)
        return smerge


class InformationTree:
    """
    This class is in charge of the graph exploration given a set of entrypoints. It will trigger the collection of observation informations.
    The tool classes ObservationRegister and FeatureExtractor are called from there.
    """

    def __init__(self, resources_list):
        self.observations = resources_list

    def get_info_dics(self):
        return self.explore_subtree()

    def is_pathend(self, resource):
        preds = resource.predicates()
        for pre in preds:
            if pre.identifier not in TO_IGNORE:
                pass

    def walk_obstree(self):
        # TODO : SUPER IMPORTANT: only gather LEAVES i.e datatypeprops modifiers with their value OR objetpropmodifiers with 0 predicates OR individuals
        # TODO : for basecode,
        for obs in self.observations:
            for feature in extractor.get_features_and_values():
                pass
            info.merge_dics(extractor.extract_features())
            # TODO: keep track of the basecode when performing the exploration
            # TODO: check if element is a child of valueset at this stage

            """
            children_info = self.explore_subtree(obs, upper_info = info)
            info = ObservationRegister(obs) 
            return [info]+children_info
            """

    def explore_subtree(self, resource, upper_info):
        """
        Recursive function, stop when the current resource has no predicates (leaf).
        Return the last resource along with the logical path that lead to it as/with the basecode, and information to be used above and in siblings (unit, date, etc.)
        Special case for the information that is not conceptual but related to patient, site, encounter
        """
        gen = resource.predicate_objects()
        if len(gen) == 0:
            return
        for pred, obj in gen:
            if pred.identifier in BLACKLIST:
                continue
            self.explore_subtree(obj, upper_info)

    def reduce_basecode(resource, prefix):
        return ""


# TODO: fill the dicts. fill the other dimensions. write unit tests.
