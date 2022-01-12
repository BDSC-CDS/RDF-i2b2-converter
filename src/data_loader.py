from configs import *
from utils import *
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

    def is_pathend(self, obj):
        """
        Check if the pointed object is the end of a search.
        Criteria to be a search end are:
            - be a datatype like xsd:string or xsd:double
            - be an object with :
                - type a class of a terminology (string-match the URI of external terminologies)
                - no type or a SPHN type NamedIndividual without any other predicate
        Else, the obj can be expanded into more predicates and then the search continues.
        """
        # Workaround to check if the predicate is a DataProperty or an ObjectProperty
        if callable(obj.value):
            if all([k.identifier in (TYPE_PREDICATE_URI, LABEL_URI) for k in obj.predicates()]):
                return True
            # We encounter an expandable object BUT it can still be a path end (if the item is a ValusetIndividual or an instance of a Terminology class)
            if terminology_indicator(obj.value(TYPE_PREDICATE_URI)):
                return True
        else:
            return True

    def explore_obstree(self, resource, upper_info):
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
            cur_info = self.package_info()
            if self.is_pathend(obj):
                return cur_info()
            else:
                cur_info.update(self.explore_subtree(obj, cur_info))
                return self.explore_obstree(obj, upper_info)

    def cur_info(self, parent_info):
        """
        Extract the information at this level, using the parent info for some a priori values.
        """
        return {}

    def get_basecode(self, resource, prefix):
        hdler = I2B2BasecodeHandler()
        self.basecode = hdler.reduce_basecode(resource, prefix.basecode)


# TODO: fill the dicts. fill the other dimensions. write unit tests.