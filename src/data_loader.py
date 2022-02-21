from multiprocessing.dummy import current_process
from configs import *
from utils import *
from i2b2wrappers import I2B2BasecodeHandler

cur_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")
with open(cur_path + "files/data_loader_config.json") as ff:
    config = json.load(ff)
for val in config["TO_IGNORE"]:
    nxt =[rdflib.URIRef(val)] if type(val) == str else [rdflib.URIRef(k) for k in val]
    globals()["TO_IGNORE"].extend(nxt)


class DataLoader:
    """
    Manage data conversion from an observation graph. TODO: is there a way to avoid loading the whole graph?
    Observations register is performed by batches defined by the type (class) of observations.
    """

    def __init__(self, parser, entrypoints, filename="OBSERVATION_FACT.csv", reset_file=True):
        """
        Take a list of class resources.
        """
        self.graph = parser.graph
        self.entry_class_resources = entrypoints
        self.filename = filename
        self.init = reset_file


    def extract_all(self):
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
        Get the next batch of entry instances (typically one class at a time using get_next_class_instance), then
            launch exploration of the RDF observation graph using an InformationTree object.
        Return the database lines for the current batch.
        """
        database_batch = []
        observations = self.get_next_class_instances()
        information_tree = InformationTree(observations)
        database_batch = information_tree.get_info_dics()
        return database_batch

    def get_next_class_instances(self, selclass=None):
        """
        Extract the next observations batch from the RDF graph.
        """
        if self.entry_class_resources == []:
            return []
        if selclass is None:
            cur = self.entry_class_resources.pop()
            selclass=cur.identifier
        obs = self.graph.query(
            """
                select ?obs
                where {
                    ?obs rdf:type ?class
                }
            """,
            initBindings={"class": selclass},
        )


class ObservationRegister:
    """
    Create a proper database line as dict and manages the overwritings of local dict (typically extracted from a FeatureExtractor with other dicts.
    """

    def __init__(self):
        self.default = COLUMNS["OBSERVATION_FACT"]
        self.records = []

    def is_empty(self):
        return self.records == []

    def get_processed_records(self):
        return self.records

    def add_record(self, end_node, basecode):
        """
        Process the information in an end of path. Add every detail in the correct column of the default observation table.
        TODO: maybe get as parameter also the highest-level element so we can get date and other details from it
        """
        self.records.append("")

    def merge_dics(self, local_info, weaker_info, stronger_info):
        base_dic = self.default.copy()
        agnostic_dic = base_dic.update(local_info)
        fmerge = weaker_info.copy().update(agnostic_dic)
        smerge = fmerge.update(stronger_info)
        return smerge


class InformationTree:
    """
    This class is in charge of the graph exploration given a collection of top-level instance.
    It will trigger the collection of observation informations.
    When the tip of a branch is found, is is processed and stored in the ObservationRegister.
    The first level is used to extract patient, hospital and encounter data. 
    """

    def __init__(self, resources_list):
        self.observations = resources_list
        self.main_register = ObservationRegister()
        self.context_register = ContextRegister()

    def get_info_dics(self):
        if self.register.is_empty():
            self.explore_subtree()
        return self.register.get_processed_records()

    def explore_tree_master(self):
        for obs in self.observations:
            self.explore_obstree(obs)

    def is_contextual_detail(self, obj):
        """
        Determines if a constructed resource should be stored in the default register or in a special one.
        Makes use of the configurable table-colums mapping to special URIs.
        """
        # TODO: if obj is not something specified in the tables (ex. hasSubjectPseudoIdentifier -> HasIdentifier, or hasAdministrativeCase -> details),
        return False

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
        # Workaround to check if an item is a rdflib.resource.Resource or a rdflib.term.Literal
        if callable(obj.value):
            # Criteria for having no forward link i.e probably is a valuesetindividual. TODO: check for corner cases
            # Cannot check if it is an actual owl:NamedIndividual because this information lies in the ontology graph
            if all([k.identifier in (TYPE_PREDICATE_URI, LABEL_URI) for k in obj.predicates()]):
                return True
            # We encounter an expandable object BUT it can still be a path end (if the item is a ValusetIndividual or an instance of a Terminology class)
            if terminology_indicator(obj.value(TYPE_PREDICATE_URI)):
                return True
        else:
            return True

    def explore_obstree(self, resource, upper_info=None, basecode_prefix=None):
        """
        Recursive function, stop when the current resource has no predicates (leaf).
        Return the last resource along with the logical path that lead to it as/with the basecode, and information to be used above and in siblings (unit, date, etc.)
        Special case for the information that is not conceptual but related to patient, site, encounter
        TODO: use the upper_info if necessary
        """
        rdfclass = resource.value(TYPE_PREDICATE_URI)
        if rdfclass in BLACKLIST:
            return
        gen = resource.predicate_objects()

        # Updating the basecode that led us to there
        hdler = I2B2BasecodeHandler()
        current_basecode = hdler.reduce_basecode(resource, basecode_prefix)

        for pred, obj in gen:
            if pred.identifier in BLACKLIST:
                continue
            # Updating the basecode with the forward link
            basecode= hdler.reduce_basecode(pred, current_basecode)
            
            cur_info = self.package_info()
            if self.is_pathend(obj):
                self.store_register(obj, pred, basecode, cur_info)
            else:
                cur_info.update(self.explore_subtree(obj, cur_info))
                return self.explore_obstree(obj, upper_info, basecode_prefix=self.basecode)

    def store_register(self, resource, origin, basecode_upto_origin, upper_info):
        """
        The parameter basecode includes the origin node but not the actual resource.
        Based on the resource type, the appropriate register will then create a new basecode or not.
        """
        if self.is_contextual_detail(resource):
            self.context_register.add(resource, origin, basecode_upto_origin)
        else:
            self.main_register.add_record(resource, origin, basecode_upto_origin)

class ContextRegister:
    """
    Handles contextual information from an observation instance depending on the configured mappings.
    """
    def __init__(self):
        self.mappings = {}
        for key, presence_dic in COLUMNS_MAPPING.items():
            if "table" in presence_dic.keys():
                self.mappings.update({table:column} for table,column in presence_dic["table"])
    
    def add(self, obj, origin, basecode):
        """
        Add the detail in the appropriate table and column.
        """
        pass





# TODO: fill the dicts. fill the other dimensions. write unit tests. avoid discovering the same objects repeatedly (e.g patient, provider info?)
