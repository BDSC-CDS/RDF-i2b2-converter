from utils import *


def is_valid(pred,obj):
    return pred.identifier.toPython() not in BLACKLIST and obj.value(TYPE_PREDICATE_URI).toPython() not in BLACKLIST

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
        res = db_to_csv(db, self.filename, columns=COLUMNS["OBSERVATION_FACT"],mode=mode)
        self.init = False
        return res is not None

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
        return obs 


class ObservationRegister:
    """
    Create a proper database line as dict and manages the overwritings of local dict (typically extracted from a FeatureExtractor with other dicts.
    """

    def __init__(self):
        self.keys = COLUMNS["OBSERVATION_FACT"]
        self.value_items = COLUMNS_MAPPING["VALUE"]
        self.records = []

    def is_empty(self):
        return self.records == []

    def get_processed_records(self):
        return self.records

    def digest(self, resource, parent, basecode, context):
        """
        Receive a resource which can be embed a rdflib.Literal or a class instance.
        If specified in the config file, class instances should be digged through using the "pred_to_value" list of predicates.
        """
        details = context.copy()
        vtype = None
        if not callable(resource.value):
            value=resource.value.toPython()
            vtype = value.datatype
        
        if vtype is not None:
            if not vtype in self.value_items.keys():
                raise Exception("Type not defined in config file: ", vtype)
            details.update({self.value_items[vtype]["col"]:value})
            details.update(self.value_items[vtype]["misc"])
        # In any case this digest thing should only add a value field if there is a value, then proceeds with adding the basecode entry in any case
        self.add_record(basecode, context=details) 

    def add_record(self, basecode, context={}):
        """
        Process the information in an end of path. Add every detail in the correct column of the default observation table.
        Use the context details.
        """
        if context == {}:
            raise Exception("Cannot add a record with an empty context")
        
        record = context.copy()
        record.update({"MODIFIER_CD":basecode})
        self.records.append(record)

class InformationTree:
    """
    This class is in charge of the graph exploration given a collection of top-level instance.
    It will trigger the collection of observation informations.
    When the tip of a branch is found, is is processed and stored in the ObservationRegister.
    The first level is used to extract patient, hospital and encounter data. 
    """

    def __init__(self, resources_list):
        self.observations = resources_list
        self.obs_register = ObservationRegister()

    def get_info_dics(self):
        if self.obs_register.is_empty():
            self.explore_tree_master()
        return self.obs_register.get_processed_records()

    def explore_tree_master(self):
        for i in range(len(self.observations)):
            obs = self.observations[i]
            self.explore_obstree(obs, instance_num=i, concept=True)

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
            # Criteria for having no forward link i.e probably is a valuesetindividual. 
            # If the item has no predicate at all, the following test will be evaluated to True
            if all([k.identifier in (TYPE_PREDICATE_URI, LABEL_URI) for k in obj.predicates()]):
                return True
            # We encounter an expandable object BUT it can still be a path end (if the item is a ValusetIndividual or an instance of a Terminology class)
            if terminology_indicator(obj.value(TYPE_PREDICATE_URI)):
                return True
        else:
            return True

    def explore_obstree(self, resource, instance_num, basecode_prefix="", parent_context={}, concept=False):
        """
        Recursive function, stop when the current resource has no predicates (leaf).
        Return the last resource along with the logical path that lead to it as/with the basecode, and information to be used above and in siblings (unit, date, etc.)
        """
        rdfclass = resource.value(TYPE_PREDICATE_URI)
        if rdfclass in BLACKLIST:
            return
        # Updating the basecode that led us to there
        hdler = I2B2BasecodeHandler()
        current_basecode = hdler.reduce_basecode(resource, basecode_prefix)

        # Get the properties
        pred_objects = [k for k in resource.predicate_objects() if is_valid(k)]

        # Digest the context and get back the "clean" list of details
        context_register = ContextFactory(parent_context)
        observation_elements = context_register.digest(pred_objects)
        if concept:
            context_register.add_concept_code(current_basecode, instance_num=instance_num)
            current_basecode = ""
            self.store_register(resource)

        for pred, obj in observation_elements:
            if pred.identifier in BLACKLIST:
                continue
            # Updating the basecode with the forward link
            basecode= hdler.reduce_basecode(pred, current_basecode)
            
            if self.is_pathend(obj):
                self.obs_register.digest(obj, pred, basecode, context_register.get_context())
            else:
                return self.explore_obstree(obj, basecode_prefix=self.basecode, parent_context = context_register)

class ContextFactory:
    """
    Handles contextual information from an observation instance depending on the configured mappings.
    """
    def __init__(self, parent_context={}):
        self.context = parent_context.copy()
        self.fields = COLUMNS_MAPPING["CONTEXT"].keys()

    def digest(self, pred_objects):
        """
        Given a list of predicate_objects, filters out the ones linked to the observation context.
        Stores the context information that isn't already stored, or the one that bears the "overwrite" flag
        """
        clean = []
        # If the context holder is empty, extract and save (else simply discard discard the context elements)
        for pred, obj in pred_objects:
            obj_type = TYPE_PREDICATE_URI
            if obj_type in self.fields :
                if self.fields[obj_type]["overwrite"]=="True" or obj_type not in self.context.keys():
                    self.add_context_element(obj_type, obj)
            else:
                clean.append((pred,obj))
        return clean

    def add_concept_code(self, basecode, instance_num):
        """
        Add the concept code as a context element (will stand for all modifiers and the concept)
        """
        self.context.update({
            COLUMNS_MAPPING["instance_num"]:instance_num,
            COLUMNS_MAPPING["basecode"]:basecode
        })

    def add_context_element(self, obj_type, obj):
        """
        Add a context element based on the instructions in the config file.
        """
        tmp = COLUMNS_MAPPING["CONTEXT"][obj_type]["pred_to_value"] if "pred_to_value" in COLUMNS_MAPPING["CONTEXT"][obj_type].keys() else []
        val = val.value(tmp.pop(0)) if callable(val.value) else val.value
        while tmp != []:
            val = val.value(tmp.pop(0))
        self.context.update({key:val})

    def get_context(self):
        return self.context
