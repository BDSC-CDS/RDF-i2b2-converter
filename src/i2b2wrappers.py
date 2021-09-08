from rdfwrappers import *


def drop(attribute):
    """
    If the attribute should be dropped as mentioned in the config file, skip it
    """
    for rn in attribute.get_children():
        cur_uri = rn.get_identifier()
        if cur_uri in ONTOLOGY_DROP_DIC.values():
            return True
    return False

#TODO maybe all this merge thing is too complex. just see if the property is a datatype and make an equivalence to the valuetype_cd code.
# Include default metadataxml depending on the datatype. when finding a unit, overwrite the unit field of the metadataxml
# But first check for distinct units usage. Or exceptionnally go fetch the equivalent loinc equivalence... Maybe in a separate module 
class I2B2Converter:
    """
    The converter object initialized with a python rdfwrappers.Concept instance.
    All concepts related to this instance (i.e it and its subconcepts) are converted to an i2b2 concept object (taking the hierarchy into account)
    From this object can be triggered the modifiers generation.
    """

    def __init__(self, concept, i2b2parent=None):
        """
        Extract and instantiate all the i2b2concepts for this run.
        They are foud by navigating through the subconcepts tree (ignoring the properties) defined in rdfwrappers.
        """
        cur = I2B2Concept(concept, i2b2parent)
        self.i2b2concepts = [cur]
        concept.get_entry_desc()
        for sub in concept.subconcepts:#TODO is that a correct recursion?
            self.i2b2concepts.extend(I2B2Converter(sub, cur).i2b2concepts)
        self.left_tosearch = self.i2b2concepts

        """
        self.i2b2concepts = [I2B2Concept(concept, i2b2parent)]
        self.concept_object = concept
    
    def populate(self)
        if len(self.i2b2concepts)>1:
            return
        for sub in self.concept_object.subconcepts:
            self.i2b2concepts.extend(I2B2Converter(sub, cur).i2b2concepts)
        self.left_tosearch = self.i2b2concepts
        """

    def get_batch(self):
        try:
            cur = self.left_tosearch.pop()
        except:
            return False
        cur.set_path()
        cur.set_code()
        cur.extract_modelems()
        self.towrite = [k.get_lines() for k in [cur] + cur.modifiers]
        return True

    def write(self, filepath):
        # use old db_csv code here
        with open(filepath, "a"):
            pass


class I2B2OntologyElement:
    def __init__(self, graph_component, parent=None):
        self.parent = parent
        self.component = graph_component
        self.basecode_handler = I2B2BasecodeHandler(self)
        self.path_handler = I2B2PathResolver(self)
        self.set_path()
        self.set_code()
        self.set_level()

    def set_path(self):
        self.path = self.path_handler.get_path()

    def set_code(self):
        self.code = self.basecode_handler.extract_basecode()

    def set_level(self):
        self.level = self.path.count("\\")

    def get_concept(self):
        pass

    def walk_mtree(self):
        res = []
        submods = self.get_filtered_children()
        for k in submods:
            cur = I2B2Modifier(k, parent=self, applied_path=self.applied_path)
            next = cur.walk_mtree()
            res.append(cur)
            res.extend(next)
        return res
        

    def single_line(self, nodetype=""):
        return {
            "c_hlevel": str(self.level),
            "c_fullname": self.path,
            "c_name": self.label,
            "c_synonym_cd": "N",
            "c_basecode": self.basecode,
            "c_comment": self.comment,
            "c_dimcode": self.path,
            "c_tooltip": "",
            "c_totalnum": "",
            "update_date": "",
            "download_date": "",
            "import_date": "",
            "sourcesystem_cd": "",
            "valuetype_cd": "",
            "m_exclusion_cd": "",
            "c_path": self.parent.path,
            "c_symbol": self.path[len(self.parent.path) :],
            "c_metadataxml": "",
        }

    def get_filtered_children(self):
        """
        Fetch the properties of self (referencing self as domain or subProperty). Keep only the ontology properties (that should appear in the hierarchy) and return them.
        In particular, discard (default) the dates, patient number, clinical site ID, encounter ID.
        """
        modifiers_tobe = []
        for attr in self.component.get_children():
            if not drop(attr):
                modifiers_tobe.append(attr)
        return modifiers_tobe


    def get_info(self):
        """
        Create a db line as dictionary for a concept, using the single_line function.
        This function looks at potential macro-concepts and allows to use multi-leveled concepts.
        Return a list of dictionaries, one for each concept found on the way.
        """
        if self.parent is None:
            return [self.single_line(nodetype="root")]
        line = []
        line.extend(self.parent.get_info())
        for el in line:
            el["c_visualattributes"] = "FA"
        previous = line[-1]
        line.append(
            self.single_line(
                type="concept",
                level=str(int(previous["c_hlevel"]) + 1),
                prefix=previous["c_fullname"],
            )
        )
        return line

    def __repr__(self):
        return self.__class__.__name__ + " at " + self.component.__repr__()


class I2B2Concept(I2B2OntologyElement):
    def extract_modelems(self):
        self.applied_path = self.path
        self.modifiers = self.walk_mtree()
    
    def get_concept(self):
        return self

    def get_info(self):
        info = super().get_info()
        info.update(
            {
                "c_facttablecolumn": "CONCEPT_CD",
                "c_tablename": "CONCEPT_DIMENSION",
                "c_columnname": "CONCEPT_PATH",
                "c_columndatatype": "T",
                "c_operator": "LIKE",
                "c_visualattributes": "FA",
                "m_applied_path": "@",
            }
        )
        return info

class I2B2PathResolver:
    def __init__(self, i2b2ontelem):
        self.element = i2b2ontelem
        self.path = ""

    def get_path(self):
        if self.path == "":
            if self.element.parent is None:
                parent_path=""
            else:
                parent_path = self.element.parent.path_handler.get_path()
            self.path = parent_path + "\\" + self.element.component.get_shortname()
        return self.path


class I2B2BasecodeHandler:
    """
    Compute and extract the basecode for a Class or a Property existing in the ontology.
    Access the attributes of the embedded RDF resource.
    If a value is specified, it will be included in the basecode computation.
    If the initializing element has a parent element with an existing basecode, it will also be included in the basecode computation.
    """

    def __init__(self, i2b2element, value=None):
        self.value = value
        self.basecode = None
        self.core = i2b2element.component.get_uri()
        self.prefix = (
            i2b2element.parent.basecode_handler.extract_basecode() if i2b2element.parent is not None else ""
        )

    def extract_basecode(self):
        if self.basecode is not None:
            return self.basecode
        return self.reduce_basecode()

    def reduce_basecode(self, debug=False, cap=MAX_BASECODE_LENGTH):
        """
        Returns a basecode for self.component. A prefix and a value can be added in the hash.
        The code is made from the URI of the RDF ontology concept, which is an info that does not depend on the ontology converter's output.
        A basecode is invisible to the user, and its only constraints is to be unique regarding the concept it is describing,
        and to be computable both from the ontology side and from the data loader side.
        The resulting code is the joining key between data tables and ontology tables.
        """
        return ""
        rdf_uri = self.core
        value = self.value
        prefix = self.prefix
        if len(value) > 0 and value[0] == "\\":
            value = value[1:]
        if rdf_uri[-1] != "\\":
            tmp_uri = rdf_uri + "\\"
        tohash = tmp_uri + value
        return tohash if debug else hashlib.sha256(tohash.encode()).hexdigest()[:cap]


class I2B2Modifier(I2B2OntologyElement):
    def __init__(self, component2, parent=None, applied_path=None):
        # Handle the case where a concept created self and registered as parent: discard (keep only modifier hierarchy)
        if parent is not None and parent.path==applied_path:
            parent=None
        super().__init__(component2, parent)
        self.applied_path = applied_path

