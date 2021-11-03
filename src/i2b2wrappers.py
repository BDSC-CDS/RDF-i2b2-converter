from rdfwrappers import *
from utils import db_to_csv, generate_xml


def drop(attribute):
    """
    If the attribute should be dropped as mentioned in the config file, skip it
    """
    for rn in attribute.get_children():
        cur_uri = rn.get_identifier()
        if cur_uri in ONTOLOGY_DROP_DIC.values():
            return True
    return False


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
        for sub in concept.subconcepts:  # TODO is that a correct recursion?
            self.i2b2concepts.extend(I2B2Converter(sub, cur).i2b2concepts)
        self.left_tosearch = self.i2b2concepts
        self.towrite = []

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
        """
        Write all the db at once through a pandas dataframe.
        If updating this function to enable append mode, do not forget to make sure header is written exactly oncein the file.
        """
        db_to_csv(self.towrite, METADATA_PATH)


class I2B2OntologyElement:
    def __init__(self, graph_component, parent=None):
        
        self.parent = parent
        self.component = graph_component
        self.basecode_handler = I2B2BasecodeHandler(self)
        self.path_handler = I2B2PathResolver(self)
        self.set_path()
        self.set_code()
        self.set_level()
        self.set_comment()
        self.line_updates={}

    def set_path(self):
        self.path = self.path_handler.get_path()

    def set_code(self):
        self.code = self.basecode_handler.extract_basecode()

    def set_level(self):
        self.level = self.path.count("\\")

    def set_comment(self):
        self.comment = self.component.get_comment()

    def get_concept(self):
        pass
    
    def mutate_valueinfo(self, datatype_string):
        """
        Some observations can store a value. In that case, the corresponding ontology element should specify the value type both in the valuetype_cd and in the XML form.
        """
        ont_values_cells = EQUIVALENCES[datatype_string]
        metadata = generate_xml(ont_values_cells["METADATA_XML"])
        self.line_updates = ont_values_cells.update({"METADATA_XML":metadata})

    def walk_mtree(self):
        res = []
        submods = self.get_filtered_children()
        for component in submods:
            # Check if the child's URI is a primary data type. If so, its information does not justify creating a new ontology item but is written in the xml field.
            cur_uri = component.get_uri()
            if cur_uri in DATA_LEAVES.keys():
                self.mutate_valueinfo(DATA_LEAVES[cur_uri])
            else:
                cur = I2B2Modifier(component, parent=self, applied_path=self.applied_path)
                next = cur.walk_mtree()
                res.append(cur)
                res.extend(next)
        return res

    def single_line(self, nodetype=""):
        return {
            "C_HLEVEL": str(self.level),
            "C_FULLNAME": self.path,
            "C_NAME": self.label,
            "C_SYNONYM_CD": "N",
            "C_BASECODE": self.basecode,
            "C_COMMENT": self.comment,
            "C_DIMCODE": self.path,
            "C_TOOLTIP": "",
            "C_TOTALNUM": "",
            "UPDATE_DATE": "",
            "DOWNLOAD_DATE": "",
            "IMPORT_DATE": "",
            "SOURCESYSTEM_CD": "",
            "VALUETYPE_CD": "",
            "M_EXCLUSION_CD": "",
            "C_PATH": self.parent.path,
            "C_SYMBOL": self.path[len(self.parent.path) :],
            "C_METADATAXML": "",
        }.update(self.line_updates)

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
            el["C_VISUALATTRIBUTES"] = "FA"
        previous = line[-1]
        line.append(
            self.single_line(
                type="concept",
                level=str(int(previous["C_HLEVEL"]) + 1),
                prefix=previous["C_FULLNAME"],
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
                "C_FACTTABLECOLUMN": "CONCEPT_CD",
                "C_TABLENAME": "CONCEPT_DIMENSION",
                "C_COLUMNNAME": "CONCEPT_PATH",
                "C_COLUMNDATATYPE": "T",
                "C_OPERATOR": "LIKE",
                "C_VISUALATTRIBUTES": "FA",
                "M_APPLIED_PATH": "@",
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
                parent_path = ""
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
            i2b2element.parent.basecode_handler.extract_basecode()
            if i2b2element.parent is not None
            else ""
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
        if parent is not None and parent.path == applied_path:
            parent = None
        super().__init__(component2, parent)
        self.applied_path = applied_path
