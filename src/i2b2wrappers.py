from rdfwrappers import *
from utils import db_to_csv, generate_xml


def drop(attribute):
    """
    If the attribute should be dropped as mentioned in the config file, skip it
    """
    for rn in attribute.get_children():
        cur_uri = rn.get_uri()
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
        for sub in concept.subconcepts: 
            self.i2b2concepts.extend(I2B2Converter(sub, cur).i2b2concepts)
        self.left_tosearch = self.i2b2concepts.copy()
        self.towrite = []

    def get_batch(self):
        try:
            cur = self.left_tosearch.pop()
        except:
            return False
        cur.set_path()
        cur.set_code()
        cur.extract_modelems()
        self.towrite = [k.get_db_line() for k in [cur] + cur.modifiers]
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
        self.basecode_handler = I2B2BasecodeHandler(self, ph=self.get_parent_codehdler())
        self.path_handler = I2B2PathResolver(self)
        self.set_path()
        self.set_code()
        self.set_displayname()
        self.set_level()
        self.set_comment()
        self.line_updates={}
        self.visual = None

    def set_path(self):
        self.path = self.path_handler.get_path()

    def set_displayname(self):
        self.displayname = self.component.get_label()

    def set_code(self):
        self.code = self.basecode_handler.get_basecode()

    def set_level(self):
        if self.parent is None:
            self.level =1
        else:
            self.level = self.parent.level +1

    def set_comment(self):
        self.comment = self.component.get_comment()

    def get_concept(self):
        pass
    
    def mutate_valueinfo(self, datatype_string):
        """
        Some observations can store a value. In that case, the corresponding ontology element should specify the value type both in the valuetype_cd and in the XML form.
        """
        ont_values_cells = EQUIVALENCES[datatype_string]
        metadata = generate_xml(ont_values_cells["C_METADATAXML"])
        ont_values_cells.copy().update({"C_METADATAXML":metadata})
        return ont_values_cells

    def walk_mtree(self):
        res = []
        submods = self.get_filtered_children()
        for component in submods:
            # Check if the child's URI is a primary data type. If so, its information does not justify creating a new ontology item but is written in the xml field.
            cur_uri = component.get_uri()
            if cur_uri in DATA_LEAVES.keys():
                self.line_updates= self.mutate_valueinfo(DATA_LEAVES[cur_uri])
            else:
                cur = I2B2Modifier(component, parent=self, applied_path=self.applied_path)
                self.set_visual("folder")
                next = cur.walk_mtree()
                res.append(cur)
                res.extend(next)
        return res

    def get_db_line(self):
        # First gather the class-specific information
        spec_dir = self.get_class_info()
        # Then update them with the item-specific information
        spec_dir.update(self.line_updates)
        # Then update the base dict with this specific dir and return.

        if self.parent is not None:
            parpath = self.parent.path 
            symbol = self.path[len(self.parent.path) :]
        else:
            parpath=""
            symbol =self.path
        return {
            "C_HLEVEL": str(self.level),
            "C_FULLNAME": ROOT_PATH + self.path,
            "C_NAME": self.displayname,
            "C_SYNONYM_CD": "N",
            "C_BASECODE": self.code,
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
            "C_PATH": parpath,
            "C_SYMBOL": symbol,
            "C_METADATAXML": "",
        }.update(spec_dir)

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

    def __repr__(self):
        return self.__class__.__name__ + " at " + self.component.__repr__()


class I2B2Concept(I2B2OntologyElement):
    def extract_modelems(self):
        self.applied_path = self.path
        self.modifiers = self.walk_mtree()

    def get_concept(self):
        return self

    def set_visual(self, type):
        """
        Default is folder anyway for now (for concepts). For later uses can be switched to other visual attributes.
        """
        if type=="folder":
            self.visual == "FA"

    def get_class_info(self):
        if self.visual is None:
            self.visual = "FA"
        info = {
                "C_FACTTABLECOLUMN": "CONCEPT_CD",
                "C_TABLENAME": "CONCEPT_DIMENSION",
                "C_COLUMNNAME": "CONCEPT_PATH",
                "C_COLUMNDATATYPE": "T",
                "C_OPERATOR": "LIKE",
                "C_VISUALATTRIBUTES": self.visual,
                "M_APPLIED_PATH": "@",
            }
        return info

    def get_parent_codehdler(self):
        """
        For an i2b2 concept, we do not need to embed the hierarchy in the basecode: the concept URI is enough.
        """
        return None


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
    If an other handler is specified as "ph" at construction, its code will be embedded in the computation. (this helps encapsulating hierarchy in codes)
    """

    def __init__(self, i2b2element, ph=None, value=""):
        self.value = value # if child of terminology append : + code, don't go through the whole tree. only problem is if LOINC> $loincel has several possible paths
        self.basecode = None
        self.core = i2b2element.component.get_uri()
        self.prefix = ph.get_basecode() if ph is not None else ""

    def get_basecode(self):
        if self.basecode is not None:
            return self.basecode
        return self.reduce_basecode()

    def reduce_basecode(self, debug=False, cap=MAX_BASECODE_LENGTH): # TODO: check only taking into account one concept is enough. test.
        """
        Returns a basecode for self.component. A prefix and a value can be added in the hash.
        The code is made from the URI of the RDF ontology concept, which is an info that does not depend on the ontology converter's output.
        A basecode is invisible to the user, and its only constraints is to be unique regarding the concept it is describing,
        and to be computable both from the ontology side and from the data loader side.
        The resulting code is the joining key between data tables and ontology tables.
        """
        rdf_uri = self.core
        value = self.value
        prefix = self.prefix
        
        if rdf_uri[-1] != "\\":
            rdf_uri = rdf_uri + "\\"

        if len(value) > 0 and value[0] == "\\":
            value = value[1:]
            tohash = rdf_uri + ":"+value
        else :
            to_hash = rdf_uri
        return tohash if debug else hashlib.sha256(to_hash.encode()).hexdigest()[:cap]


class I2B2Modifier(I2B2OntologyElement):
    def __init__(self, component2, parent, applied_path=None):
        # Handle the case where a concept created self and registered as parent: discard (keep only modifier hierarchy)
        if parent.path == applied_path:
            self.applied_concept = parent
        else:
            self.applied_concept = parent.applied_concept
        super().__init__(component2, parent)
        self.applied_path = applied_path
        self.visual = "RA"

    def set_visual(self, type):
        """
        Default is leaf, can be switched to folder. For later uses can be switched to other visual attributes.
        """
        if type=="folder":
            self.visual == "DA"

    def get_class_info(self):
        info = {
                "C_FACTTABLECOLUMN": "MODIFIER_CD",
                "C_TABLENAME": "MODIFIER_DIMENSION",
                "C_COLUMNNAME": "MODIFIER_PATH",
                "C_COLUMNDATATYPE": "T",
                "C_OPERATOR": "LIKE",
                "C_VISUALATTRIBUTES": self.visual,
                "M_APPLIED_PATH": self.applied_path
            }
        return info

    def get_parent_codehdler(self):
        """
        For an i2b2 modifier, we embed the hierarchy in the basecode since collisions can happen.
        If we are at the root of the modifier hierarchy, embed the applied concept
        """
        if self.parent is not None:
            return self.parent.basecode_handler
        return self.applied_concept.basecode_handler