from rdfwrappers import *

class I2B2Converter:
    """
    The converter object initialized with a python rdfwrappers.Concept instance. 
    All concepts related to this instance (i.e it and its subconcepts) are converted to an i2b2 concept object (taking the hierarchy into account)
    From this object can be triggered the modifiers generation.
    """
    def __init__(self, concept:Concept):
        self.i2b2concepts = [I2B2Concept(concept)]
        self.i2b2concepts.extend([I2B2Concept(sub, parent=concept) for sub in concept.subconcepts])#TODO use recursion here to handle multi level concept inclusion
        self.left_tosearch = self.i2b2concepts

    def get_batch(self):
        try:
            cur = self.left_tosearch.pop()
        except:
            return False
        cur.set_path()
        cur.set_code()
        cur.extract_modelems()
        self.towrite = [k.get_lines() for k in [cur]+cur.modifiers]
        return True

    def write(self, filepath):
        #use old db_csv code here
        with open(filepath, "a"):
            pass

class I2B2OntologyElement:
    def __init__(self, component, parent=None):
        self.parent = parent
        self.component = component
        self.basecodehdler=I2B2BasecodeHandler(self)
        self.path_handler=I2B2PathResolver(self)

    def set_path(self):
        self.path= self.path_handler.get_path()

    def set_code(self):
        self.code= self.basecodehdler.get_code()

    def set_level(self):
        self.level = self.c_path.count("\\")

    def walk_mtree(self):#TODO finish this. use recursion
        return [I2B2Modifier(k, parent=self) for k in self.get_filtered_children()]

    def single_line(self):
        return {
            "c_hlevel": str(self.level),
            "c_fullname": self.c_path,
            "c_name": self.label,
            "c_synonym_cd": "N",
            "c_basecode": self.basecode,
            "c_comment": self.comment,
            "c_dimcode": self.c_path,
            "c_tooltip": "",
            "c_totalnum": "",
            "update_date": "",
            "download_date": "",
            "import_date": "",
            "sourcesystem_cd": "",
            "valuetype_cd": "",
            "m_exclusion_cd": "",
            "c_path": self.parent.c_path,
            "c_symbol": self.c_path[len(self.parent.c_path) :],
            "c_metadataxml": "",
        }

    def get_filtered_children(self, toignore=OBSERVATION_PRED+[DATE_DESCRIPTOR]):
        """
        Fetch the properties of self (referencing self as domain). Keep only the ontology properties (that should appear in the hierarchy) and return them.
        In particular, discard (default) the dates, patient number, clinical site ID, encounter ID.
        """
        modifiers = []
        for attr in self.component.get_children():
            if attr.identifier.toPython() not in toignore:
                modifiers_tobe.append(attr)  
        return modifiers_tobe

    def get_info(self):
        """
        Create a db line as dictionary for a concept, using the single_line function.
        This function looks at potential macro-concepts and allows to use multi-leveled concepts.
        Return a list of dictionaries, one for each concept found on the way.
        """
        if self.parent is None:
            return [single_line(entry, type="root")]
        line = []
        line.extend(self.parent.get_info())
        for el in line:
            el["c_visualattributes"] = "FA"
        previous = line[-1]
        line.append(
            self.single_line(
                entry,
                type="concept",
                level=str(int(previous["c_hlevel"]) + 1),
                prefix=previous["c_fullname"],
            )
        )
        return line

class I2B2Concept(I2B2OntologyElement):
    def extract_modelems(self):#TODO finish this
        self.modifiers.extend(self.walk_mtree())

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
    

    def walk_mtree(self, parent):#TODO finish this
        modifiers = super().walk_mtree()
        for mod in modifiers:
            mod.set_applied_concept(self)

    
class I2B2PathResolver:
    def __init__(self, component):
        self.component = component
        self.path = ""

    def get_path(self):
        if self.path == "":
            parent_path = self.component.parent.path_resolver.get_path()
            self.path = parent_path + "\\" + self.component.shortname
        return self.path

class I2B2BasecodeHandler:
    """
    Compute and extract the basecode for a Class or a Property existing in the ontology.
    If a value is specified, it will be included in the basecode computation.
    If the initializing component has a parent component with an existing basecode, it will also be included in the basecode computation.
    """

    def __init__(self, component, value=None):
        self.value = value
        self.core = component.resource.identifier
        self.prefix = component.parent.basecode

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
    def set_applied_concept(self, concept):
        self.applied_path = concept.path
