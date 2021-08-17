from rdfwrappers import *

class I2B2Converter:
    """
    The converter object initialized with a python rdfwrappers.Concept instance. 
    All concepts related to this instance (i.e it and its subconcepts) are converted to an i2b2 concept object (taking the hierarchy into account)
    From this object can be triggered the modifiers generation.
    """
    def __init__(self, concept:Concept):
        self.i2b2concepts = [I2B2Concept(concept)]
        self.i2b2concepts.extend([I2B2Concept(sub, parent=concept) for sub in concept.subconcepts])

    def generate_modifiers(self):
        """
        TODO: do we generate modifiers all at once or recursively
        """
        for conc in self.i2b2concepts:
            pass

class I2B2PathResolver:
    def __init__(self, component):
        self.component = component

    def compute_path(self):
        parent_path = self.component.parent.path_resolver.extract_path()
        self.path = parent_path + "\\" + self.component.shortname

    def extract_path(self):
        if self.path == "":
            self.compute_path()
        return self.path


class I2B2OntologyElement:
    def set_level(self):
        self.level = self.c_path.count("\\")

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
    def __init__(self, concept:Concept, parent=None):
        self.concept = concept
        self.parent = parent
        self.basecodehdler = BasecodeHandler(self.concept)
        self.path_handler = I2B2PathResolver(self.concept)

    def get_concept_details(self):
        pass

    def get_lines(self):
        pass

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

    def filter_obsfact(self, toignore=OBSERVATION_INFO):
        """
        Fetch the properties of self (referencing self as domain). Keep only the ontology properties (that should appear in the hierarchy) and return them.
        In particular, discard (default) the dates, patient number, clinical site ID, encounter ID.
        """
        modifiers = []
        for attr in self.concept.list_properties():
            if attr.identifier not in toignore:
                modifiers.append(I2B2Modifier(attr, self))  # todo change this, weird af
        return modifiers


class BasecodeHandler:
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
    def __init__(self, resource, i2b2concept, parent=None):
        self.property = Property(resource)
        self.basecodehdler = BasecodeHandler(self.concept)
        self.path_handler = I2B2PathResolver(self.concept)
        self.applied_concept = i2b2concept
        self.parent = parent
        self.basecode = self.reduce_basecode(prefix=parent.basecode)
