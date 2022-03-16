from rdfwrappers import *
import hashlib


def drop(attribute):
    """
    If the attribute should be dropped because it points to a class referenced in the config file, skip it
    """
    for rn in attribute.get_children(verbose=False):
        cur_uri = rn.get_uri()
        if cur_uri in ONTOLOGY_DROP_DIC.values():
            return cur_uri
    return False


def save_from_drop(droppy_uri, parent_uri):
    """
    Handles corner cases for attributes that are normally dropped but are exceptionally kept.
    """
    if droppy_uri in UNDROP_LEAVES.keys() and parent_uri in UNDROP_LEAVES[droppy_uri]:
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
        They are found by navigating through the subconcepts tree (ignoring the properties) defined in rdfwrappers.
        """
        self.i2b2voidconcepts = []
        self.left_tosearch = []
        # Exploring into our concept
        cur = I2B2Concept(concept, i2b2parent)
        concept.get_entry_desc()
        # If it's not a directory, register it to be modifier-expanded. Stop condition of the recursion.
        if concept.subconcepts == []:
            self.left_tosearch.append(cur)
            return
        # Else, register it to be written out - but not expanded, and renew the operation on its subconcepts
        self.i2b2voidconcepts.append(cur)
        for sub in concept.subconcepts:
            next = I2B2Converter(sub, cur)
            self.left_tosearch.extend(next.left_tosearch)
            self.i2b2voidconcepts.extend(next.i2b2voidconcepts)

    def get_batch(self):
        # First flush the directory concepts (that do not span modifiers) if any
        self.towrite = [k.get_db_line() for k in self.i2b2voidconcepts]
        self.i2b2voidconcepts = []
        # Then taking the next real concept (spanning a modifier tree)
        if self.left_tosearch == []:
            return False
        cur = self.left_tosearch.pop()
        print("Exploring concept ", cur)
        cur.extract_modelems()
        self.towrite.extend([k.get_db_line() for k in [cur] + cur.modifiers])
        return True

    def write(self, filepath=OUTPUT_TABLES+"METADATA.csv", init_table=False):
        """
        Write all the db at once through a pandas dataframe.
        If updating this function to enable append mode, do not forget to make sure header is written exactly once in the file.
        """
        db_to_csv(self.towrite, filepath, init_table, columns=COLUMNS["METADATA"])


class I2B2OntologyElement:
    def __init__(self, graph_component, parent=None):
        self.parent = parent
        self.logical_parent = (
            parent if graph_component.get_logic_indicator() else parent.logical_parent
        )
        self.component = graph_component
        self.basecode_handler = I2B2BasecodeHandler(self)
        self.path_handler = I2B2PathResolver(self)
        self.set_root()
        self.set_path()
        self.set_code()
        self.set_displayname()
        self.set_level()
        self.set_comment()
        self.line_updates = {}
        self.visual = None

    def set_root(self):
        if self.parent is not None:
            if self.parent.root == "\\":
                self.root = self.parent.path
            else:
                self.root = self.parent.root
        else:
            self.root="\\"

    def set_path(self):
        self.path = self.path_handler.get_path()

    def set_displayname(self):
        self.displayname = self.component.get_label()

    def set_code(self):
        self.code = self.basecode_handler.get_basecode()

    def set_level(self):
        if self.parent is None:
            self.level = 0
        else:
            self.level = self.parent.level + 1

    def set_comment(self):
        self.comment = self.component.get_comment()

    def mutate_valueinfo(self, datatype_string):
        """
        Some observations can store a value. In that case, the corresponding ontology element should specify the value type both in the valuetype_cd and in the XML form.
        """
        ont_values_cells = EQUIVALENCES[datatype_string].copy()
        metadata = generate_xml(ont_values_cells["C_METADATAXML"])
        ont_values_cells.update({"C_METADATAXML": metadata})
        return ont_values_cells

    def walk_mtree(self, counter=0):
        res = []
        submods = self.get_filtered_children()
        for component in submods:
            if not self.absorb_child_info(component):
                cur = I2B2Modifier(
                    component, parent=self, applied_path=self.applied_path
                )
                new_counter = counter+1
                if new_counter<2:
                    print("Casting i2b2 object from ",component)
                next = cur.walk_mtree(counter=new_counter)
                res.append(cur)
                res.extend(next)
        return res

    def absorb_child_info(self, child_component):
        # Check if the child's URI is a primary data type. If so, its information does not justify creating a new ontology item but is written in the xml field.
        cur_uri = child_component.get_uri()
        if cur_uri in DATA_LEAVES.keys():
            # Merge the information in the xml panel of the current element, do not display the child
            self.line_updates = self.mutate_valueinfo(DATA_LEAVES[cur_uri])
            return True
        self.set_visual("folder")
        return False

    def get_db_line(self):
        # First gather the class-specific information
        spec_dir = self.get_class_info()
        # Then update them with the item-specific information
        spec_dir.update(self.line_updates)
        # Then update the base dict with this specific dir and return.

        if self.parent is not None:
            parpath = self.parent.path
        else:
            parpath = ""
        # Commented because i2b2 sets a limit on "symbol" len to 50
        # symbol = self.path[len(self.parent.path) :] 
        symbol=""
        res = {
            "C_HLEVEL": str(self.level),
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
        }
        res.update(spec_dir)
        return res

    def get_filtered_children(self):
        """
        Fetch the properties of self (referencing self as domain or subProperty). Keep only the ontology properties (that should appear in the hierarchy) and return them.
        In particular, discard (default) the dates, patient number, clinical site ID, encounter ID.
        """
        modifiers_tobe = []
        children = self.component.get_children()
        for attr in children:
            found_drop_criterion = drop(attr)
            if (not found_drop_criterion) or save_from_drop(
                found_drop_criterion, self.component.get_uri()
            ):
                modifiers_tobe.append(attr)
        return modifiers_tobe

    def __repr__(self):
        return self.__class__.__name__ + " at " + self.component.__repr__()


class I2B2Concept(I2B2OntologyElement):
    def extract_modelems(self):
        self.applied_path = self.path
        self.modifiers = self.walk_mtree()

    def get_root(self):
        return self.root

    def set_visual(self, typev):
        """
        Default is folder anyway for now (for concepts). For later uses can be switched to other visual attributes.
        """
        if typev == "folder":
            self.visual = "FA"
        elif typev == "hidden":
            self.visual = "LH"
        elif typev == "leaf":
            self.visual = "LA"

    def get_class_info(self):
        if self.visual is None:
            self.visual = "LA"
        info = {
            "C_FACTTABLECOLUMN": "CONCEPT_CD",
            "C_FULLNAME": self.path,
            "C_TABLENAME": "CONCEPT_DIMENSION",
            "C_COLUMNNAME": "CONCEPT_PATH",
            "C_COLUMNDATATYPE": "T",
            "C_OPERATOR": "LIKE",
            "C_VISUALATTRIBUTES": self.visual,
            "M_APPLIED_PATH": "@",
        }
        return info


class I2B2PathResolver:
    def __init__(self, i2b2ontelem):
        self.element = i2b2ontelem
        self.path = ""

    def get_path(self):
        if self.path == "":
            if self.element.parent is None:
                parent_path = self.element.get_root()
            else:
                parent_path = self.element.parent.path_handler.get_path()
            self.path = parent_path + self.element.component.get_shortname() + "\\"
        return self.path



class I2B2Modifier(I2B2OntologyElement):
    def __init__(self, component2, parent, applied_path):
        # Handle the case where a concept created self and registered as parent: discard (keep only modifier hierarchy)
        if parent.path == applied_path:
            self.applied_concept = parent
            parent = None
        else:
            self.applied_concept = parent.applied_concept

        super().__init__(component2, parent)
        self.applied_path = applied_path

    def set_level(self):
        """
        Modifier levels start at 1, not 0.
        """
        super().set_level()
        self.level = self.level+1 if self.level==0 else self.level

    def set_visual(self, typev):
        """
        Default is leaf, can be switched to folder. For later uses can be switched to other visual attributes.
        """
        if typev == "folder":
            self.visual = "DA"
        elif typev == "hidden":
            self.visual = "RH"
        elif typev == "leaf":
            self.visual = "RA"

    def get_root(self):
        return "\\" 

    def get_class_info(self):
        if self.visual is None:
            self.visual = "RA"
        info = {
            "C_FACTTABLECOLUMN": "MODIFIER_CD",
            "C_TABLENAME": "MODIFIER_DIMENSION",
            "C_FULLNAME": self.path,
            "C_COLUMNNAME": "MODIFIER_PATH",
            "C_COLUMNDATATYPE": "T",
            "C_OPERATOR": "LIKE",
            "C_VISUALATTRIBUTES": self.visual,
            "M_APPLIED_PATH": self.applied_path,
        }
        return info
