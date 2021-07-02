import rdflib
import hashlib
import json
import pdb

"""
This file features RDF functions used by both the ontology builder and the data loader, but also other utility functions that do not fit 
in the low-level "utils" file, typically functions triggering the external terminologies search.
"""

ROOT = "\\SPHNv2020.1\\"
SPHN = rdflib.Namespace("http://sphn.ch/rdf/ontology/")
RDF = rdflib.Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
RDFS = rdflib.Namespace("http://www.w3.org/2000/01/rdf-schema#")
SPO = rdflib.Namespace("http://sphn.ch/rdf/spo/ontology/")
OWL = rdflib.Namespace("http://www.w3.org/2002/07/owl#")
XSD = rdflib.Namespace("http://www.w3.org/2001/XMLSchema#")
RESOURCE = rdflib.Namespace("http://sphn.ch/rdf/resource/")

with open("files/ontology_config.json") as ff:
    config = json.load(ff) 
for key, val in config["parameters"].items():
    globals()[key] = val
for key, val in config["uris"].items():
    globals()[key] = val

with open("files/i2b2_rdf_mapping.json") as ff:
    config = json.load(ff) 
for key, val in config.items():
    globals()[key] = val



def rname(uri, graph):
    full = graph.qname(uri)
    return full[full.find(":") + 1 :]

def format_global(ontograph=ONTOLOGY_GRAPH_LOCATION, to_filter=[]):
    """
    Utility functions allowing to use substrings in the BLACKLIST definition instead of correct resource names.
    Overwrite the BLACKLIST by finding the matching (shortened) URIs.
    """
    graph = rdflib.Graph()
    graph.parse(ontograph, format="turtle")
    ress = graph.query(
        """
        SELECT DISTINCT ?s
        WHERE {
            ?s rdf:type ?u
        }
        """
    )
    forbidden = []
    for i in range(len(to_filter)):
        for subject in ress:
            if to_filter[i] in subject[0]:
                forbidden.append(rname(subject[0], graph))
    return forbidden

""" 
BLACKLIST = format_global(to_filter=BLACKLIST_TOSORT)
DEACTIVATE_VALUESET = format_global(to_filter=DEACTIVATE_VALUESET_TOSORT)
EXCLUDED_COMPONENT = list(set(ABSTRACT_CLASSES+BLACKLIST+OBSERVATION_INFO)) """

class Component:
    """
    Component is a wrapper for the rdflib.Resource class.
    """
    def __init__(self, resource):
        self.resource = resource
        self.shortname = rname(resource.identifier, resource.graph)
        self.set_label()


    def set_label(self):
        """
        Set the language-dependent label (to be used as display name)
        """
        labels = self.resource.graph.preferredLabel(self.resource.identifier, lang=PREF_LANGUAGE)
        if len(labels)>0: 
            self.label = labels[0].toPython()
            return
    
        # If the resource had no language-tagged label, get the normal label. If it does not exist, say the label will be the URI suffix
        fmtd_label = self.resource.graph.label(self.resource.identifier)
        self.label = self.shortname if fmtd_label == '' else fmtd_label.toPython()

    def __repr__(self):
        return self.__class__.__name__+"(" + self.resource.graph.namespace_manager.normalizeUri(self.resource.identifier)+ ")"

class Concept(Component):
    def __init__(self, resource):
        super().__init__(resource)
        self.subconcepts = []
        self.properties = []

    def extract_ontology_properties(self):
        return self.filter_obsfact()

    def explore_children(self):
        resolver = OntologyDepthExplorer(self)
        self.subconcepts.extend(resolver.explore_subclasses())
        self.properties.extend(resolver.explore_properties())

        # Trigger recursive call on first-level children
        for subc in self.subconcepts:
            subc.explore_children()
        for predicate in self.properties:
            predicate.explore_ranges()



class Property(Component):    
    def __init__(self, resource):
        super().__init__(resource)
        self.concept = None 
        self.ranges = []

    def explore_ranges(self):
        to_expand = self.mute_ranges()
        for obj in self.ranges:
            # The explore method will trigger subclasses and properties discovery
            obj.explore_children()

    def mute_ranges(self):
        """
        Overwrite the "subconcepts" attribute of some Concepts stored in self.range. 
        Comsequence will be that concept.explore_children() will only return properties of such muted concepts.
        The overwriting rule is typically dependent on the RDF implementation. Set the config variable ALWAYS_DEEP to True to deactivate this filter.

        In the SPHN implementation, we want to expand an property range node into its subclasses if and only if 
        it is a descendant of a sphn:Terminology and is the only of its kind (same prefix) in the ranges list.
        To achieve this, we find the ranges having "terminology brothers" and mute their subconcepts. 

        """
        if ALWAYS_DEEP:
            return 0
        muted_total = 0
        
        # Extract the indices of self.ranges which belong to an external terminology
        termins = [(elem, RDFS.subClassOf*OneOrMore, TERMINOLOGY_MARKER_URI) in self.resource.graph for elem in self.ranges]
        idx_termsinrange = [indx for indx,truth_val in enumerate(termins) if x]

        # Now count occurrence of each specific terminology
        counts={}
        for cur_idx in idx_termsinrange:
            cur_terminology = self.resource.graph.qname(self.ranges[cur_idx])
            if cur_terminology in counts.keys():
                counts[cur_terminology]=counts[cur_terminology]+1
            else:
                counts[cur_terminology]=1

        # Now search in self.ranges which range belong to an ontology and have brother in it.
        # When found, prune its subconcepts so it cannot be expanded
        for rn_idx in range(len(self.ranges)):
            if rn_idx in idx_termsinrange :
                if counts[self.resource.graph(qname(self.ranges[rn_idx]))]>1:
                    self.ranges[rn_idx].subconcepts=[]
                    muted_total=muted_total+1
        return muted_total

    def addrange(self, range_list):
        self.ranges.extend(range_list)

    def extract_range_type(self):
        """
        Return the range type of the property, expanding the bnode if any. 
        The return value is a list.
        """
        response = self.resource.graph.query("""
        SELECT DISTINCT ?class 
        where {
            {
            ?self rdfs:range ?class }
            union
            {
                ?self rdfs:range [ a owl:Class ;
                                    owl:unionOf [ rdf:rest*/rdf:first ?class ]
                ]
                    }
        }
        """, initBindings={"self":self.resource.identifier})
        listed_res = [self.resource.graph.resource(row[0]) for row in response]
        # If there are several ranges, remove the first element which is in fact the name of the blank node
        if len(listed_res)>1:
            listed_res=listed_res[1:]
        return listed_res
        

class PropertyFilter:
    """
    Handle the property extraction for a concept.
    """
    def __init__(self, concept):
        self.concept = concept

    def filter_properties(self, properties):
        """
        Discard all blacklisted properties.
        Update the range attribute of each Property object so it embeds all the range objects.
        """
        def filter_valid(res_list):
            # Discards elements referenced in the blacklist, proceed with the other
            filtered = [item for item in res_list if item.identifier.toPython() not in BLACKLIST]
            return filtered

        properties_clean = []
        # Loop over Properties, check they are not blacklisted and not all their ranges are blacklisted
        for el in properties : 
            # If the predicate is blacklisted, skip 
            if filter_valid([el.resource])!=[el.resource]: 
                continue
            rnge_type = el.extract_range_type()
            # If the predicate is not blacklisted and has at least one non-blacklisted range, add it 
            if filter_valid([el.resource])==[el.resource]: 
                rnges = filter_valid(rnge_type)
                if len(rnges)>0:
                    el.addrange([Concept(obj) for obj in rnges])
                    properties_clean.append(el)
        return properties_clean

    def list_unique_properties(self):
        """
        Extract the (predicate, object TYPE) couples for predicates of a resource.
        Extracts only finest properties, which means if two properties are related (hierarchy), only the most specific is kept.
        """
        self_res = self.concept.resource
        response = self_res.graph.query("""
            SELECT ?p 
            WHERE {
                ?p rdfs:domain ?self .
                FILTER NOT EXISTS {
                    ?child rdfs:domain ?self .
                    ?p rdfs:subPropertyOf+ ?child 
                }
            }
        """, initBindings={"self":self_res.identifier})

        # Extract all resources referencing this class as their domain
        return [Property(self_res.graph.resource(row[0])) for row in response]

class OntologyDepthExplorer:
    """
    Constructed by a Concept. Fetch the subgraph spanned from this concept.
    All searches are done recursively, fetching only the first level of children and creating an other OntologyDepthExplorer object for 
    """
    def __init__(self, concept):
        self.concept = concept
        self.filter = PropertyFilter(concept)
    
    def explore_subclasses(self):
        """
        Fetch the direct subclasses of the concept.
        """
        subs = self.concept.resource.subjects(RDFS.subClassOf)
        return [Concept(sub) for sub in subs if sub.identifier not in BLACKLIST]

    def explore_properties(self, entrypoint=None):
        """
        Fetch the properties 
        """
        if entrypoint is None:
            entrypoint = self.concept
        all_properties = self.filter.list_unique_properties()
        return self.filter.filter_properties(all_properties)


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
        return  {
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
            "c_symbol": self.c_path[len(self.parent.c_path):],
            "c_metadataxml": ""}
    
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
    def __init__(self, resource, parent = None):
        self.concept = Concept(resource)
        self.basecodehdler = BasecodeHandler(self.concept)
        self.path_handler = I2B2PathResolver(self.concept)
        self.parent = parent

    def get_concept_details(self):
        pass

    def get_lines(self):
        pass

    def get_info (self):
        info = super().get_info()
        info.update({
            "c_facttablecolumn": "CONCEPT_CD",
            "c_tablename": "CONCEPT_DIMENSION",
            "c_columnname": "CONCEPT_PATH",
            "c_columndatatype": "T",
            "c_operator": "LIKE",
            "c_visualattributes": "FA",
            "m_applied_path": "@",})
        return info

    def filter_obsfact(self, toignore=OBSERVATION_INFO):
        """
        Fetch the properties of self (referencing self as domain). Keep only the ontology properties (that should appear in the hierarchy) and return them.
        In particular, discard (default) the dates, patient number, clinical site ID, encounter ID.
        """
        modifiers = []
        for attr in self.concept.list_properties():
            if attr.identifier not in toignore:
                modifiers.append(I2B2Modifier(attr, self)) #todo change this, weird af
        return modifiers
    
class BasecodeHandler():
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
    
def setup():
    """
    Load the ontology graph in memory. 
    Extract the instantiable Concepts ; a Concept is a RDF element of type owl:Class, NOT specified as "abstract" by the ABSTRACT_CLASSES macro.
    It reflects the elements in the hierarchy that are instantiable on their own (compared to properties, never instantiated alone)
    """
    g = rdflib.Graph()
    g.parse(ONTOLOGY_GRAPH_LOCATION, format=RDF_FORMAT)
    classes_uris = list_all_classes_uri(g)
    primaries = [I2B2Concept(g.resource(k)) for k in classes_uris if k not in EXCLUDED_COMPONENT]
    db_lines = []
    for element in primaries:
        db_lines.append(element.get_lines())
    return db_lines


def unify_graph(graphs=DATA_GRAPHS_LOCATION):
    g = rdflib.Graph()
    for target in graphs:
        try:
            g.parse(target, format=RDF_FORMAT)
        except:
            print("file not parsed:" + target)
    return g


def classname_to_resource(classname, classdict):
    clean = classname.replace(" ", "")
    clean = clean.replace("-", "")
    for el in classdict.keys():
        if clean.lower() in el.lower():
            return classdict[el]
    raise Exception("class not found : " + classname)




def disc_valuesets(graph=ONTOLOGY_GRAPH_LOCATION):
    """
    Extract a list of elements in the ontology graph that reference a specific valueset.
    This is needed because at the data loading, free text values are not stored and queried in the same fashion than elements of a known finite set.
    """
    items = []
    g = rdflib.Graph()
    g.parse(graph, format=RDF_FORMAT)
    res = g.query(
        """
        SELECT DISTINCT ?s 
        WHERE {
            ?s sphn:valueset ?o
        }
        ORDER by ?s
        """
    )
    for el in res:
        items.append(rname(el[0], g))
    return items


def extract_valueset(concept): # todo fix
    g = concept.graph
    resp = g.query("""
        select * where { 
            ?concept rdfs:subClassOf sphn:ValueSet FILTER(CONTAINS(STR(?concept),"sphn")) .
            ?value_set rdf:type ?concept 
                FILTER(?concept=sphn:FOPHDiagnosis_rank) . # sphn:Death_status
        } order by ?concept ?value_set
    """)
    for row in resp:
        pass
    return resp


def detect_toextend(vname):
    """
    This function decides if the passed value is a literal to be used as such or if it needs to be expanded/interpreted.

    Basically a way to bypass the Code RDF abstraction by using a "@" prefix
    Ex : this function should return 0 if applied to elements of [alive, dead] but return 1 if called on "ICD-10 Diagnosis" or "Author statement".
    Ideally, a different code would indicate which type of extension is needed
    """
    # Could replace this function by adding a flag on the value, ex @ICD10 or @
    if not "@" in vname:
        return None, ""
    if "child of" in vname:
        vname = format_vsetdescr(vname)
    return vname[vname.find("@") + 1 :], vname[: vname.find("@")]




def format_vsetdescr(vname):
    """
    Parses the human-readable valueset codes such as "SNOMED code child of 197425".
    """
    sep = vname.split(" ")
    dicname = 0
    for word in sep:
        if "@" in word:
            dicname = word
            continue
        try:
            int(word)
            if dicname:
                return dicname + "." + word
            else:
                continue
        except:
            continue


def remove_prefix(modifier_rsc):
    """
    Add a :: delimiter after redundant information in properties names.
    Useful only if properties are formatted like

    ?s rdf:label :SomeConcept-property
    ?s rdfs:domain :SomeConcept
    """
    full_label = modifier_rsc.label().toPython()
    concept_short = rname(
        modifier_rsc.value(RDFS.domain).identifier, modifier_rsc.graph
    )
    if concept_short in full_label:
        idx = full_label.find(concept_short)
        return (
            full_label[: idx + len(concept_short)]
            + "::"
            + full_label[idx + len(concept_short) + 1 :]
        )


def list_all_classes_uri(g):
    """
    Return the list of all the owl:Class-typed elements descending from the root concept, as URIs.
    """
    res = g.query(
        """
            SELECT ?s
            WHERE {
                    ?s rdf:type owl:Class .
                    ?s rdfs:subClassOf+ ?root
            }
            ORDER by ?s
            """, initBindings={"root":ROOT_URI}
    )
    return [row[0] for row in res]




def sort_by_concept(datagraph, onto_g):
    """
    1. SPARQL query for all instantiable concepts (not only the primary ones)
    2. For each, create a graph of all its instances and their predicates + objects
    3. Return the list of graphs
    """
    classes = list_classes_simple(onto_g)
    classes = [e[0] for e in classes]
    sorted_dic = {}
    for element in classes:
        split = datagraph.query(
            """
            SELECT ?i ?p ?o
            WHERE
            {
                ?i rdf:type ?cl .
                ?i ?p ?o
            }
            ORDER BY ?i
            """,
            initBindings={"cl": element},
        )
        if len(split) == 0:
            pass
        else:
            toadd = []
            G = rdflib.Graph()
            G.bind("sphn", SPHN)
            G.bind("resource", RESOURCE)
            G.bind("spo", SPO)
            for e in split:
                quad = list(e)
                quad.append(G)
                toadd.append(quad)
            else:
                G.addN(toadd)
                sorted_dic.update({rname(element, onto_g): G})
    return sorted_dic


def split_graph(obs_g, onto_g, output_template=""):
    graphs = sort_by_concept(obs_g, onto_g)
    for concept, graph in graphs.items():
        with open(output_template + concept + ".ttl", "w") as ff:
            ff.write(graph.serialize(format="turtle").decode("utf-8"))


def check_deadlinks_L123(obs_g):
    to_check = obs_g.query(
        """
        SELECT DISTINCT ?o
        WHERE {
            {
                ?s ?L1 ?o
            }
            UNION
            {
                ?s ?L2 ?o
            }
            UNION
            {
                ?s ?L3 ?o
            }
        }
        """,
        initBindings={
            "L1": SPHN["L1-data_provider_institute"],
            "L2": SPHN["L2-subject_pseudo_identifier"],
            "L3": SPHN["L3-encounter"],
        },
    )
    all_deadlinks = [obs_g.namespace_manager.normalizeUri(e[0]) for e in to_check]
    with open("files/data/log_deadL123", "w") as ff:
        json.dump(all_deadlinks, ff)


def bootstrap_counts(graph, *args):
    table = {}
    for variable in args:
        temp = {}
        nums = graph.query(
            """
            SELECT DISTINCT ?o
            WHERE {
                ?u ?t ?o
            }
            """,
            initBindings={"t": variable},
        )
        nums = [e[0] for e in nums]
        temp.update(
            {rname(nums[index], graph): index + 1 for index in range(len(nums))}
        )
        table.update({variable: temp.copy()})
        with open("lookup_table_" + rname(variable, graph), "w") as ff:
            json.dump(temp, ff)
    return table


def walk_RDF_terminology(codename):
    def uri_to_nameblock(uri, graph):
        name = graph.prefLabel(uri, lang=PREF_LANGUAGE)
        nameblock = "(" + uri.toPython().split("/")[-1] + ") " + name[0][1].toPython()

    filename = [
        el for el in os.listdir(EXTERNAL_LOCATION + "RDF/") if codename.lower() in el
    ][0]
    extension = filename.split(".")[-1]
    output = {}
    rdf_dic = {}
    onto = rdflib.Graph()
    formt = "turtle" if extension == "ttl" else "xml"
    onto.parse(filename, format=formt)
    mother = onto.query(
        """
        SELECT ?s 
        WHERE ?s rdf:type rdfs:Class 
        FILTER NOT EXISTS ?s ?rdfs:subClassOf
    """
    )
    for el in mother:
        uri = el[0]
        path = "\\" + uri_to_nameblock(uri)
    output.update({path: "True"})
    rdf_dic.update({uri: path})
    pass

    # itératif: commencer par celui qui n'a pas de subclassof, puis aller au niveau 1, puis 2, etc.
    # Créer le chemin en concaténant le label (choisir langue) au chemin du parent
    # Garder un dictionnaire qui lie chaque URI de classe à son chemin (incluant ses parents). Ajouter au dic de sortie ce chemin
    pass
