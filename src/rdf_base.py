import rdflib
import hashlib
import json
import sys, os
import pdb
from configs import *
from utils import *

"""
This file features RDF functions used by both the ontology builder and the data loader, but also other utility functions that do not fit 
in the low-level "utils" file, typically functions triggering the external terminologies search.
"""

# TODO uncomment this block when testing phase is finished (done in the test file to have it easily accessible)
""" myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + "/../src/")

ONTOLOGY_GRAPH = rdflib.Graph()
ONTOLOGY_GRAPH.parse(ONTOLOGY_GRAPH_LOCATION, format="turtle")
for file in os.listdir(TERMINOLOGIES_LOCATION):
    print("Adding " + file + " to the graph")
    ONTOLOGY_GRAPH.parse(TERMINOLOGIES_LOCATION + file, format="turtle")

def give_entry_concepts():
    return [ONTOLOGY_GRAPH.resource(e) for e in ENTRY_CONCEPTS]"""
SUBCLASS_PRED = rdflib.URIRef(SUBCLASS_PRED_URI)

class GraphParser:
    def __init__(self, paths):
        self.graph = rdflib.Graph()
        my_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../')
        for path in paths:
            print("Exploring directory "+ path)
            g_path = my_path + path
            if os.path.isdir(g_path):
                files = os.listdir(g_path)
                for filek in files:
                    print("Loading file: "+ filek)
                    dot=filek.rfind(".")
                    fname=filek[:dot]
                    if fname in TERMINOLOGIES_FILES.keys():
                        cur = rdflib.Graph()
                        cur.parse(g_path+filek, format="turtle")
                        TERMINOLOGIES_FILES.update({fname:cur})
                    else:
                        self.graph.parse(g_path+filek, format="turtle")
            else:
                self.graph.parse(g_path, format="turtle")
        print("Graph is fully loaded in memory.")

    def define_namespaces(self):
        ns = [e for e in self.graph.namespace_manager.namespaces()]
        return ns

    def get_entrypoints(self, list=[ROOT_URI]):
        # TODO :  support entrypoints other than root (i.e the ontology file should still work AND root line still be written)
        return [self.graph.resource(uri) for uri in list]

def rname(uri, graph):
    full = graph.qname(uri)
    return full[full.find(":") + 1 :]

def which_graph(uri):
    for key in TERMINOLOGIES_GRAPHS.keys():
        if key in uri:
            return TERMINOLOGIES_FILES[TERMINOLOGIES_GRAPHS[key]]
    return False


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


def extract_valueset(concept):  # todo fix
    g = concept.graph
    resp = g.query(
        """
        select * where { 
            ?concept rdfs:subClassOf sphn:ValueSet FILTER(CONTAINS(STR(?concept),"sphn")) .
            ?value_set rdf:type ?concept 
                FILTER(?concept=sphn:FOPHDiagnosis_rank) . # sphn:Death_status
        } order by ?concept ?value_set
    """
    )
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
            """,
        initBindings={"root": ROOT_URI},
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
