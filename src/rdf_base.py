from utils import *
import rdflib, hashlib

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


def rname(uri, graph):
    full = graph.qname(uri)
    return full[full.find(":") + 1 :]


def format_global(ontograph=ONTOLOGY_GRAPH, to_filter=[]):
    """
    Utility functions allowing to use substrings in the CONCEPT_BLACKLIST definition instead of correct resource names.
    Overwrite the CONCEPT_BLACKLIST by finding the matching (shortened) URIs.
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


CONCEPT_BLACKLIST = format_global(to_filter=CONCEPT_BLACKLIST_TOSORT)
DEACTIVATE_VALUESET = format_global(to_filter=DEACTIVATE_VALUESET_TOSORT)


def setup():
    g = rdflib.Graph()
    g.parse(ONTOLOGY_GRAPH, format=RDF_FORMAT)

    classes = list_classes_simple(g)
    classesdict = resources_lookup(classes, g)
    narrowkeys = classesdict.keys()
    primaries_dict = {
        k: classesdict[k]
        for k in classesdict.keys()
        if k not in ABSTRACT_CLASSES and k not in CONCEPT_BLACKLIST and k in narrowkeys
    }
    return primaries_dict


def unify_graph(graphs=DATA_GRAPHS):
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


def filter_obsfact(attributes, toignore=OBSERVATION_INFO):
    """
    Takes as input a list of predicates. Filters the ones belonging in the ontology table from the observation_fact data such as units, datetime, Data provider and subject ID info.
    """
    modifiers = []
    for attr in attributes:
        idd = rname(attr.identifier, attr.graph)
        if idd == RDF.type or any([el in idd for el in toignore]):
            continue
        modifiers.append(attr)
    return modifiers


def disc_valuesets(graph=ONTOLOGY_GRAPH):
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


def extract_raw_valueset(res):
    """
    Extracts an explicit valueset from a formatted string to a list of string values.
    """
    if type(res) == rdflib.graph.Resource:
        raw = res.value(SPHN.valueset)
    else:
        raw = res
    if "[" in raw:
        raw = raw[1:-1]
    items = raw.split(";")
    return items


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


def reduce_basecode(path, cap=49, value="", debug=False):
    """
    Returns a basecode for the specified path. A value can be added in the hash.
    The path is traditionally the URI of the RDF concept, which is the only info shared by both the data file and the ontology file.
    This basecode is used to join the ontology table (storing the concept and modifier paths) and the observations.

    A basecode is invisible to the user, and its only constraints is to be unique regarding the concept it is describing,
    and to be computable both from the ontology side and from the data loader side.
    The resulting code is the joining key between data tables and ontology tables.
    """
    if len(value) > 0 and value[0] == "\\":
        value = value[1:]
    if path[-1] != "\\":
        path = path + "\\"
    tohash = path + value
    return tohash if debug else hashlib.sha256(tohash.encode()).hexdigest()[:cap]


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


def resources_lookup(classes, g):
    reslist = {}
    # Build a lookup table between class names and their Resource object
    for item in classes:
        elemURI = item.asdict()["s"]
        literal = rname(elemURI, g)
        reslist.update({literal: g.resource(elemURI)})
    # Read the csv file and update the resources using the class names as key
    return reslist


def list_classes_simple(g):
    res = g.query(
        """
            SELECT DISTINCT ?s
            WHERE {
                    ?s rdf:type owl:Class
            }
            ORDER by ?s
            """
    )
    return res


def list_modifiers(mainclass):
    """
    Returns a list of all the entities (as resources) that reference the class passed as parameter, as value for their rdfs:domain predicate.
    """

    def shortname(uri):
        rsc = mainclass.graph.resource(uri)
        rnge = rsc.value(RDFS.range)
        return rname(rnge.identifier, rsc.graph)

    res = mainclass.graph.query(
        """
            SELECT DISTINCT ?m
            WHERE {?m rdfs:domain ?mainclass}
            
            """,
        initBindings={"mainclass": mainclass.identifier},
    )
    modifiers = [
        mainclass.graph.resource(el[0])
        for el in res
        if shortname(el[0]) not in CONCEPT_BLACKLIST
        and rname(el[0], mainclass.graph) not in CONCEPT_BLACKLIST
    ]
    return modifiers


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
