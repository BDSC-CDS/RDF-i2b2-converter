import pandas as pd
import pdb
import hashlib
import glob
import rdflib
import json, os, sys, datetime
import gc

""""
This file figures file and format utility functions.
It initializes global variables by reading the "ontology_config" file.
"""

###########
GRAPH_CONFIG = "/config/graph_config.json"
I2B2_MAPPING = "/config/i2b2_rdf_config.json"
DATA_CONFIG = "/config/data_config.json"

with open(GRAPH_CONFIG) as ff:
    config = json.load(ff)
for key, val in config["parameters"].items():
    globals()[key] = val
for key, val in config["uris"].items():
    globals()[key] = (
        rdflib.URIRef(val) if type(val) == str else [rdflib.URIRef(k) for k in val]
    )

with open(I2B2_MAPPING) as ff:
    config = json.load(ff)
for key, val in config.items():
    globals()[key] = val if val != "False" else False

with open(DATA_CONFIG) as ff:
    config = json.load(ff)
for key, val in config.items():
    val = int(val) if type(val) == str and val.isnumeric() else val
    globals()[key] = val
for key, val in config["data_global_uris"].items():
    globals()[key] = (
        rdflib.URIRef(val) if type(val) == str else [rdflib.URIRef(k) for k in val]
    )


SUBCLASS_PRED = rdflib.URIRef(SUBCLASS_PREDICATE_URI)
TERMINOLOGIES_FILES = {}


class GraphParser:
    def __init__(self, paths):
        self.graph = rdflib.Graph()
        result = []
        for pathi in paths:
            if not os.path.exists(pathi):
                continue
            if os.path.isfile(pathi):
                result.append(pathi)
                continue
            result.extend(glob.glob(pathi + "/**/*", recursive=True))
        for filek in result:
            rdf_format = rdflib.util.guess_format(filek)
            if rdf_format is None or (RDF_FORMAT != "*" and rdf_format != RDF_FORMAT):
                print("Couldn't parse file", filek, ", skipping")
                continue
            dot = filek.rfind(".")
            slash = filek.rfind("/")
            fname = filek[slash + 1 : dot]
            if fname in TERMINOLOGIES_GRAPHS.values():
                print("Creating a dedicated graph for", fname)
                cur = rdflib.Graph()
                cur.parse(filek, format=rdf_format)
                TERMINOLOGIES_FILES.update({fname: cur})
            else:
                print("adding to the main graph: ", fname)
                self.graph.parse(filek, format=rdf_format)
        print("Graph is fully loaded in memory.")

    def define_namespaces(self):
        ns = [e for e in self.graph.namespace_manager.namespaces()]
        return ns

    def get_entrypoints(self, list=ROOT_URIS):
        return [self.graph.resource(uri) for uri in list]

    def free_memory(self):
        del self.graph
        gc.collect()


class I2B2BasecodeHandler:
    """
    Compute and extract the basecode for a Class or a Property existing in the ontology.
    Access the attributes of the embedded RDF resource.
    If a value is specified, it will be included in the basecode computation.
    If an other handler is specified as "ph" at construction, its code will be embedded in the computation. (this helps encapsulating hierarchy in codes)
    """

    def __init__(self, i2b2element=None):
        self.basecode = None
        if i2b2element is not None:
            self.core = i2b2element.component.get_uri()
            self.prefix = (
                i2b2element.logical_parent.basecode_handler.get_basecode()
                if i2b2element.logical_parent is not None
                else ""
            )

    def get_basecode(self):
        if self.basecode is not None:
            return self.basecode
        return self.reduce_basecode(rdf_uri=self.core, prefix=self.prefix, debug=DEBUG)

    def reduce_basecode(self, rdf_uri, prefix, debug=False, cap=MAX_BASECODE_LENGTH):
        """
        Returns a basecode for self.component. A prefix and a value can be added in the hash.
        The code is made from the URI of the RDF ontology concept, which is an info that does not depend on the ontology converter's output.
        A basecode is invisible to the user, and its only constraints is to be unique regarding the concept it is describing,
        and to be computable both from the ontology side and from the data loader side.
        The resulting code is the joining key between data tables and ontology tables.
        """
        if type(rdf_uri) == rdflib.URIRef:
            rdf_uri = rdf_uri.toPython()
        if rdf_uri != "" and rdf_uri[-1] != "\\":
            rdf_uri = rdf_uri + "\\"

        to_hash = rdf_uri
        to_hash = prefix + to_hash
        return to_hash if debug else hashlib.sha256(to_hash.encode()).hexdigest()[:cap]


def rname(uri, graph):
    full = graph.qname(uri)
    return full[full.find(":") + 1 :]


def create_dir(relative_path):
    # Create the directory if it doesn't already exist.
    return os.makedirs(relative_path) if not os.path.exists(relative_path) else 0


def terminology_indicator(resource):
    """
    Determine if it is worth looking for properties of this concept or not.
    In the SPHN implementation, if the concept comes from a terminology (testable easily by looking at the URI) it doesn't have any properties
    """
    return any([k in resource.identifier for k in TERMINOLOGIES_GRAPHS.keys()])


def which_graph(uri):
    for key in TERMINOLOGIES_GRAPHS.keys():
        if key in uri and TERMINOLOGIES_GRAPHS[key] in TERMINOLOGIES_FILES.keys():
            res = TERMINOLOGIES_FILES[TERMINOLOGIES_GRAPHS[key]]
            return res if res != "" and res is not None else False
    return False


def check_domains(resource):
    dom = resource.value(rdflib.RDFS.domain)
    if resource.graph.resource(rdflib.OWL.unionOf) in dom.predicates():
        print(
            "Warning: ",
            resource,
            "is bound to a collection of domains and was not overwritten by a more specific item.",
        )


def shortname(resource):
    """
    Reduce the resource URI.
    In most cases the rdflib reasoner is able to do it, but in case it fails this method will do it explicitly.
    The protocol is finding the namespaces reduction that reduces the most the item and decide this is the prefix.
    """
    shortname = resource.graph.namespace_manager.normalizeUri(resource.identifier)
    uri = resource.identifier
    if uri in shortname:
        ns = resource.graph.namespace_manager.namespaces()
        best_guess_len = 0
        for key, value in ns:
            if value in uri and len(value) > best_guess_len:
                best_guess_len = len(value)
                shortname = key + ":" + uri[len(value) :]
    return shortname


def format_date(rdfdate, generalize=True):
    """
    Format an RDF xsd:date resource onto a "timestamp without time zone" string, readable in postgres.
    If the kyarg "generalize" is set, replace the date by Jan 1st
    """
    mydate = rdfdate.toPython()
    if generalize:
        mydate = datetime.date(mydate.year, 1, 1)
    elif type(mydate) != datetime.date:
        mydate = mydate.date()
    cpy = mydate.__str__()
    return cpy + " 00:00:00"


def wipe_directory(dir_path, names=[]):
    """
    Delete files in the given directory.
    """
    if names == []:
        names = os.listdir(dir_path)
    for k in names:
        os.remove(dir_path + k)
        print("Removed file: ", dir_path + k)


def db_to_csv(db, filename, init=False, columns=[]):
    """
    Simple tool writing a list of dictionaries with matching keys to a csv database-ready file.
    Argument is only the target filename, will be written in the output_tables directory.
    """
    df = pd.DataFrame(db, columns=columns) if columns != [] else pd.DataFrame(db)
    mode = "w" if init else "a"
    header = init
    df.fillna("").to_csv(path_or_buf=filename, mode=mode, header=header, index=False)


def from_csv(filename):
    """
    Read the csv database and outputs a list of dicts.
    Argument is the full relative path.
    """
    db = pd.read_csv(filename)
    return db.to_dict("records")


def generate_xml(metadata_dict):
    """
    Generate the metadata_xml panel for a specific entry. Uses the XML_PATTERN and UNITS_DIC global variables.
    enum types are not used since we have explicit valuesets as modifier leaves.
    Default type is string but PosFloat, Integer, PosInteger, Float are accepted
    """
    keyz = metadata_dict.keys()
    res = XML_PATTERN
    for k in keyz:
        ftag = "<" + k + ">"
        etag = "</" + k + ">"
        val = metadata_dict[k]
        if val is None:
            continue
        if k == "EnumValues":
            enumstr = "".join(
                ['<Val description="">' + elem + "</Val>" for elem in val]
            )
            val = enumstr
        res = res.replace(ftag + etag, ftag + val + etag)
    return res


def remove_duplicates(dics):
    """
    Given an unordered list of dictionaries, return a shortened list without duplicates.
    d1 is a duplicate of d2 if d1 == d2.
    """
    return [dict(t) for t in {tuple(d.items()) for d in dics}]
