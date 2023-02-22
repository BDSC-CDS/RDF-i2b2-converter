"""
Simple utility standalone functions.
"""
from typing import List
import pandas as pd
import pdb
import hashlib
import glob
import rdflib
import json
import os
import sys
import datetime
import gc

TERMINOLOGIES_FILES = {}


def read_config(confpath):
    """
    Read the specified config file.
    Expected keyword for the JSON dictionary and associated behaviours are:
        - "uris": all items are cast to a rdflib.URIRef string representation
    Other elements are read as strings, except integers and boolean (cast as such)
    """
    with open(confpath) as ffile:
        parsed_config = json.load(ffile)
        for key, val in parsed_config.items():
            if val == "True" or val == "False":
                parsed_config[key] = val == "True"
            elif key == "uris":
                for key2, val2 in val.items():
                    val[key2] = (
                        rdflib.URIRef(val2)
                        if isinstance(val2, str)
                        else [rdflib.URIRef(k) for k in val2]
                    )
                assert 0
            elif isinstance(val, str) and val.isnumeric():
                val = int(val)
    return parsed_config


class GraphParser:
    """
    A class for file discovery and management of in-memory graphs loading
    """

    def __init__(self, paths, rdf_format, terminologies_links):
        """
        :param rdf_format The RDF format the users want to take into account,
                            should be '*' or a proper RDF format.
                            Mismatches with rdflib's guessed format will be ignored.
        """
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
            # Check the format parameter matches something doable
            guessed_format = rdflib.util.guess_format(filek)
            if guessed_format is None or (
                rdf_format != "*" and guessed_format != rdf_format
            ):
                print("Couldn't parse file", filek, ", skipping")
                continue
            dot = filek.rfind(".")
            slash = filek.rfind("/")
            fname = filek[slash + 1 : dot]
            if fname in terminologies_links.values():
                print("Creating a dedicated graph for", fname)
                cur = rdflib.Graph()
                cur.parse(filek, format=guessed_format)
                TERMINOLOGIES_FILES.update({fname: cur})
            else:
                print("adding to the main graph: ", fname)
                self.graph.parse(filek, format=guessed_format)
        print("Graph is fully loaded in memory.")

    def define_namespaces(self):
        """
        To check (iterating through the namespace generator apparently activates them?)
        """
        return list(self.graph.namespace_manager.namespaces())

    def get_entrypoints(self, entrypoints: List[rdflib.URIRef]):
        """
        Collect the resources associated to the specified entrypoints.
        """
        return list(self.graph.resource(uri) for uri in entrypoints)

    def free_memory(self):
        """
        Free the memory. To trigger when graph operations are over.
        """
        del self.graph
        TERMINOLOGIES_FILES.clear()
        gc.collect()


class I2B2BasecodeHandler:
    """
    Compute and extract the basecode for a Class or a Property existing in the ontology.
    Access the attributes of the embedded RDF resource.
    If a value is specified, it will be included in the basecode computation.
    If an other handler is specified as "logical_parent" at construction, its code
        will be embedded in the computation. (helps encapsulating hierarchy in codes)
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
        return self.reduce_basecode(rdf_uri=self.core, prefix=self.prefix, debug=False)

    def reduce_basecode(self, rdf_uri, prefix, debug=False, cap=50):
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


def terminology_indicator(resource, terminologies_graphs):
    """
    Determine if it is worth looking for properties of this concept or not.
    In the SPHN implementation, if the concept comes from a terminology (testable easily by looking at the URI) it doesn't have any properties
    """
    return any([k in resource.identifier for k in terminologies_graphs.keys()])


def which_graph(uri, terminologies_graphs):
    for key in terminologies_graphs.keys():
        if key in uri and terminologies_graphs[key] in terminologies_files.keys():
            res = terminologies_files[terminologies_graphs[key]]
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


def merge_roots(target_file):
    df = pd.read_csv(target_file)
    df = df.replace([":Concept"], ["sphn:SPHNConcept"], regex=True)
    lvl = df.loc[df["C_HLEVEL"] == 0]
    if len(lvl) > 1:
        lvl = lvl.drop(lvl.iloc[[0]].index)
        df = df.drop(lvl.index)
    df.fillna("").to_csv(
        path_or_buf=target_file,
        mode="w",
        header=True,
        index=False,
    )


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
