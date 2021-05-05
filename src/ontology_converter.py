from utils import *
from rdf_base import *
from xml_tools import *

"""
This file is the core of the ontology converter.
The pipeline is the following :
- setup() will extract the entrypoints to the RDF ontology graph
- gather_onto_lines() will trigger the following functions and return the database list:
    - extract_units will search in the data graph (if available) the units used for each numerical observation
    - filter_obsfact that filters out observation details that do not fit in the ontology context
    - add_single_line that extracts information from the current entrypoint only and stores it
    - add_modifiertree that extracts information from all the attributes tree spanned from the current entrypoint and stores it

If using i2b2, the returned database (as a list of dictionaries) should then be used as argument to extract_C_M_dim() to extract i2b2's star schema dimensions "CONCEPT_DIMENSION"
and "MODIFIER_DIMENSION".
If not using i2b2, the way gathered information is distributed along database fields should be modified in add_single_line and add_modifier_tree (in the dictionary update blocks).
"""


def gather_onto_lines(primaries_dict):
    """
    From the entry-level ontology concepts/modifier list, generates a csv file of ontology db lines (one for each concept and modifier).
    Makes use of predicate_to_ONtline().
    """
    db = []
    entryconcepts = [k for k in primaries_dict.values()]
    # Trigger the Units extraction with the ontology graph, the data graph and the list of valid classes
    extract_units(
        entryconcepts[0].graph,
        unify_graph(DATA_GRAPHS),
        [k.identifier for k in entryconcepts],
    )
    buffer = []
    for entry in entryconcepts:
        modifiers = filter_obsfact(list_modifiers(entry))
        # Add the concept line to the DB and retrieve the concept path (to reuse for each modifier
        lines = line_concept(entry)
        # Only add the concept to the db. Macro concept are stored in a buffer, will be sorted so there are no duplicates.
        db.extend(lines[-1:])
        conceptinfo = lines[-1]
        buffer.extend(lines[0:-1])
        cpath = conceptinfo["c_fullname"]
        cname = conceptinfo["c_name"]
        # Walk through the modifiers and add the lines to the DB
        for mod in modifiers:
            db.extend(add_modifiertree(mod, cpath, cname))
    # Add the macro concepts lines
    toadd = remove_duplicates(buffer)
    db.extend(toadd)
    db = sanitize(db, ["c_fullname", "m_applied_path", "c_dimcode"])
    return db


def line_concept(entry):
    """
    Create a db line as dictionary for a concept, using the single_line function.
    This function looks at potential macro-concepts and allows to use multi-leveled concepts.
    Return a list of dictionaries, one for each concept found on the way.
    """
    parent = entry.value(RDFS.subClassOf)
    if parent is None:
        return [single_line(entry, type="root")]
    line = []
    line.extend(line_concept(parent))
    for el in line:
        el["c_visualattributes"] = "FA"
    previous = line[-1]
    line.append(
        single_line(
            entry,
            type="concept",
            level=str(int(previous["c_hlevel"]) + 1),
            prefix=previous["c_fullname"],
        )
    )
    return line


def single_line(concept=None, type=None, level=1, prefix=ROOT):
    """
    Constructs a database line for a specific concept or the root element.
    The modifiers' dictionaries are set in the dedicated add_mtree function since this task is more complex.
    """
    if type == "root":
        return {
            "c_hlevel": "0",
            "c_fullname": prefix,
            "c_name": ONTOLOGY_NAME,
            "c_synonym_cd": "N",
            "c_visualattributes": "CA",
            "c_basecode": reduce_basecode(ROOT),
            "c_facttablecolumn": "CONCEPT_CD",
            "c_tablename": "CONCEPT_DIMENSION",
            "c_columnname": "CONCEPT_PATH",
            "c_columndatatype": "T",
            "c_operator": "LIKE",
            "c_comment": "",
            "c_dimcode": ROOT,
            "c_tooltip": "SPHN.2020.1",
            "m_applied_path": "@",
            "c_totalnum": "",
            "update_date": "",
            "download_date": "",
            "import_date": "",
            "sourcesystem_cd": "",
            "valuetype_cd": "",
            "m_exclusion_cd": "",
            "c_path": "",
            "c_symbol": "",
            "c_metadataxml": "",
        }
    elif type == "concept":
        c_name = concept.label().toPython()
        c_path = prefix + c_name
        c_name = add_spaces(c_name)
        comment = concept.value(RDFS.comment)
        c_bc = reduce_basecode(concept.identifier.toPython())
        if c_path[len(c_path) - 1] != "\\":
            c_path = c_path + "\\"
        return {
            "c_hlevel": str(level),
            "c_fullname": c_path,
            "c_name": c_name,
            "c_synonym_cd": "N",
            "c_visualattributes": "FA",
            "c_basecode": c_bc,
            "c_facttablecolumn": "CONCEPT_CD",
            "c_tablename": "CONCEPT_DIMENSION",
            "c_columnname": "CONCEPT_PATH",
            "c_columndatatype": "T",
            "c_operator": "LIKE",
            "c_comment": comment,
            "c_dimcode": c_path,
            "c_tooltip": "",
            "m_applied_path": "@",
            "c_totalnum": "",
            "update_date": "",
            "download_date": "",
            "import_date": "",
            "sourcesystem_cd": "",
            "valuetype_cd": "",
            "m_exclusion_cd": "",
            "c_path": "",
            "c_symbol": "",
            "c_metadataxml": "",
        }
    else:
        raise Exception("incorrect use of add_line")


def extract_C_M_dim(db):
    """
    Build two tables: i2b2's CONCEPT_DIMENSION and MODIFIER_DIMENSION that stores the concepts and modifiers codes as well as the full paths used in the ontology.
    These informations are needed to join CRC tables to ontology tables.
    """
    concept_dimension = []
    modifier_dimension = []
    for el in db:
        if el["c_tablename"] == "CONCEPT_DIMENSION":
            concept_dimension.append(
                {
                    "concept_path": el["c_fullname"],
                    "concept_cd": el["c_basecode"],
                    "name_char": el["c_name"],
                    "concept_blob": "",
                    "update_date": "",
                    "download_date": "",
                    "import_date": "",
                    "sourcesystem_cd": "",
                    "upload_id": "",
                }
            )
        elif el["c_tablename"] == "MODIFIER_DIMENSION":
            modifier_dimension.append(
                {
                    "modifier_path": el["c_fullname"],
                    "modifier_cd": el["c_basecode"],
                    "name_char": el["c_name"],
                    "modifier_blob": "",
                    "update_date": "",
                    "download_date": "",
                    "import_date": "",
                    "sourcesystem_cd": "",
                    "upload_id": "",
                }
            )
        else:
            raise Exception("not a concept nor a modifier")
    db_csv(concept_dimension, "CONCEPT_DIMENSION")
    db_csv(modifier_dimension, "MODIFIER_DIMENSION")


def dig_values(modifier, code_prehash=""):
    """
    Navigates through the modifiers possible values and returns a subtree.
    This function bypasses the SPHN "Code"  abstraction by detecting if a valueset contains such object to be expanded.
    Returns a list of paths to be appended to the modifier name.

    Each modifier is assigned to a dictionary storing information.
    In particular, a "code" key (bound to a hash) represents the i2b2 concept_cd column value.
    This code captures the modifiers hierarchy in an "onion-hash" of the RDF URIs representing the modifiers. The reason is that codes generated from
    the ontology have to match codes generated by the data loader, that does not have direct access to the modifier paths.
    """

    # Also assign RA/DA to the root modifier : RA (leaf) only if it is a Datatype without a valueset, DA if ObjectProperty (either code or submodifier)
    # do it in add_mfolders to avoid having all tasks in one function?
    endpoints = {}
    default = {"has_children": False, "comment": "", "valuetype": "T", "xml": False}
    m_id = remove_prefix(modifier)
    m_dic = default.copy()
    m_dic.update(
        {
            "comment": modifier.value(RDFS.comment),
            "code": reduce_basecode(code_prehash + modifier.identifier.toPython()),
        }
    )
    # CASE 1: a valueset is specified
    if modifier.graph.resource(SPHN.valueset) in modifier.predicates():
        print("found valueset:")
        values = extract_raw_valueset(modifier)
        m_dic.update({"has_children": True})
        for leaf in values:
            if leaf[0] == " ":
                leaf = leaf[1:]
            # Inspect the valueset
            detected, prefix = detect_toextend(leaf)
            # 'detected' is None if the leaf was a simple string, but is a (formatted) dictionary name if leaf was a dictionary name
            if detected is not None:
                leaf = detected
                tmp = extend_valuepath(detected)
                # Do we actually possess the expansion for this element ?
                if tmp:
                    # This call takes care of vastly populated, hierarchical valuesets.
                    # It will read the appropriate file and return a dictionary of endpoints
                    extract_hierarchical(
                        prefix,
                        tmp,
                        default,
                        modifier,
                        code_prehash,
                        m_id,
                        endpoints,
                        detected,
                    )
                    continue
            # Back to the case where the element of the valueset is not to be expanded
            # Add the details for the modifier leaf
            endic = default.copy()
            endic["code"] = reduce_basecode(
                code_prehash + modifier.identifier.toPython(), value=leaf
            )
            if detected:
                endic["valuetype"] = "T"
                endic["xml"] = True
            if leaf[len(leaf) - 1] == "\\":
                endic["has_children"] = True
            endpoints.update({m_id + "\\" + leaf: endic})

    # CASE 2: no valueset is specified. Object value is free text/number/date
    elif modifier.value(RDF.type) == modifier.graph.resource(OWL.DatatypeProperty):
        # The item is a Datatype property i.e a value leaf (text or numeric)
        print("Free datatype: " + modifier.label())
        vtype = modifier.value(RDFS.range)
        if vtype == modifier.graph.resource(XSD.string):
            m_vtype = "T"
        elif vtype == modifier.graph.resource(XSD.double):
            m_vtype = "N"
        elif vtype == modifier.graph.resource(XSD.dateTime):
            # In practice, datetime values are not queriable by i2b2 but can be get at a higher layer
            m_vtype = "N"
        else:
            Exception("unsupported modifier format" + vtype.label())
        m_dic.update({"valuetype": m_vtype, "xml": True})

    # CASE 3: no valueset is specified. Object value is an other object
    else:
        print("found subconcept")
        m_dic.update({"has_children": True})
        if modifier.value(RDF.type) == modifier.graph.resource(OWL.ObjectProperty):
            totest = modifier.value(RDFS.range)
        else:
            totest = modifier
        for subm in filter_obsfact(list_modifiers(totest)):
            # Cannot use modifiertree here, for hierarchy reasons
            interm = dig_values(subm, code_prehash=m_dic["code"])
            keyadd = {}
            for key in interm.keys():
                keyadd.update({m_id + "\\" + key: interm[key]})
            endpoints.update(keyadd)
    endpoints.update({m_id: m_dic})
    return endpoints


def extract_hierarchical(
    prefix, tmp, default, modifier, code_prehash, m_id, endpoints, codingsys
):
    """
    Constructs modifier paths for expandables values (ex ICD10, ATC...)
    """
    code = modifier.value(RDF.type) == modifier.graph.resource(OWL.ObjectProperty)
    if prefix != "":
        if prefix[0] == " ":
            prefix = prefix[1:]
        if prefix[-1:] == "\\":
            prefix = "\\" + prefix[:-1]
    # Add the details for the modifier, but update the node/leaf status
    for key in tmp.keys():
        elem = {}
        newv = default.copy()
        # tmp is a simple path-boolean mapping referencing the paths and if whetĥer or not they have children
        # The value used to compute the code has to be the same than in the data loader i.e it cannot encapsulate the full path
        context = codingsys + ":" if code else ""
        leafvalue = "\\" + context + reduce_term(key)
        newv.update(
            {
                "has_children": tmp[key],
                "code": reduce_basecode(
                    code_prehash + modifier.identifier.toPython(),
                    value=prefix + leafvalue,
                ),
            }
        )
        elem.update({m_id + prefix + key: newv})
        endpoints.update(elem)


def add_modifiertree(modifier, c_path, c_name):
    """
    Build the dictionary information for each modifier line. These lines are to be written in a CSV files and loaded into the ontology (metadata) database.
    Make use of the recursive search dig_values().
    """

    basedic = {
        "c_hlevel": "",
        "c_fullname": "",
        "c_name": "",
        "c_synonym_cd": "N",
        "c_visualattributes": "",
        "c_basecode": "",
        "c_facttablecolumn": "MODIFIER_CD",
        "c_tablename": "MODIFIER_DIMENSION",
        "c_columnname": "MODIFIER_PATH",
        "c_columndatatype": "T",
        "c_operator": "LIKE",
        "c_dimcode": "",
        "c_tooltip": "",
        "m_applied_path": c_path,
        "c_totalnum": "",
        "update_date": "",
        "download_date": "",
        "import_date": "",
        "sourcesystem_cd": "",
        "valuetype_cd": "",
        "m_exclusion_cd": "",
        "c_path": "",
        "c_symbol": "",
    }

    reslist = []
    # Collect all the possible modifier values
    expanded = dig_values(modifier)
    # Construct modifier paths
    for path, packed in expanded.items():
        # Standardize path, extract display name and display flag
        if path[len(path) - 1] != "\\":
            path = path + "\\"
        chunks = path.split("\\")
        name = chunks[-2] if "\\" in path else path
        if "::" in name:
            name = name[name.find("::") + 2 :]
        if "Birth" in c_name:
            name = "BirthYear-value"
        name = add_spaces(name)
        name = name[0].upper() + name[1:]
        visual = "DA" if packed["has_children"] else "RA"
        # Ad-hoc deal with the gene modifiers
        if any([e in path for e in DIRECT_VALUE]):
            if visual == "RA":
                visual = "RH"
            if visual == "DA":
                visual = "RA"
        if UNITS_DESCR in path or DATE_DESCR in path:
            # Hide the "unit" and "datetime" parameters, since it is used in the XML panel and as an observation information
            visual = "RH"
        line = basedic.copy()
        m_bc = packed["code"]
        level = str(path.count("\\"))
        xml = ""
        if visual == "RA" and (
            packed["xml"] is True or any([e in c_path for e in DIRECT_VALUE])
        ):
            if packed["valuetype"] == "N":
                xmltype = "PosFloat"
            elif packed["valuetype"] == "T":
                xmltype = "String"
            else:
                xmltype = None
            xml = generate_xml(
                rname(modifier.identifier, modifier.graph), path, xmltype, xmlcode=m_bc
            )
        line.update(
            {
                "c_hlevel": level,
                "c_fullname": "\\" + path,
                "c_name": name,
                "c_basecode": m_bc,
                "c_operator": "LIKE",
                "c_visualattributes": visual,
                "c_dimcode": "\\" + path,
                "c_comment": packed["comment"],
                "c_columndatatype": "T",
                "valuetype_cd": packed["valuetype"],
                "c_metadataxml": xml,
            }
        )

        reslist.append(line)
    return reslist
