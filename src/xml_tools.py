from rdf_base import *

UNITS_DIC = {}
UNITS_EQUIVOCAL = []
UNITS_FACTORS = {}
XML_PATTERN = "<?xml version='1.0'?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType></DataType><CodeType></CodeType><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>"
"""
This file centralizes functions that help filling the c_metadataxml field.
This includes collecting the units available in the data graph, storing them and retrieving them when needed during the ontology walk.
If no data graph is available, the XML panels will simply not include units.
"""


def generate_xml(shortname, path, simple_type="String", valueset=[], xmlcode=""):
    """
    Generate the metadata_xml panel for a specific entry. Uses the XML_PATTERN and UNITS_DIC global variables.
    enum types are not used since we have explicit valuesets as modifier leaves.
    Default type is string but PosFloat, Integer, PosInteger, Float are accepted
    """
    if simple_type is None:
        return
    res = XML_PATTERN
    res = res.replace("<TestName></TestName>", "<TestName>" + shortname + "</TestName>")
    res = res.replace(
        "<DataType></DataType>", "<DataType>" + simple_type + "</DataType>"
    )
    res = res.replace("<TestID></TestID>", "<TestID>" + xmlcode + "</TestID>")
    if simple_type == "Enum" and len(valueset) > 0:
        enumstr = "".join(
            ['<Val description="">' + elem + "</Val>" for elem in valueset]
        )
        res = res.replace(
            "<EnumValues></EnumValues>", "<EnumValues>" + enumstr + "</EnumValues>"
        )
    res = res.replace(
        "</ValueMetadata>", import_units(shortname, path) + "</ValueMetadata>"
    )
    return res


def find_other_origins(onto_graph, uri):
    """
    From parameter URI of a concept C, searches other concepts that might redirect to parameter URI through C as range of their own attributes.
    Ex :
    "quantity" is an attribute of concept "FOOD"
    but concept "ILLNESS" has an attribute "cause" that takes as values instances of "FOOD"

    then a full instance of "ILLNESS" can reference a "quantity" attribute through ILLNESS/cause/FOOD/quantity.

    In practice, higher transition degrees (C1/a1/C2/a2/C3) never happen.
    Yet, upport for them can be easily added by adding a while loop and a length test on the query response.
    """
    res = onto_graph.query(
        """
        SELECT DISTINCT ?oc 
        WHERE {
            ?trans rdfs:domain ?oc .
            ?trans rdfs:range ?class .
            ?curmod rdfs:domain ?class 
        }
    """,
        initBindings={"curmod": uri},
    )
    return len(res) > 0


def extract_units(onto_graph, data_graph, primary_classes, unit_class=SPHN.Unit):
    """
    Main loop to extract the units.
    Explore the data to extract the resources using units, then calls units_entry() on each item.
    The global dictionary links the upper to the unit, which can then be retrieved from any modifier down in the tree.
    This function is called at the beginning of the ontology extraction process and queries the data graphs.
    If the data graphs are not available, no units will be extracted and it errors can occur when querying numeric data.
    """
    if data_graph is None:
        return True
    # Add all items for which the unit object is encapsulated in a {unit, value} specific class
    numerics = onto_graph.query(
        """
        SELECT DISTINCT ?objprop ?origin ?c
        WHERE {
            ?origin rdfs:range ?c .
            ?objprop rdfs:domain ?c .
            ?objprop rdfs:range ?u 
        }
        """,
        initBindings={"u": unit_class},
    )
    wrapper_classes = []
    potential_origins = {}
    coll = []
    for e in numerics:
        # at this point, e is like [Substanceamount-unit, DAE-quantity, SubstanceAmount]
        if e[2] in CONCEPT_BLACKLIST:
            continue
        short = rname(e[1], onto_graph)
        # Search for other concepts that might lead to the same entry point (such as DAE-quantity that can be an attribute of OncologyDrugTreatment)
        if find_other_origins(onto_graph, e[1]):
            UNITS_EQUIVOCAL.append(short)
        # Append a True flag to each answer and store. In this case the origin attribute will be the key, so we shorten it
        coll.append((e[0], rname(e[1], onto_graph), True))
        # Keep a list of all the classes that are already in the list of keys so we don't double book
        if e[2] not in wrapper_classes:
            wrapper_classes.append(e[2])
    # Now get all the numeric-valued modifiers of a concept having a Unit-valued modifier (other case of unit elements)
    unrefs = onto_graph.query(
        """
        SELECT DISTINCT ?brothers ?c
        WHERE {
            ?objprop rdfs:domain ?c .
            ?objprop rdfs:range ?u .
            ?brothers rdfs:domain ?c .
            ?brothers rdfs:range ?n
        }
        """,
        initBindings={"u": unit_class, "n": XSD.double},
    )
    # Append a False flag and store all elements that weren't found by the "dedicated wrapper class" search
    coll.extend(
        [
            (rname(e[0], onto_graph), e[1], False)
            for e in unrefs
            if e[1] not in wrapper_classes
        ]
    )
    for row in coll:
        # Go fetch the available units for each key
        UNITS_DIC.update(units_entry(data_graph, row[:2], row[2]))
    return True


def units_entry(data_graph, pair, parentiskey):
    """
    Extracts the units for a specific concept/modifier
    Units are stored as a list in a dictionary, which keys are the upper concept/modifier URI.
    """
    if parentiskey:
        units = data_graph.query(
            """
            SELECT DISTINCT ?value
            WHERE{
                ?c ?modif ?unitobj .
                ?unitobj ?unitvalue ?value
            }
            ORDER BY ?value
            """,
            initBindings={"modif": pair[0], "unitvalue": SPHN["A100-Unit-value"]},
        )
        key = pair[1]
    else:
        # in this case, param 'pair' is made of the numeric modifier and the concept class.
        units = data_graph.query(
            """
        SELECT DISTINCT ?value
        WHERE{
            ?item rdf:type ?c .
            ?item ?bro ?unitobj .
            ?unitobj ?unitvalue ?value
        }
        ORDER BY ?value
        """,
            initBindings={"c": pair[1], "unitvalue": SPHN["A100-Unit-value"]},
        )
        key = pair[0]
    return {key: [e[0].toPython() for e in units]}


def import_units(shortname, path):
    """
    Add to the metadata_xml parameter the enumeration (and converting factors if applicable) of the units that
    apply to the concept/modifier represented by the uri parameter.
    This function is called by generate_xml.
    """
    if (
        shortname not in UNITS_DIC.keys()
    ):  # TODO find how to check units_origin, see if this uri cannot be reduced to an existing one
        found = False
        for key in UNITS_EQUIVOCAL:
            if key in path:
                shortname = key
                found = True
                break
        if not found:
            return ""
    unit_list = "<UnitValues>"
    units = UNITS_DIC[shortname]
    converting = []
    for i in range(len(units)):
        # First, set the reference unit (first on the list)
        if i == 0:
            unit_list = unit_list + "<NormalUnits >" + units[i] + "</NormalUnits>"
            if units[i] in UNITS_FACTORS and UNITS_FACTORS[units[i]] != 1:
                raise Exception(
                    "Unit "
                    + units[i]
                    + "should not have a relative factor if it is a reference unit"
                )
        # Store the units that can be converted
        elif units[i] in UNITS_FACTORS:
            converting.append(
                "<Units >"
                + units[i]
                + "</Units><MultiplyingFactor>"
                + UNITS_FACTORS[units[i]]
                + "</MultiplyingFactor>"
            )
        # Add the other units passing by
        else:
            unit_list = unit_list + "<EqualUnits>" + units[i] + "</EqualUnits>"

    # Merge the units that can be converted
    unit_list = (
        unit_list
        + "<ConvertingUnits>"
        + "".join([e for e in converting])
        + "</ConvertingUnits>"
    )
    pdb.set_trace()

    return unit_list + "</UnitValues>"
