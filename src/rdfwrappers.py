from utils import *

# add patient, encounter,  provider info in the blacklist to speedup the searches. usually should not be discarded at this stage since i2b2 takes care of them


def filter_valid(res_list):
    # Discards elements referenced in the blacklist, proceed with the other
    filtered = [item for item in res_list if item.identifier not in BLACKLIST]
    return filtered


class Component:
    """
    Component is a wrapper for the rdflib.Resource class.
    """

    def __init__(self, resource, parent_class=None):
        self.is_terminology_term = (
            terminology_indicator(resource)
            if parent_class is None
            else parent_class.is_terminology_term
        )
        self.resource = (
            self.switch_graph(resource) if self.is_terminology_term else resource
        )
        self.set_shortname()
        self.parent_class = parent_class
        com = resource.value(COMMENT_PREDICATE_URI)
        self.comment = com.toPython() if com is not None else com
        self.set_label()

    def switch_graph(self, resource):
        if not self.is_terminology_term:
            return resource
        graph = which_graph(resource.identifier)
        if graph is False:
            return resource
        return graph.resource(resource.identifier)

    def get_children(self, *kwargs):
        """
        Allows to go down the ontology tree independently if the object is a Concept or Property.
        """
        pass

    def get_entry_desc(self):
        """
        Dig the ontology from the current point only considering subclass relations.
        """
        pass

    def is_entry(self):
        return self.resource.identifier in ENTRY_CONCEPTS

    def is_logical_desc(self):
        """
        Checks if the current element was created as subclass of an other (return False) or as property of an other (return True).
        """
        return self.parent_class is None

    def is_valueset(self):
        tmp = self.resource.value(SUBCLASS_PREDICATE_URI)
        return tmp is not None and tmp.identifier in VALUESET_MARKER_URIS

    def get_label(self):
        return self.label

    def get_uri(self):
        return self.resource.identifier.toPython()

    def get_shortname(self):
        return self.shortname

    def get_comment(self):
        return self.comment

    def set_shortname(self):
        self.shortname = shortname(self.resource)

    def set_label(self):
        """
        Set the language-dependent label (to be used as display name)
        """
        labels = self.resource.graph.preferredLabel(
            self.resource.identifier, lang=PREF_LANGUAGE
        )
        if len(labels) > 0:
            self.label = labels[0][1].toPython()
        # If the resource had no language-tagged label, get the normal label. If it does not exist, say the label will be the URI suffix
        fmtd_label = self.resource.label()
        self.label = self.shortname if fmtd_label == "" else fmtd_label.toPython()

        if self.is_terminology_term:
            sep = self.shortname.rfind(":")
            code = self.shortname[sep + 1 :]
            term_name = self.shortname[:sep].upper()

            if code not in self.label and term_name not in IGNORE_TERM_ID:
                if code.isnumeric() and len(code) < 2:
                    code = "0" + code
                self.label = code + " - " + self.label
            if (
                self.parent_class is None or term_name not in self.parent_class.label
            ) and term_name not in self.label:
                self.label = term_name + ":" + self.label

    def __repr__(self):
        return (
            self.__class__.__name__
            + "("
            + self.resource.graph.namespace_manager.normalizeUri(
                self.resource.identifier
            )
            + ")"
        )

    def __eq__(self, other):
        return self.resource.identifier == other.resource.identifier


class Concept(Component):
    def __init__(self, resource, parent_class=None):
        super().__init__(resource, parent_class)
        self.subconcepts = []
        self.properties = []
        self.resolver = OntologyDepthExplorer(self)

    def get_entry_desc(self):
        """
        Gather all the concepts objects descending from self, using only the subclass attribute.
        Concepts descending from self through an  other predicate are ignored.
        """
        self.find_subconcepts(filter_mode="whitelist")

    def get_children(self, verbose=True):
        """
        Trigger the recursion and return the first level children.
        Only does so if both subconcepts and properties are empty, else consider the search has already been done.
        """
        # TODO :  is it ok to skip properties search for items that already have only subconcepts?
        if self.properties == [] and self.subconcepts == []:
            self.explore_children()
        if not self.is_terminology_term and verbose:
            print("Done digging in graph for " + self.get_shortname())
        return self.properties + self.subconcepts

    def explore_children(self):
        """
        Recursively populate the subconcepts and properties.
        """
        for k in self.find_subconcepts():
            k.explore_children()

        if self.is_terminology_term:
            return

        # Properties are expanded only when no subconcept was found (leaf concept or generic concept)
        # Note generic concepts are dealt with in the Property.sort_silent_range method which flags them as child-free concepts
        self.properties.extend(self.resolver.explore_properties())
        for predicate in self.properties:
            predicate.digin_ranges()

    def find_subconcepts(self, filter_mode="blacklist"):
        if len(self.subconcepts) == 0:
            self.subconcepts = self.resolver.explore_subclasses(filter_mode)
            # Trigger recursive call on first-level children
            for sub in self.subconcepts:
                sub.find_subconcepts(filter_mode)

        return self.subconcepts


class ChildfreeConcept(Concept):
    """A family of concepts for which we are not interested in expanding their subconcepts, either because they don't have any by nature, or they are irrelevant.
    This allows for example to have a concept "Condition" with a free text property, but still having as subconcepts terminology classes such as ICD-10 or ICD-9.
    This way, every ICD-10 concept indeed is a descendant of "Condition", but an ontology element of type "Condition" will only be defined by a free text.
    In other words, it allows to instantiate a class that would otherwise be abstract.

    To implement this, simply override find_subconcepts.
    """

    def find_subconcepts(self, filter_mode="blacklist"):
        return []


class LeafConcept(ChildfreeConcept):
    """
    LeafConcepts are leaves of the tree, by definition. By the ChildfreeConcept inheritance, they have no subconcepts. On top of that, they have no useful properties.
    """

    def explore_children(self):
        return


class Property(Component):
    def __init__(self, resource, valid_ranges):
        super().__init__(resource)
        self.ranges_res = valid_ranges
        self.ranges = []

    def get_children(self, **kwargs):
        return self.ranges

    def digin_ranges(self):
        prop_type = self.resource.value(TYPE_PREDICATE_URI)
        if prop_type is None or prop_type.identifier == OBJECT_PROP_URI:
            processed_range_res = self.sort_silent_ranges()
            raw_ranges = [Concept(reg) for reg in processed_range_res["regular"]] + [
                ChildfreeConcept(gen) for gen in processed_range_res["muted"]
            ]
            for obj in raw_ranges:
                # The explore method will trigger subclasses and properties discovery
                if obj.is_valueset():
                    self.ranges.extend(obj.resolver.explore_valueset())
                else:
                    obj.explore_children()
                    self.ranges.append(obj)
        elif prop_type.identifier == DATATYPE_PROP_URI:
            # The ranges are tree leaf objects without properties and without subclasses
            self.ranges = [LeafConcept(reg) for reg in self.ranges_res]

    def sort_silent_ranges(self):
        """
        Create Concept or ChildfreeConcept based on the resources stored in self.range_res and populate self.range with them.
        Comsequence will be that concept.explore_children() will only return properties of such muted concepts.
        The overwriting rule is typically dependent on the RDF implementation. Set the config variable ALLOW_MIXED_TREES to True to deactivate this filter.

        In the SPHN implementation, we want to expand an property range node into its subclasses if and only if
        it is a descendant of a sphn:Terminology and is the only of its kind (same prefix) in the ranges list.
        To achieve this, we find the ranges having "terminology brothers" and mute their subconcepts.

        """
        final_ranges = {"muted": [], "regular": []}
        if ALLOW_MIXED_TREES == "True":
            final_ranges["regular"] = self.range_res
            return 0

        # Extract the indices of self.ranges_res which belong to an external terminology
        termins = [terminology_indicator(elem) for elem in self.ranges_res]
        idx_termsinrange = [indx for indx, truth_val in enumerate(termins) if truth_val]

        # Now for each specific terminology, keep track to which range it applies to
        counts = {}
        for cur_idx in idx_termsinrange:
            qnam = self.resource.graph.qname(self.ranges_res[cur_idx].identifier)
            cur_terminology = qnam[: qnam.rfind(":")]
            if cur_terminology in counts.keys():
                counts[cur_terminology].append(self.ranges_res[cur_idx])
            else:
                counts[cur_terminology] = [self.ranges_res[cur_idx]]

        # Ranges that live in the same terminology are muted
        for val in counts.values():
            if len(val) > 1:
                final_ranges["muted"].extend(val)

        # Other ranges are normal
        final_ranges["regular"].extend(
            list(set(self.ranges_res) - set(final_ranges["muted"]))
        )

        return final_ranges


class RangeFilter:
    """
    Fetch and filter the Range elements of a Property discarding the blacklisted ones.
    """

    def __init__(self, res):
        self.resource = res

    def extract_range_res(self):
        """
        Return all the ranges a property points to, except the ones referring to metadata (see the config file)
        """
        rnge_types = self.extract_range_type()
        return filter_valid(rnge_types)

    def extract_range_type(self):
        """
        Return the range type of the property, expanding the bnode if any.
        The return value is a list.
        """
        response = self.resource.graph.query(
            """
        SELECT DISTINCT ?class 
        where {
            {
            ?self ?range ?class }
            union
            {
                ?self ?range [ a owl:Class ;
                                    owl:unionOf [ rdf:rest*/rdf:first ?class ]
                ]
                    }
        }
        """,
            initBindings={
                "self": self.resource.identifier,
                "range": RANGE_PREDICATE_URI,
            },
        )
        listed_res = [self.resource.graph.resource(row[0]) for row in response]
        # If there are several ranges, remove the first element which is in fact the name of the blank node
        if len(listed_res) > 1:
            listed_res = listed_res[1:]
        return listed_res


class PropertyFilter:
    """
    Handle the property extraction for a concept.
    Method 'list_unique_properties' fetches in the graph all properties and ensures no redundancy.
    Method 'filter_properties' ensures no blacklisted property is returned.
    Wanted behaviour is to call them in this order so blacklisting is effective and not a generalization of the property.

    For instance, if "Code" is present and "Diagnosis-code" (descendant of Code) too, blacklisting the latter should not fall back on the former but simply
    remove this property.
    """

    def __init__(self, concept):
        self.concept = concept
        self.resources = []

    def get_properties(self):
        self.fetch_unique_properties()
        self.filter_bl_properties()
        ranges = self.filter_ranges()
        if len(ranges) != len(self.resources):
            raise Exception("Bad property-range matching")
        return [
            Property(self.resources[i], ranges[i]) for i in range(len(self.resources))
        ]

    def filter_ranges(self):
        """
        Discard property resources for which all the ranges are blacklisted.
        """
        cleanres = []
        ranges = []
        for res in self.resources:
            handler = RangeFilter(res)
            reachable = handler.extract_range_res()
            if reachable != []:
                cleanres.append(res)
                ranges.append(reachable)
        self.resources = cleanres
        return ranges

    def filter_bl_properties(self):
        """
        Discard all blacklisted properties.
        """
        # Loop over Properties, check they are not blacklisted
        self.resources = filter_valid(self.resources)

    def fetch_unique_properties(self):
        """
        Extract the (predicate, object TYPE) couples for predicates of a resource.
        Extracts only finest properties uris, which means if two properties are related (hierarchy), only the most specific is kept.
        """
        if self.resources != []:
            return
        self_res = self.concept.resource
        response = self_res.graph.query(
            """
            SELECT ?p 
            WHERE {
                {
                ?p rdfs:domain ?self 
                }
                UNION
                {
                ?p rdfs:domain [ a owl:Class ;
                                    owl:unionOf [ rdf:rest*/rdf:first ?self ]
                                ]
                }
                FILTER NOT EXISTS { 
                    ?child rdfs:subPropertyOf+ ?p .
                    {
                    ?child rdfs:domain ?self 
                    }
                    UNION
                    {
                    ?child rdfs:domain [ a owl:Class ;
                                        owl:unionOf [ rdf:rest*/rdf:first ?self ]
                                    ]
                    }
                }
            }
        """,
            initBindings={"self": self_res.identifier},
        )

        # Extract all resources referencing this class as their domain
        self.resources = [self_res.graph.resource(row[0]) for row in response]


class OntologyDepthExplorer:
    """
    Constructed by a Concept. Fetch the subgraph spanned from this concept.
    All searches are done recursively, fetching only the first level of children and creating an other OntologyDepthExplorer object for
    """

    def __init__(self, concept):
        self.concept = concept
        self.filter = PropertyFilter(concept)

    def explore_subclasses(self, filter_mode):
        """
        Fetch the direct subclasses of the concept. Reference the parent_class concept.
        If the current node is a terminology element, all its predicate can be in a separate graph.
        """
        subs = self.concept.resource.subjects(SUBCLASS_PRED)
        if subs is None:
            return []
        if filter_mode == "blacklist":
            return [
                Concept(sub, parent_class=self.concept)
                for sub in subs
                if sub.identifier not in BLACKLIST
            ]
        elif filter_mode == "whitelist":
            return [
                Concept(sub, parent_class=self.concept)
                for sub in subs
                if sub.identifier in ENTRY_CONCEPTS
            ]
        return []

    def explore_properties(self):
        """
        Fetch the properties
        """
        return self.filter.get_properties()

    def explore_valueset(self):
        """
        If the concept is a child of "Valueset", then all possible instances should be specified as children of this concept.
        At data loading, these instances should be treated differently as other instances (for which only the class is important)
        """
        graph = self.concept.resource.graph
        res2 = graph.query(
            """
        select ?s 
        where {
            ?s rdf:type ?o
        }
        """,
            initBindings={"o": self.concept.resource.identifier},
        )
        return [LeafConcept(graph.resource(row[0])) for row in res2]
