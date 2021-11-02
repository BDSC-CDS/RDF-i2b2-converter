from rdf_base import *

# add patient, encounter,  provider info in the blacklist to speedup the searches. usually should not be discarded at this stage since i2b2 takes care of them
BLACKLIST = BLACKLIST + [k for k in ONTOLOGY_DROP_DIC.values()]


def filter_valid(res_list):
    # Discards elements referenced in the blacklist, proceed with the other
    filtered = [
        item for item in res_list if item.identifier.toPython() not in BLACKLIST
    ]
    return filtered


def terminology_indicator(concept):
    """
    Determine if it is worth looking for properties of this concept or not.
    In the SPHN implementation, if the concept comes from a terminology (testable easily by looking at the URI) it doesn't have any properties
    """
    return PROJECT_RDF_NAMESPACE not in concept.resource.identifier


class Component:
    """
    Component is a wrapper for the rdflib.Resource class.
    """

    def __init__(self, resource):
        self.resource = resource
        self.shortname = resource.graph.namespace_manager.normalizeUri(
            resource.identifier
        )
        self.comment = resource.value(COMMENT_URI).toPython()
        self.set_label()

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

    def get_uri(self):
        return self.resource.identifier.toPython()

    def get_shortname(self):
        return self.shortname

    def get_comment(self):
        return self.comment

    def set_label(self):
        """
        Set the language-dependent label (to be used as display name)
        """
        labels = self.resource.graph.preferredLabel(
            self.resource.identifier, lang=PREF_LANGUAGE
        )
        if len(labels) > 0:
            self.label = labels[0][1].toPython()
            return

        # If the resource had no language-tagged label, get the normal label. If it does not exist, say the label will be the URI suffix
        fmtd_label = self.resource.graph.label(self.resource.identifier)
        self.label = self.shortname if fmtd_label == "" else fmtd_label.toPython()

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
    def __init__(self, resource, parent=None):
        super().__init__(resource)
        self.subconcepts = []
        self.properties = []
        self.parent = parent
        self.resolver = OntologyDepthExplorer(self)

    def get_entry_desc(self):
        """
        Gather all the concepts objects descending from self, using only the subclass attribute.
        Concepts descending from self through an  other predicate are ignored.
        """
        self.find_subconcepts(filter_mode="whitelist")

    def get_children(self):
        """
        Trigger the recursion and return the first level children.
        """
        if self.properties == [] or self.subconcepts == []:
            self.explore_children()
        return self.properties + self.subconcepts

    def explore_children(self):
        for k in self.find_subconcepts():
            k.explore_children()
        if terminology_indicator(self):
            return

        self.subconcepts.extend(self.resolver.explore_valueset())

        # Properties are expanded only when no subconcept was found (leaf concept or generic concept)
        # Note generic concepts are dealt with in the Property.sort_silent_range method which flags them as leaf concepts
        self.properties.extend(self.resolver.explore_properties())
        for predicate in self.properties:
            predicate.digin_ranges()

    def find_subconcepts(self, filter_mode="blacklist"):
        if len(self.subconcepts)>0:
            return self.subconcepts
        self.subconcepts = self.resolver.explore_subclasses(filter_mode)
        # Trigger recursive call on first-level children
        for sub in self.subconcepts:
            sub.find_subconcepts(filter_mode)


class LeafConcept(Concept):
    """A family of concepts we want to treat as leaves concepts, i.e we are not interested in expanding their subconcepts.
    This allows for example to have a concept "Condition" with a free text property, but still having as subconcepts terminology classes such as ICD-10 or ICD-9.
    This way, every ICD-10 concept indeed is a descendant of "Condition", but an ontology element of type "Condition" will only be defined by a free text.
    In other words, it allows to instantiate a class that would otherwise be abstract.

    To implement this, simply override find_subconcepts.
    """

    def find_subconcepts(self, resolver):
        return []


class ValuesetIndividual(LeafConcept):
    """
    Individuals are leaves of the tree, by definition. They cannot have children nor properties and will match observation instances.
    """

    def explore_children(self):
        return


class Property(Component):
    def __init__(self, resource, valid_ranges):
        super().__init__(resource)
        self.ranges_res = valid_ranges
        self.ranges = []

    def get_children(self):
        return self.ranges

    def digin_ranges(self):
        if self.resource.value(TYPE_PREDICATE_URI)==DATATYPE_PROP_URI:
            # If we are a Datatype property, we are a leaf object in the ontology tree. stop there.
            # TODO: find a way to pass the info that we are bearing a datatype that will end up in the i2b2 metadataxml field
            # the solution to write the range as a concept is dumb because it will create an extra level in the i2b2 tree...
            # UNLESS i2b2 takes care of not writing concepts which uri fall in the MERGE_DIC category as new ontology items but as xml details of the parent.
            # this looks like the only corect solution else it conditions the rdfwrapper behaviour on an i2b2 objective. do put it as a concept.
            return

        elif self.resource.value(TYPE_PREDICATE_URI)==OBJECT_PROP_URI:
            processed_range_res = self.sort_silent_ranges()
            self.ranges = self.instantiate_ranges(processed_range_res)
            for obj in self.ranges:
                # The explore method will trigger subclasses and properties discovery
                obj.explore_children()

    def instantiate_ranges(self, range_resources_dic):
        """
        Instantiate the ranges as Concepts or LeafConcepts
        """
        return [Concept(reg) for reg in range_resources_dic["regular"]] + [LeafConcept(gen) for gen in range_resources_dic["muted"]]


    def sort_silent_ranges(self):
        """
        Create Concept or LeafConcept based on the resources stored in self.range_res and populate self.range with them.
        Comsequence will be that concept.explore_children() will only return properties of such muted concepts.
        The overwriting rule is typically dependent on the RDF implementation. Set the config variable ALWAYS_DEEP to True to deactivate this filter.

        In the SPHN implementation, we want to expand an property range node into its subclasses if and only if
        it is a descendant of a sphn:Terminology and is the only of its kind (same prefix) in the ranges list.
        To achieve this, we find the ranges having "terminology brothers" and mute their subconcepts.

        """
        final_ranges = {"muted":[], "regular":[]}

        if ALWAYS_DEEP:
            final_ranges["regular"]=self.range_res
            return 0

        # Extract the indices of self.ranges_res which belong to an external terminology
        termins = [
            (
                elem.identifier,
                SUBCLASS_PRED * rdflib.paths.OneOrMore,
                TERMINOLOGY_MARKER_URI,
            )
            in self.resource.graph
            for elem in self.ranges_res
        ]
        idx_termsinrange = [indx for indx, truth_val in enumerate(termins) if truth_val]

        # Now count occurrence of each specific terminology
        counts = {}
        for cur_idx in idx_termsinrange:
            cur_terminology = self.resource.graph.qname(self.ranges_res[cur_idx])
            if cur_terminology in counts.keys():
                counts[cur_terminology] = counts[cur_terminology] + 1
            else:
                counts[cur_terminology] = 1

        # Now search in self.ranges_res which range belong to an ontology and have brother in it.
        # When found, prune its subconcepts so it cannot be expanded
        for rn_idx in range(len(self.ranges_res)):
            if rn_idx in idx_termsinrange and counts[self.resource.graph.qname(self.ranges_res[rn_idx])] > 1:
                final_ranges["muted"].append(self.ranges_res[rn_idx])
                continue
            final_ranges["regular"].append(self.ranges_res[rn_idx])

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
            ?self rdfs:range ?class }
            union
            {
                ?self rdfs:range [ a owl:Class ;
                                    owl:unionOf [ rdf:rest*/rdf:first ?class ]
                ]
                    }
        }
        """,
            initBindings={"self": self.resource.identifier},
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
        print("Fetching properties for concept " + self.concept.__repr__())
        self_res = self.concept.resource
        # TODO enhance this
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
        Fetch the direct subclasses of the concept. Reference the parent concept.
        """
        subs = self.concept.resource.subjects(SUBCLASS_PRED)
        if filter_mode == "blacklist":
            return [
                Concept(sub, parent=self.concept)
                for sub in subs
                if sub.identifier.toPython() not in BLACKLIST
            ]
        if filter_mode == "whitelist":
            return [
                Concept(sub, parent=self.concept)
                for sub in subs
                if sub.identifier.toPython() in ENTRY_CONCEPTS
            ]

    def explore_properties(self, entrypoint=None):
        """
        Fetch the properties
        """
        return self.filter.get_properties()

    def explore_valueset(self):
        """
        If the concept is a child of "Valueset", then all possible instances should be specified as children of this concept.
        At data loading, these instances should be treated differently as other instances (for which only the class is important)
        """
        if self.concept.resource.value(
            rdflib.URIRef(SUBCLASS_PRED_URI)
        ).identifier != rdflib.URIRef(VALUESET_MARKER_URI):
            return []
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
        return [ValuesetIndividual(graph.resource(row[0])) for row in res2]
