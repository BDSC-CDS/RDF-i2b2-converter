# RDF-i2b2 converter tutorial

Dependencies:
* python >= v3.7
* rdflib >= v.5.0.0
* pandas >= 1.3.4


The high-level idea is to migrate RDF's
` Subject - Predicate - Object` relation to a `Concept - Attribute - Value `hierarchy, at least for instantiable concepts (referred to as _primary concepts_ here). In the case of i2b2, attributes are named _modifiers_.

Attributes discovery does not depend on the ontology terms nor on the target data format. The only assumption is an equivalent non-cyclic hierarchy of various depths. The main function of this core is (given a set of entrypoints) to walk through a RDF graph and to list all spanned absolute paths.

The converter consists of two parts: an **ontology converter** and a **data converter**. Both use generic modules and project-dependent modules.

## Ontology converter

Core components are located in **/src/**. The configuration document is **/files/
The required resources are:
* a RDF ontology file, featuring rdf:type and owl:class predicates ((we used a .ttl file, hence the _format=turtle_ keyword argument in calls to rdflib. Change it accordingly to the format of your RDF files.)
* configuration files, by default named *graph_config.json* and *i2b2-mapping.json* featuring parameters for the ontology extraction and mapping to i2b2 tables.

### A walk through the ontology configuration files:
```python
--- graph_config.json

ONTOLOGY_GRAPH_LOCATION # Relative path to the ontology RDF graph files (can be a whole folder)
TERMINOLOGIES_LOCATION  # Path to the external terminologies as RDF graph files
OUTPUT_TABLES           # Path to destination folder of created i2b2 tables
RDF_FORMAT              # Typically turtle or owl, if supported by rdflib
PREF_LANGUAGE           # Corresponding to the language label tag in RDF resources, e.g "fr" or "de"
ENTRY_CONCEPTS          # A list of URIs that should be treated as entrypoints to the graph, 
                        # as highest non-abstract level of concept. 
                        # Used both by the ontology and data converters.
BLACKLIST               # Any URI, either class or property URI can be blacklisted and ignored in
                        # the graph walk. Use it with caution.

--- i2b2_rdf_mapping.json

DATA_LEAVES             # Datatypes (int, float, string...) that
EQUIVALENCES            # should be processed as contextual 
                        # details for ontology elements (merged into the same row as an ontology element, 
                        # but in a dedicated column)

COLUMNS                 # The column list for every i2b2 table 
                        # we want to create. All columns mentioned in the EQUIVALENCES collection 
                        # defined above can be found there.
```

Ontology generation pipeline
------


`$ cd RDF-i2b2-converter/`

`$ python3 src/main_ontology.py`

------------------

## Data samples converter

The data converter is made of two parts: values extraction from the instances and junction to the ontology concepts. 

The end-to-end scheme is quite specific to i2b2 but as for the ontology conversion, the core is a graph walk extracting individually every observation and value and loading a list of Python dictionaries with them.

The i2b2 data system is built along a [star schema](https://community.i2b2.org/wiki/display/ServerSideDesign/I2B2+DATA+MART), and concept paths written in the ontology table are not specified in the data table. Instead, a _concept code_ allows to join the data table elements (_observations_) to their corresponding ontology path. Both the path and code are written in the CONCEPT_DIMENSION. The same logic applies to _modifiers_ and the MODIFIER_DIMENSION.

The input graph can be either a unique files of instances or several files to be joined into a unique data graph. The loading script will discover all files in the `DATA_GRAPH_LOCATION` directory (see config file). 

### A walk through the ontology configuration files:
```python
--- data_config.json

DATA_GRAPHS_LOCATION    # Where to look for the data samples, formatted as .ttl
                        
CONTEXT_GRAPHS_LOCATION # Location of other graphs if needed (typically, an terminology graph for units of measurement)

TO_IGNORE               # A BLACKLIST-equivalent variable for elements that can be present in the data but should not be used

ENTRY_DATA_CONCEPTS     # Which classes to use as entrypoints to the data graph. 
                        # We recommend having them identical as the ENTRY_CONCEPTS used in the ontology.


COLUMNS_MAPPING         # A lookup nested-dict structure pointing to the destination columns of fields that are details of ontology elements. 
                        # These are more or less the same fields as the DATA_LEAVES collection in the ontology configuration file.

    VALUE               # Instructions to extract and store the actual value from numbers, strings, etc.
    CONTEXT             # Instructions to extract and store contextual informations such as patient, encounter, etc. The "pred_to_value" gives the RDF links to follow to get to the actual usable value. The "col" field 

```


Data conversion pipeline
-----------

`$ cd RDF-i2b2-converter/`

`$ python3 src/main_data.py`