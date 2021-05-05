# RDF-i2b2 converter tutorial

Dependencies:
* rdflib >= v.5.0.0


The high-level idea is to migrate RDF's
` Subject - Predicate - Object` relation to a `Concept - Attribute - Value `hierarchy, at least for instantiable concepts (referred to as _primary concepts_ here). In the case of i2b2, attributes are named _modifiers_.

Attributes discovery does not depend on the ontology terms nor on the target data format. The only assumption is an equivalent non-cyclic hierarchy of various depths. The main function of this core is (given a set of entrypoints) to walk through a RDF graph and to list all spanned absolute paths.

The converter consists of two parts: an **ontology converter** and a **data loader**. Both use generic modules and project-dependent modules.

## Ontology: how to use

Core components are files **`ontology_converter.py`** and **`utils.py`**.
The required resources are:
* a RDF ontology file, featuring rdf:type and owl:class predicates ((we used a .ttl file, hence the _format=turtle_ keyword argument in calls to rdflib. Change it accordingly to the format of your RDF files.)
* a configuration file named "ontology_config" featuring parameters for the ontology extraction and data loading.

```python
ONTOLOGY_GRAPH_LOCATION

DATA_GRAPH_LOCATION
"../files/data/"

NARROW_ONTOLOGY # Defines a smaller graph that will be used as filter for the bigger graph

ABSTRACT_CLASSES  # Never instantiated because internal/wrappers
["SubjectPseudoIdentifier",  "Frequency", "Duration", "SubstanceAmount", "RelativeDoseOfDrugApplied"]

BLACKLIST         # Simply not instantiated in this project
["SimpleScore", "OncologySurgery", "BodySite"]

# "Banned" concepts will be completely ignored. If they are attributes of other concepts, the latter will still exist but without this specific attribute

EXT_DICTS         # Optional
{"bioportal":["SNOMEDCT", "ICD10",  "ATC", "LOINC"], "none" : ["CHOP", "GTIN"], "SVIP" : ["SVIP"]}
# Python-formatted dictionary binding function names to terminologies they can query. This field is not useful if you can generate the terminologies record files by other means (see the requirements in the next paragraph).

```
* records of terminologies
  - the terminologies should be formatted as json-dumped python dictionaries with items as such, to reflect the hierarchy as an absolute path collection, figuring a boolean indicating if the element has children somewhere in the tree or not:
```python
ontology_dict = {
        # this item has children in the tree
        "\\Root element\\": true,        
        # these items are tree leaves and do not have children
        "\\Root element\\ Child element 1" : false, 
        "\\Root element\\ Child element 2" : false 
        }
json.dump(filebuffer, ontology_dict)
```

**Extracting terminology records (toolkit):**

We provide an engine querying the [Bioportal API](https://bioportal.bioontology.org/ontologies) and reconstructing the items paths. Any terminology referenced in Bioportal can be queried by this module. A second engine querying the [SVIP API](https://svip-dev.nexus.ethz.ch/api/v1/) for genomic mutations is also available. For any other terminology, the user should generate a file as described above by other means.
This engine performs function switching depending on which API you want to use, make sure to change the `extract_biotree` and `switch_terminology` functions if you want to add one.

In case the desired terminology exists as an i2b2 metadata table, we also provide an extraction routine in the `terminology_format` file, that will map it to the simple model described above.

**Valuesets**

One of the core features of the converter is the ability to detect and expand _valuesets_, namely constraints for concept instantiation. They are typically described as (for a .ttl syntax)
```
my_domain:my_class a owl:class
    my_domain:valueset "[value_A; value_B; @expandable_C]"
```
where `@expandable_C` denotes an external terminology i.e a link to a larger valueset which is specified in a separate file, as mentioned at the beginning of this section -- don't forget the '@' prefix!

Ontology generation pipeline
------

1. **`keys = setup()`**
2. **`database = gather_onto_lines(keys)`**

Breaking it down:

First, extract primary concepts (entrypoints to the graph)
   - **`setup()`** function that reads the configuration file `ontology_config`
  
Then for each concept, trigger a recursive discovery of its attributes and their attributes, all bound to the concept
  - **`gather_onto_lines()`** function which calls `extract_units`, `add_modifiertree()` and `dig_values()`

Upon discovery of attributes having a `valueset` property, treat each element of the said `valueset` as a child of the attribute
  - `extend_valuepath()` function, called by ` dig_values()`

 Generate a database row for each discovered element
  - This is distributed between **`gather_onto_lines()`** and `add_modifiertree()`.

 
## Ontology: project-specific settings

  
* Excluding attributes: RDF ontology concepts might have predicates that you want to ignore such as patient information and other attributes that only make sense once the concept is instantiated. Use the config file to change this according to your needs.
* Importing units and dates into i2b2 works by term-matching, terms to be defined in the config file.
* Exporting the ontology database is adapted to i2b2 by default, but moving to any other tree-related model requires minor source code changes in `add_single_line()` and `add_mtree()`, so the output dictionaries (virtually, database lines) reflect the information the system needs.
* i2b2 specific: extracting CONCEPT_DIMENSION and MODIFIER_DIMENSION tables. This is done by default at the end of the ontology database computation by calling `extract_C_M_dim`.
* 

## Data loading: how to use

The data loading is made of two parts: values extraction from the instances and junction to the ontology concepts. 

The end-to-end scheme is quite specific to i2b2 but as for the ontology conversion, the core is a graph walk extracting individually every observation and value and loading a list of Python dictionaries with them.

The i2b2 data system is built along a [star schema](https://community.i2b2.org/wiki/display/ServerSideDesign/I2B2+DATA+MART), and concept paths written in the ontology table are not specified in the data table. Instead, a _concept code_ allows to join the data table elements (_observations_) to their corresponding ontology path. Both the path and code are written in the CONCEPT_DIMENSION. The same logic applies to _modifiers_ and the MODIFIER_DIMENSION.


Data loading pipeline
-----------
The input graph can be either a unique files of instances or several files to be joined into a unique data graph. The loading script will discover all files in the `DATA_GRAPH_LOCATION` directory (see config file). 

1. **`instances = navigate_graph()`**
2. **`database = generate_dbblock(instances)`**

Breaking it down:

The setup phase for data loading consists in filtering the instances of the _primary concepts_, done by **`navigate_graph()`**. These are the entry points to the data graph.

A recursive search thourgh the instances' attributes is performed by the `dig_attributes` function, called in the main loop of the **`generate_dbblock()`** function.

For each primary instance, we then extract the **observation details** (i.e instances of `ABSTRACT_CLASSES` elements such as dates, patient or data provider information, medical encounter reference etc.) This is done by `extract_metainfo()` once for each concept, and **applies to all the tree i.e the primary concept instance and all its attributes**. 

As last step of the main loop, the function **`obsfact_dblines()`** merges these three pieces of information and constructs database lines accordingly. Typically the number of database lines will be equal to the number of items in the modifers info list, plus one.

## Data loading: project-specific settings

Depending on your data structure, you might want to change/deactivate the _observation details_ retrieval, integrate it in the attribute search (do so by modyfing the config file), and changing the database lines computation in `obsfact_dblines`. The recursive search that walks the graph from its entrypoints and lists the retrieved items should not require any important modification.

In the case of our i2b2 data loading, we extract three more tables to fill the _star schema_ : `PATIENT_DIMENSION`, `PROVIDER_DIMENSION` and `VISIT_DIMENSION`.
