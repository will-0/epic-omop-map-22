# Resources for UCSD OMOP vocab mapping

The following items need to be present for the code to work:

* `./resource.db` - This is an sqlite database object, which must contain the OMOP `CONCEPT` table. This table is used for appending concept names, vocabulary IDs, etc, from the OMOP concept ID.
* `__ReadOnly/__ElementDefinitions.csv` - This table contains the list of elements that are to be investigated in the mapping. It cannot be included in the repo due to EPIC restrictions, but see `__Readonly/__ElementDefinitions_EXAMPLE.csv` for format example
* `__Readonly/__ValueDefinitions.csv` - This table contains the list of pre-populated options for a given data element. See `__Readonly/__ValueDefinitions_EXAMPLE.csv` for format example.
* `__Readonly/__OrigIndex.csv` - This table is simply for internal consistency. It provides the 'original' order of the data elements in the tables, meaning tables can always be presented to mappers in the same order (making mapping process easier).