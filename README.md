SagIngest
================

This code uses the DBIngestor library (https://github.com/aipescience/DBIngestor) to ingest SAG catalogues (semi-analytical galaxies from cosmological simulations) into a database.

These catalogues come in HDF5 format, so the hdf5-c++ libraries are used for reading the data and need to have been installed. 

At the moment, the path to the data (inside the file) is very hard-coded, as well as some other specific things. Nonetheless it should be fairly straightforward to adjust it to other HDF5 data files as well.

For any questions, please contact me at
Kristin Riebe, kriebe@aip.de


Data files
-----------
There are a number of results file, each one of them is in HDF5-format and contains a number of datasets and groups directly at the root-level ("/"). All data of one file belong to one snapshot number, which is provided as an attribute at the root level, along with the redshift.  
The column names roughly correspond to the names in the database table for most columns. Some columns are ignored for the database, though, some more are added. 

Features
---------
Byteswapping is automatically taken care of by the HDF5-library.  
Provide a map-file to map fields from the data file to database columns.  
The format is:  
`name_in_file`  `datatype_in_file`  `name_in_DB`  `datatype_in_DB`

(see readMappingFile function in SchemaMapper.cpp)


Installation
--------------
see INSTALL


Example
--------
The *Example* directory contains:

* *create_sag_test_mysql.sql*: example create table statement  
* *sag_test.fieldmap*: example map file for mapping data file fields to database fields  
* *sag_test_results.hdf5*, an example data file, extracted from a SAG HDF5 output. It contains only data for output numbers 70 - 79, corresponding to snapshot numbers 116 - 125  

First a database and table must be created on your server (in the example, I use MySQL, adjust to your own needs). Then you can ingest the example data into the Sag_test table with a command line like this: 

```
build/SagIngest.x  -s mysql -D TestDB -T Sag_test -U myusername -P mypassword -H 127.0.0.1 -O 3306 -f Example/sag_test.fieldmap  --fileNum=0 Example/sag_test_results.hdf5
```

Replace *myusername* and *mypassword* with your own credentials for your own database. 

The important new options are:   

`-f`: filename for field map  


TODO
-----
* Avoid reading more than necessary by comparing the available datasets with the mapping file and already removing at this stage what will not be needed. 
* Probably do not need my own class dataBlock at all; use dataset or hyperslab or so instead?
* Make mapping file optional, use internal names and datatypes as default
* Read constant values as well (for phkeys)
* Allow calculations on the fly (ix, iy, iz)
* Maybe use same format as structure files of AsciiIngest
* Make data path for HDF5-file variable (user input?)
* Allow usage of startRow, maxRows to start reading at an arbitrary row
* Use asserters


