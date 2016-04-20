/*  
 *  Copyright (c) 2016, Kristin Riebe <kriebe@aip.de>,
 *                      E-Science team AIP Potsdam
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership. You may obtain a copy
 *  of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>   // sqrt, pow
#include "sagingest_error.h"
#include <list>
//#include <boost/filesystem.hpp>
//#include <boost/serialization/string.hpp> // needed on erebos for conversion from boost-path to string()
#include <boost/regex.hpp> // for string regex match/replace to remove redshift from dataSetNames

#include "Sag_Reader.h"

//using namespace boost::filesystem;

//#include <boost/chrono.hpp>
//#include <cmath>
#include <boost/date_time/posix_time/posix_time.hpp>


namespace Sag {
    SagReader::SagReader() {
        fp = NULL;

        currRow = 0;
    }
    
    SagReader::SagReader(string newFileName, int newFileNum) {

        fileName = newFileName;
        
        // get directory number and file number from file name?
        // no, just let the user provide a file number 
        fileNum = newFileNum;

        fp = NULL;

        currRow = 0;
        countInBlock = 0;   // counts values in each datablock (output)

        blocksize = 6;

        // factors for constructing dbId, could/should be read from user input, actually
        snapnumfactor = 1000;
        rowfactor = 1000000;

        openFile(newFileName);
        //offsetFileStream();

        getMeta();
        cout << "size of dataSetMap: " << dataSetMap.size() << endl;
        //cout << dataSetMap.find("GalaxyID") << endl;
        // print them all:
        //vector<string> dataSetNames;
        //for(map<string,int>::iterator it = dataSetMap.begin(); it != dataSetMap.end(); ++it) {
        //    cout << it->first << "\n";
        //}
        
    }


    SagReader::~SagReader() {
        closeFile();
        // delete data sets? i.e. call DataBlock::deleteData?
    }
    
    void SagReader::openFile(string newFileName) {
        // open file as hdf5-file
        H5std_string h5fileName;
        h5fileName = (H5std_string) newFileName;

        if (fp) {
            fp->close();
            delete fp;
        }

        // TODO: catch error, if file does not exist or not accessible? before using H5 lib?
        fp = new H5File(h5fileName, H5F_ACC_RDONLY); // allocates properly
        
        if (!fp) { 
            SagIngest_error("SagReader: Error in opening file.\n");
        }
        
    }
    
    void SagReader::closeFile() {
        if (fp) {
            fp->close();
            delete fp;
        }
        fp = NULL;
    }

    void SagReader::getMeta() {
        char line[1000];
        int snapnum;
        float redshift;

        string s;
        string dsname;
        string matchname;

        string outputName;
        hid_t    grp;

        cout << "Finding dataset names in the file ... " << endl;
        Group group(fp->openGroup("/"));
        grp = H5Gopen(group.getId(),"/", H5P_DEFAULT);

        dataSetNames.clear();
        scan_group(grp, &dataSetNames);
        
        numDataSets = dataSetNames.size();
        cout << "numDataSets: " << numDataSets << endl;
        
        // create a key-value map for the dataset names, do it once for each file
        // TODO: should actually check here already, if the dataSet-names match
        //      the expectations from the mapping file;
        //      Exit, if datasets are missing.
        // TODO: read only those datasets that have a match in mapping file, 
        //      ignore the others!! => Save time and memory!!

        herr_t status;
        dataSetMap.clear();

        for (int k=0; k<numDataSets; k++) {
            dsname = dataSetNames[k];
            dataSetMap[dsname] = k;
        }

        // get the constant attributes for the main group
        Attribute att = group.openAttribute("Redshift");
        DataType type = att.getDataType();
        att.read(type, &redshift);

        cout << "redshift: " << redshift << endl;

        Attribute att2 = group.openAttribute("Snapshot");
        DataType type2 = att2.getDataType();
        att2.read(type2, &snapnum);
        cout << "snapnum: " << snapnum << endl;
        
        // store in global variables:
        current_snapnum = snapnum;
        current_redshift = redshift;

        // should close the group now
        group.close();

        return;
    }

    long SagReader::getNumRowsInDataSet(string s) {
        // get number of rows (data entries) in given dataset
        // just check with the one given dataset and assume that all datasets
        // have the same size!
        //sprintf(outputname, "Outputs/Output%d/nodeData", ioutput);
        long nvalues;

        DataSet *d = new DataSet(fp->openDataSet(s));
        DataSpace dataspace = d->getSpace();

        int rank = dataspace.getSimpleExtentNdims();
        hsize_t dims_out[rank];
        int ndims = dataspace.getSimpleExtentDims(dims_out, NULL);
        //cout << "dimension " << (unsigned long)(dims_out[0]) << endl;
        nvalues = dims_out[0];

        delete d;

        return nvalues;
    }


    vector<string> SagReader::getDataSetNames() {
        return dataSetNames;
    }


    int SagReader::getNextRow() {
        //assert(fileStream.is_open());

        DataBlock b;
        string name;
        string outputName;
        long n;

        // get one line from already read datasets (using readNextBlock)
        // use readNextBlock to read the next blocks from datasets, if necessary
        // readNextblock returns blocksize = number of read values; this 
        // may be adjusted at the end of the file, new value is then returned
        // and can beused to check here, when we reach the end of the file
        if (currRow == 0) {
            // we are at the very beginning
            // read block, initialize counter
            blocksize = readNextBlock(blocksize);
            //cout << "nvalues in getNextRow: " << nvalues << endl;
            countInBlock = 0;
        } else if (countInBlock == blocksize-1) {
            // end of block reached, read the next block
            blocksize = readNextBlock(blocksize);
            //cout << "nvalues in getNextRow: " << nvalues << endl;
            countInBlock = 0;
        } else {
            //cout << "blocksize: " << blocksize << endl;
            countInBlock++;
        }

        if (blocksize <= 0) {
            return 0;
        } 

        currRow++; // global counter for all rows

        // stop reading/ingesting, if mass is lower than threshold?
        // stop after reading maxRows?
        // stop after a certain number of blocks?
        
        // Do not have a reliable number of values in the datasets, so 
        // rely on readNextBlock to return something < 0 if it encounters 
        // end of file.
    
        return 1;
    }

    int SagReader::readNextBlock(long blocksize) {
        cout << "read next block: with numDataSets: " << numDataSets << endl;

        // read one block from SAG HDF5-file, max. blocksize values
        // nvalues is a global variable

        //performance output stuff
        boost::posix_time::ptime startTime;
        boost::posix_time::ptime endTime;
        
        string s;
        string dsname;

        IntType intype;
        FloatType ftype;
        size_t dsize; 
        
        herr_t status;
        hsize_t count[2];       // size of the hyperslab in the file (number of blocks)
        hsize_t offset[2];      // hyperslab offset in the file
        hsize_t nblock[2];      // block size to be read

        // clear datablocks from previous block, before reading new ones:
        datablocks.clear();

        nvalues = getNumRowsInDataSet(dataSetNames[0]); // could do this already outside of this function!

        // make sure that we are not exceeding the max. number 
        // of values in this dataset:
        blocksize = min(blocksize, nvalues-currRow);

        offset[0] = currRow;
        offset[1] = 0; // we actually only have one dimension > 1 for SAG data

        nblock[0] = blocksize;
        nblock[1] = 1;

        
        cout << "nvalues, currRow, blocksize: " << nvalues << ", " << currRow << ", " << blocksize << endl;
        if (currRow >= nvalues) {
            // already reached end of file, no more data available
            cout << "End of dataset reached. Nothing more to read." << endl; 
            return 0;
        }

        startTime = boost::posix_time::microsec_clock::universal_time();

        // read each desired data set, use corresponding read routine for different types
        for (int k=0; k<numDataSets; k++) {
        //for (int k=0; k<2; k++) {

            dsname = dataSetNames[k];
            //s = string(outputName) + string("/") + dsname;
            //s = string("/") + dsname;
            s = dsname;
            cout << "s-name: " << s << endl;
            DataSet *dptr = new DataSet(fp->openDataSet(s));
            DataSet dataset = *dptr; // for convenience

            // check class type
            H5T_class_t type_class = dataset.getTypeClass();
            //cout << "type_class " << type_class << endl;

            if (type_class == H5T_INTEGER) {
                // check exactly, if int or long int
                intype = dataset.getIntType();
                dsize = intype.getSize();
                if (sizeof(long) == dsize) {
                    cout << "DataSet has long type!" << endl;
                    //long *data = readLongDataSet(s, nvalues);  
                    long *data = readLongDataSet(s, nvalues, nblock, offset);
 
                } else if (sizeof(int) == dsize) {
                    cout << "DataSet has int type!" << endl;
                     //int *data2 = readIntDataSet(s, nvalues);
                    cout << "ERROR: Reading datasets of int type not implemented yet!" << endl;
                    abort();
                } else {
                    cout << "ERROR: Do not know how to deal with int-type of size " << dsize << endl;
                    abort();
                }
            } else if (type_class == H5T_FLOAT) {
                // check exactly, if float or double
                ftype = dataset.getFloatType();
                dsize = ftype.getSize();
                if (sizeof(double) == dsize) {
                    cout << "DataSet has double type!" << endl;
                    double *data3 = readDoubleDataSet(s, nvalues, nblock, offset);
                } else if (sizeof(float) == dsize) {
                    cout << "DataSet has float type!" << endl;
                    float *data4 = readFloatDataSet(s, nvalues, nblock, offset);
                } else {
                    cout << "ERROR: Do not know how to deal with float-type of size " << dsize << endl;
                    abort();
                }
            } else {
                cout << "ERROR: Reading datasets of type " << type_class << " not implemented yet!" << endl;
                abort();
            }
            //cout << nvalues << " values read." << endl;
        }


        // How to proceed from here onwards??
        // Could read all data into data[0] to data[104] or so,
        // but I need to keep the information which is which!
        // Alternatively create one big structure to hold it all?

        // => use a small class that contains
        // 1) name of dataset
        // 2) array of values, number of values
        // use vector<newclass> to create a vector of these datasets.
        // maybe can use datasets themselves, so no need to define own class?
        // => assigning to the new class has already happened now inside the read-class.

        endTime = boost::posix_time::microsec_clock::universal_time();
        printf("Time for reading (%ld rows): %lld ms\n", blocksize, (long long int) (endTime-startTime).total_milliseconds());
        fflush(stdout);
            
        return blocksize; // number of read values
    }


    long* SagReader::readLongDataSet(const std::string s, long &nvalues, hsize_t *nblock, hsize_t *offset) {
        // read a long-type dataset

        //cout << "Reading DataSet '" << s << "'" << endl;
        hsize_t count[2];    // size of the hyperslab in the file
        hsize_t stride[2];  // should be 1,1
        hsize_t block[2];   // block size, should use nblock-values

        // DataSet dataset = fp->openDataSet(s);
        // rather need pointer to dataset in order to delete it later on:
        DataSet *dptr = new DataSet(fp->openDataSet(s)); // need pointer because of "new ..."
        DataSet dataset = *dptr; // for convenience (TODO: CHECK: only works, if class contains a copy-constructor)

        // check class type
        H5T_class_t type_class = dataset.getTypeClass();
        if (type_class != H5T_INTEGER) {
            cout << "Data does not have long type!" << endl;
            abort();
        }
        // check byte order
        IntType intype = dataset.getIntType();
        H5std_string order_string;
        H5T_order_t order = intype.getOrder(order_string);
        //cout << order_string << endl;

        // check again data sizes
        if (sizeof(long) != intype.getSize()) {
            cout << "Mismatch of long data type." << endl;
            abort();
        }

        size_t dsize = intype.getSize();
        //cout << "Data size is " << dsize << endl;

        // get dataspace of the dataset
        DataSpace dataspace = dataset.getSpace();
        //cout << "Dataspace is " << dataspace << endl;

        // get number of dimensions in dataspace
        int rank = dataspace.getSimpleExtentNdims();
        //cout << "Dataspace rank is " << rank << endl;

        // I expect this to be 2 for all SAG datasets!
        // There are no more-dimensional arrays stored in one dataset, are there?
        if (rank == 1) {
            hsize_t dims_out[1];
            int ndims = dataspace.getSimpleExtentDims(dims_out, NULL);
            //cout << "dimension " << (unsigned long)(dims_out[0]) << endl;
            nvalues = dims_out[0];
           
        }
        else if (rank == 2) {
            hsize_t dims_out[2];
            int ndims = dataspace.getSimpleExtentDims(dims_out, NULL);
            if (dims_out[rank-1] == 1) {
                //cout << "rank " << rank << ", dimensions " <<
                //    (unsigned long)(dims_out[0]) << " x " <<
                //    (unsigned long)(dims_out[1]) << endl;
                nvalues = dims_out[0];
            } else {
                cout << "ERROR: Cannot cope with this dataset, dimensions too high:" << 
                    (unsigned long)(dims_out[0]) << " x " <<
                    (unsigned long)(dims_out[1]) << endl;
                abort();
            }
        } else {
            cout << "ERROR: Cannot cope with multi-dimensional datasets! rank: " << rank << endl;
            abort();
        }

        // define hyperslab
        // offset already provided when calling this function, no need to redefine here
        count[0]  = 1;  // just use 1 block, so count = 1
        count[1]  = 1;

        stride[0] = 1;
        stride[1] = 1;
        
        block[0] = nblock[0]; // use block instead of count, might be faster
        block[1] = 1;

        hsize_t dimsm[2]; // memory space dimensions, must be the same as hyperslab-size
        dimsm[0] = nblock[0];
        dimsm[1] = 1;

        // define memory space
        DataSpace memspace(rank, dimsm, NULL);

        // selec the hyperslap from the dataspace
        dataspace.selectHyperslab(H5S_SELECT_SET, count, offset, stride, block); 
        //dataspace.selectHyperslab( H5S_SELECT_SET, count, offset ); 
        
        // read data from selection
        long rdata[nblock[0]][1]; // read requirer buffer to have same dimensions as dataset
        dataset.read(rdata,PredType::NATIVE_LONG, memspace, dataspace);
        
        // copy over to an array with 1 dim. only, for convenience
        long *buffer = new long[nblock[0]]; // = same as malloc

        for (int i=0;i<nblock[0];i++) {
            buffer[i] = rdata[i][0]; // should use memcpy or similar for faster copying!
        }

        // the data is stored in buffer now, so we can delete the dataset etc.;
        // to do this, call delete on the pointer to the dataset
        dataspace.close();
        memspace.close();
        dataset.close();
        // delete dataset is not necessary, if it is a variable on the heap.
        // Then it is removed automatically when the function ends.
        delete dptr;

        DataBlock b;
        b.nvalues = nvalues;
        b.longval = buffer;
        b.name = s;
        datablocks.push_back(b);
        // block b with the data is added to datablocks-vector now

        return buffer;
    }


    double* SagReader::readDoubleDataSet(const std::string s, long &nvalues, hsize_t *nblock, hsize_t *offset) {
        // read a double-type dataset
        // TODO: not tested yet, since SAG-data only contain floats (4 byte)

        hsize_t count[2];   // size of the hyperslab in the file
        hsize_t stride[2];  // should be 1,1
        hsize_t block[2];   // block size, should use nblock-values

        //cout << "Reading DataSet '" << s << "'" << endl;

        DataSet *dptr = new DataSet(fp->openDataSet(s));
        DataSet dataset = *dptr; // for convenience

        // check class type
        H5T_class_t type_class = dataset.getTypeClass();
        if (type_class != H5T_FLOAT) {
            cout << "Data does not have double type!" << endl;
            abort();
        }
        // check byte order
        FloatType intype = dataset.getFloatType();
        H5std_string order_string;
        H5T_order_t order = intype.getOrder(order_string);
        //cout << order_string << endl;

        // check again data sizes
        if (sizeof(double) != intype.getSize()) {
            cout << "Mismatch of double data type." << endl;
            abort();
        }

        size_t dsize = intype.getSize();
        //cout << "Data size is " << dsize << endl;

        // get dataspace of the dataset (the array length or so)
        DataSpace dataspace = dataset.getSpace();
        //hid_t dataspace = H5Dget_space(dataset); --> this does not work!! At least not with dataset defined as above!

        // get number of dimensions in dataspace
        int rank = dataspace.getSimpleExtentNdims();
        //cout << "Dataspace rank is " << rank << endl;

        // I expect this to be 2 for all SAG datasets!
        // There are no more-dimensional arrays stored in one dataset, are there?
        if (rank == 1) {
            hsize_t dims_out[1];
            int ndims = dataspace.getSimpleExtentDims(dims_out, NULL);
            nvalues = dims_out[0];
        }
        else if (rank == 2) {
            hsize_t dims_out[2];
            int ndims = dataspace.getSimpleExtentDims(dims_out, NULL);
            if (dims_out[rank-1] == 1) {
                nvalues = dims_out[0];
            } else {
                cout << "ERROR: Cannot cope with this dataset, dimensions too high:" << 
                    (unsigned long)(dims_out[0]) << " x " <<
                    (unsigned long)(dims_out[1]) << endl;
                abort();
            }
        } else {
            cout << "ERROR: Cannot cope with multi-dimensional datasets! rank: " << rank << endl;
            abort();
        }

        // define hyperslab
        // offset already provided when calling this function, no need to redefine here
        count[0]  = 1;  // just use 1 block, so count = 1
        count[1]  = 1;

        stride[0] = 1;
        stride[1] = 1;
        
        block[0] = nblock[0]; // use block instead of count, might be faster
        block[1] = 1;

        hsize_t dimsm[2]; // memory space dimensions, must be the same as hyperslab-size
        dimsm[0] = nblock[0];
        dimsm[1] = 1;

        // define memory space
        DataSpace memspace(rank, dimsm, NULL);

        // selec the hyperslap from the dataspace
        dataspace.selectHyperslab(H5S_SELECT_SET, count, offset, stride, block); 
        //dataspace.selectHyperslab( H5S_SELECT_SET, count, offset ); 
        
        // read data from selection
        double rdata[nblock[0]][1]; // read requires buffer to have the same dimensions as dataset
        dataset.read(rdata,PredType::NATIVE_DOUBLE, memspace, dataspace);

        // copy over to an array with 1 dim. only, for convenience
        double *buffer = new double[nblock[0]]; // = same as malloc

        for (int i=0;i<nblock[0];i++) {
            buffer[i] = rdata[i][0]; // should use memcpy or similar for faster copying!
        }

        // the data is stored in buffer now, so we can delete the dataset etc.;
        // to do this, call delete on the pointer to the dataset
        dataspace.close();
        memspace.close();
        dataset.close();
        // delete dataset is not necessary, if it is a variable on the heap.
        // Then it is removed automatically when the function ends.
        delete dptr;

        DataBlock b;
        b.nvalues = nvalues;
        b.doubleval = buffer;
        b.name = s;
        datablocks.push_back(b);
        // block b with the data is added to datablocks-vector now

        return buffer;
    }


    float* SagReader::readFloatDataSet(const std::string s, long &nvalues, hsize_t *nblock, hsize_t *offset) {
        // read a float-type dataset (4 bytes, not double)

        hsize_t count[2];   // size of the hyperslab in the file
        hsize_t stride[2];  // should be 1,1
        hsize_t block[2];   // block size, should use nblock-values

        //cout << "Reading DataSet '" << s << "'" << endl;

        DataSet *dptr = new DataSet(fp->openDataSet(s));
        DataSet dataset = *dptr; // for convenience

        // check class type
        H5T_class_t type_class = dataset.getTypeClass();
        if (type_class != H5T_FLOAT) {
            cout << "Data does not have FLOAT type!" << endl;
            abort();
        }

        // check byte order
        FloatType ftype = dataset.getFloatType();
        H5std_string order_string;
        H5T_order_t order = ftype.getOrder(order_string);
        cout << order_string << endl;

        // check again data sizes
        size_t dsize = ftype.getSize();
        //cout << "Data size is " << dsize << endl;
        if (sizeof(float) != dsize) {
            cout << "Mismatch of float data type." << endl;
            abort();
        }

        // get dataspace of the dataset (the array length or so)
        DataSpace dataspace = dataset.getSpace();
 
        // get number of dimensions in dataspace
        int rank;
        rank = dataspace.getSimpleExtentNdims();

        // I expect this to be 2 for all SAG datasets!
        // There are no more-dimensional arrays stored in one dataset, are there?
        if (rank == 1) {
            hsize_t dims_out[1];
            int ndims = dataspace.getSimpleExtentDims(dims_out, NULL);
            //cout << "dimension " << (unsigned long)(dims_out[0]) << endl;
            nvalues = dims_out[0];
        }
        else if (rank == 2) {
            hsize_t dims_out[2];
            int ndims = dataspace.getSimpleExtentDims(dims_out, NULL);
            if (dims_out[rank-1] == 1) {
                nvalues = dims_out[0];
            } else {
                cout << "ERROR: Cannot cope with this dataset, dimensions too high:" << 
                    (unsigned long)(dims_out[0]) << " x " <<
                    (unsigned long)(dims_out[1]) << endl;
                abort();
            }
        } else {
            cout << "ERROR: Cannot cope with multi-dimensional datasets! rank: " << rank << endl;
            abort();
        }

        // define hyperslab
        // offset already provided when calling this function, no need to redefine here
        count[0]  = 1;  // just use 1 block, so count = 1
        count[1]  = 1;

        stride[0] = 1;
        stride[1] = 1;
        
        block[0] = nblock[0]; // use block instead of count, might be faster
        block[1] = 1;

        hsize_t dimsm[2]; // memory space dimensions, must be the same as hyperslab-size
        dimsm[0] = nblock[0];
        dimsm[1] = 1;

        // define memory space
        DataSpace memspace(rank, dimsm, NULL);

        // selec the hyperslap from the dataspace
        dataspace.selectHyperslab(H5S_SELECT_SET, count, offset, stride, block); 
        
        // read data from selection
        float rdata[nblock[0]][1]; // read requires buffer to have the same dimensions as dataset
        dataset.read(rdata,PredType::NATIVE_FLOAT, memspace, dataspace);
        
        // copy over to an array with 1 dim. only, for convenience
        float *buffer = new float[nblock[0]]; // = same as malloc

        for (int i=0;i<nblock[0];i++) {
            //cout << "rdata: " << rdata[i][0] << endl;
            buffer[i] = rdata[i][0]; // should use memcpy or similar for faster copying!
        }

        // the data is stored in buffer now, so we can delete the dataset etc.;
        // to do this, call delete on the pointer to the dataset
        dataspace.close();
        memspace.close();
        dataset.close();
        // delete dataset is not necessary, if it is a variable on the heap.
        // Then it is removed automatically when the function ends.
        delete dptr;

        DataBlock b;
        b.nvalues = nvalues;
        b.floatval = buffer;
        b.name = s;
        datablocks.push_back(b);
        // block b with the data is added to datablocks-vector now

        return buffer;
    }

    bool SagReader::getItemInRow(DBDataSchema::DataObjDesc * thisItem, bool applyAsserters, bool applyConverters, void* result) {
        //reroute constant items:

        //cout << "Name in getItemInRow: " << thisItem->getDataObjName()<< endl;
        if(thisItem->getIsConstItem() == true) {
            getConstItem(thisItem, result);
        } else if (thisItem->getIsHeaderItem() == true) {
            printf("We never told you to read headers...\n");
            exit(EXIT_FAILURE);
        } else {
            getDataItem(thisItem, result);
        }
        //cout << " again ioutput: " << ioutput << endl;
        //check assertions
        //checkAssertions(thisItem, result);
        
        //apply conversion
        //applyConversions(thisItem, result);

        return 0;
    }

    bool SagReader::getDataItem(DBDataSchema::DataObjDesc * thisItem, void* result) {
        // check which DB column is requested and assign the corresponding data value,
        // variables are declared already in Sag_Reader.h
        // and the values were read in getNextRow()
        bool isNull;
        //cout << " again2 ioutput: " << ioutput << endl;
        //NInFile = currRow-1;	// start counting rows with 0       
        
        // go through all data items and assign the correct column numbers for the DB columns, 
        // maybe depending on a read schema-file:
        
        //cout << "dataobjname: " << thisItem->getDataObjName() << endl;

        isNull = false;

        DataBlock b;
        string name;

        // quickly access the correct data block by name (should have redshift removed already),
        // but make sure that key really exists in the map
        map<string,int>::iterator it = dataSetMap.find(thisItem->getDataObjName());

        if (it != dataSetMap.end()) {
            b = datablocks[it->second];
            //cout << "b.name: " << b.name << endl;

            if (b.longval) {
                *(long*)(result) = b.longval[countInBlock];
                return isNull;
            } else if (b.doubleval) {
                *(double*)(result) = b.doubleval[countInBlock];
                return isNull;
            } else if (b.floatval) {
                *(float*)(result) = b.floatval[countInBlock];
                return isNull;
            } else {
                cout << "Error: No corresponding data found!" << " (" << thisItem->getDataObjName() << ")" << endl;
                abort();
            }
        }
            
        // get snapshot number and expansion factor from already read metadata 
        // for this output
        if (thisItem->getDataObjName().compare("snapnum") == 0) {
            *(int*)(result) = current_snapnum;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("redshift") == 0) {
            *(float*)(result) = current_redshift;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("NInFile") == 0) {
            *(long*)(result) = currRow;
            //result = (void *) countInBlock;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("fileNum") == 0) {
            *(int*) result = fileNum;
            return isNull;
        }


        if (thisItem->getDataObjName().compare("dbId") == 0) {
            *(long*)(result) = (fileNum * snapnumfactor + current_snapnum) * rowfactor + countInBlock;
            return isNull;
        }


        if (thisItem->getDataObjName().compare("forestId") == 0) {
            *(long*) result = 0;
            isNull = true;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("depthFirstId") == 0) {
            *(long*) result = 0;
            isNull = true;
            return isNull;
        }

        /*if (thisItem->getDataObjName().compare("rockstarId") == 0) {
            map<string,int>::iterator it2 = dataSetMap.find("satelliteNodeIndex");
            if (it2 != dataSetMap.end()) {
                b = datablocks[it2->second];
                if (b.longval) {
                    *(long*)(result) = b.longval[countInBlock];
                    return isNull;
                }
            } else {
                cout << "Error: No corresponding data found!" << " (satelliteNodeIndex)" << endl;
                abort();
            }
        }
        */

        if (thisItem->getDataObjName().compare("ix") == 0) {
            *(int*) result = 0;
            isNull = true;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("iy") == 0) {
            *(int*) result = 0;
            isNull = true;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("iz") == 0) {
            *(int*) result = 0;
            isNull = true;
            return isNull;
        }

        if (thisItem->getDataObjName().compare("phkey") == 0) {
            *(long*) result = 0;
            isNull = true;
            return isNull;
        }

        // --> do it on the database side;
        // or: put current x, y, z in global reader variables,
        // could calculate ix, iy, iz here, but can only do this AFTER x,y,z
        // were assigned!  => i.e. would need to check in generated schema
        // that it is in correct order!

        // if we still did not return ...
        fflush(stdout);
        fflush(stderr);
        printf("\nERROR: Something went wrong... (no dataItem for schemaItem %s found)\n", thisItem->getDataObjName().c_str());
        exit(EXIT_FAILURE);

        return isNull;
    }

    void SagReader::getConstItem(DBDataSchema::DataObjDesc * thisItem, void* result) {
        memcpy(result, thisItem->getConstData(), DBDataSchema::getByteLenOfDType(thisItem->getDataObjDType()));
    }

    void SagReader::setCurrRow(long n) {
        currRow = n;
        return;
    }

    long SagReader::getCurrRow() {
        return currRow;
    }

    long SagReader::getNumOutputs() {
        return numOutputs;
    }


    DataBlock::DataBlock() {
        nvalues = 0;
        name = "";
        idx = -1;
        doubleval = NULL;
        longval = NULL;
        floatval = NULL;
        type = "unknown";
    };

    /* // copy constructor, probably needed for vectors? -- works better without, got strange error messages when using this and trying to use push_back
    DataBlock::DataBlock(DataBlock &source) {
        nvalues = source.nvalues;
        name = source.name;
        idx = source.idx;
        doubleval = source.doubleval;
        longval = source.longval;
        type = source.type;
    }
    */

    void DataBlock::deleteData() {
        if (longval) {
            delete longval;
            nvalues = 0;
        }
        if (doubleval) {
            delete doubleval;
            nvalues = 0;
        }
        if (floatval) {
            delete floatval;
            nvalues = 0;
        }

    }

    OutputMeta::OutputMeta() {
        ioutput = 0;
        outputExpansionFactor = 0;
        outputTime = 0;
    };

    /* OutputMeta::deleteData() {
    }; */
}


// Here comes a call back function, thus it lives outside of the Sag-Reader class;
// operator function, must reside outside of Sag-class, because it's extern C?
herr_t file_info(hid_t loc_id, const char *name, const H5L_info_t *linfo, void *opdata) {
    // display name of group/dataset
    //cout << "Name : " << name << endl;
    //cout << name << endl;

    // check if this is a group or a dataset
    herr_t status;
    H5O_info_t object_info;
    hid_t lapl_id;
    status = H5Oget_info_by_name(loc_id, name, &object_info, H5P_DEFAULT);

    cout << "name, status: " << name << "," << status << endl;
    cout << "type: " << object_info.type << endl;
    //cout << "type: " << object_info.H5O_type_t << endl;

    // if group, then search for further datasets:
/*    if (object_info.type == H5O_TYPE_GROUP) {
        grpid = H5Gopen(gid,memb_name, H5P_DEFAULT);
        Group group(loc_id.openGroup(name));
        // maybe check here that it worked?
        int idx2  = H5Literate(loc_id, H5_INDEX_NAME, H5_ITER_INC, NULL, file_info, opdata);
    } else {
*/
    // store name in string vector, pointer to this is provided as parameter
    vector<string>* n = (vector<string>*) opdata;
    n->push_back(name);
//    }


    return 0;
}

// also see https://www.hdfgroup.org/ftp/HDF5/examples/introductory/C/h5getinfo.c
#define MAX_NAME 1024

void scan_group(hid_t gid, void *opdata) { 
    int i;
    ssize_t len;
    hsize_t nobj;
    herr_t err;
    int otype;
    hid_t grpid;
    hid_t datatypeid;
    id_t  dsid;
    char group_name[MAX_NAME];
    char memb_name[MAX_NAME];
    char dataset_name[MAX_NAME];

    len = H5Iget_name (gid, group_name, MAX_NAME);
    err = H5Gget_num_objs(gid, &nobj);
    for (i = 0; i < nobj; i++) {
        // for each group object, get its name and type
        // if type is group: scan recursively
        // if type is dataset: add to vector of datasetNames
        len = H5Gget_objname_by_idx(gid, (hsize_t) i, 
            memb_name, (size_t) MAX_NAME );
        otype =  H5Gget_objtype_by_idx(gid, (size_t) i );

        vector<string>* n = (vector<string>*) opdata;

        switch(otype) {
            case H5G_LINK:
                // skip symlinks
                break;
            case H5G_GROUP:
                grpid = H5Gopen(gid, memb_name, H5P_DEFAULT);
                // recursively scan group for more datasets
                scan_group(grpid, opdata);
                H5Gclose(grpid);
                break;
            case H5G_DATASET:
                {
                    dsid = H5Dopen(gid, memb_name, H5P_DEFAULT);
                    // store name of dataset in vector;
                    len = H5Iget_name (dsid, dataset_name, MAX_NAME);
                    cout << "found dataset_name: " << dataset_name << endl;
                    n->push_back(dataset_name);
                    H5Dclose(dsid);
                    break;
                }
            case H5G_TYPE:
                datatypeid = H5Topen(gid, memb_name, H5P_DEFAULT);
                // skip datatypes
                H5Tclose(datatypeid);
                break;
            default:
                cout << "Unknown datatype encountered. Skipping." << endl;
                break;
            }

        }
}
