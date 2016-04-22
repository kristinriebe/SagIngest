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

#include <Reader.h>
#include <string>
#include <fstream>
#include <stdio.h>
#include <assert.h>
#include <list>
#include <sstream>
#include <map>

#ifndef Sag_Sag_Reader_h
#define Sag_Sag_Reader_h

using namespace DBReader;
using namespace DBDataSchema;
using namespace std;
//using cout;
//using endl;
#include "H5Cpp.h"
using namespace H5;

extern "C" herr_t file_info(hid_t loc_id, const char *name, const H5L_info_t *linfo,
                                    void *opdata);
extern "C" void scan_group(hid_t gid, void *opdata);

namespace Sag {
    
    class OutputMeta {
        public:
            int ioutput;
            int snapnum; // usually the same as ioutput, but we never know ...
            string outputName;
            float outputExpansionFactor; // scale
            float outputTime;

            OutputMeta();
    };


    class DataBlock {
        public:
            long nvalues;   // number of values in the block
            string name;
            long idx;
            double *doubleval;
            float *floatval;
            long *longval;
            int8_t *tinyintval;
            string type;

            DataBlock();
            //DataBlock(DataBlock &source);

            void deleteData();
    };
    // This custom DataBlock-class is similar to the DataSet-class, 
    // but if using hyperslabs, it contains only a part of the data.
    
    class SagReader : public Reader {
    private:
        string fileName;
        string mapFile;

        ifstream fileStream;

        H5File* fp; //holds the opened hdf5 file
        long ioutput; // number of current output
        long numOutputs; // total number of outputs (one for reach redshift)
        long numDataSets; // number of DataSets (= row fields, = columns) in each output
        long nvalues; // values in one dataset (assume the same number for each dataset of the same output group (redshift))
        long blocksize; // number of elements in one read-block, should be small enough to fit (blocksize * number of datasets) into memory

        long snapnumfactor;
        long rowfactor;

        vector<string> dataSetNames; // vector containing names of the HDF5 datasets
        map<string,int> dataSetMap;

        // improve performance by defining it here (instead of inside getItemInRow)
        string tmpStr;

        long currRow;
        long countInBlock;
        int countSnap;

        int current_snapnum;
        vector<int> user_snapnums;
        long NInFileSnapnum;
        double scale;
        float current_redshift;

        int fileNum;

        int ix;
        int iy;
        int iz;
        long phkey;
        
        // define something to hold all datasets from one read block 
        // (one complete Output* block or a part of it)
        vector<DataBlock> datablocks;

    public:
        SagReader();
        SagReader(string newFileName, int fileNum, int newBlocksize, vector<string> datafileFieldNames);
        // DBDataSchema::Schema*&
        ~SagReader();

        void openFile(string newFileName);

        void closeFile();

        void getMeta(vector<string> datafileFieldNames);

        int getNextRow();
        int readNextBlock(long blocksize);
        long* readLongDataSet(const std::string s, long &nvalues, hsize_t *nblock, hsize_t *offset);
        int8_t* readTinyIntDataSet(const std::string s, long &nvalues, hsize_t *nblock, hsize_t *offset);
    
        double* readDoubleDataSet(const string s, long &nvalues, hsize_t *nblock, hsize_t *offset);
        float* readFloatDataSet(const std::string s, long &nvalues, hsize_t *nblock, hsize_t *offset);
 
        long getNumRowsInDataSet(string s);

        vector<string> getDataSetNames();

        void setCurrRow(long n);
        long getCurrRow();
        long getNumOutputs();

        int getSnapnum(long ioutput);
        
        bool getItemInRow(DBDataSchema::DataObjDesc * thisItem, bool applyAsserters, bool applyConverters, void* result);

        bool getDataItem(DBDataSchema::DataObjDesc * thisItem, void* result);

        void getConstItem(DBDataSchema::DataObjDesc * thisItem, void* result);
    };
    
}

#endif
