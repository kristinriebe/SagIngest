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

#include "Sag_SchemaMapper.h"
#include <SchemaItem.h>
#include <DataObjDesc.h>
#include <DType.h>
#include <DBType.h>
#include <stdlib.h>

using namespace std;
using namespace DBDataSchema;

namespace Sag {
    
    SagSchemaMapper::SagSchemaMapper() {
        
    }

    SagSchemaMapper::SagSchemaMapper(DBAsserter::AsserterFactory * newAssertFac, DBConverter::ConverterFactory * newConvFac) {
        assertFac = newAssertFac;
        convFac = newConvFac;
    }

    SagSchemaMapper::~SagSchemaMapper() {
        
    }


    DataField::DataField() {
        name = "";
        type = "unknown";
    }

    DataField::DataField(string newName) {
        name = newName;
        type = "unknown";
    }
    DataField::DataField(string newName, string newType) {
        name = newName;
        type = newType;
    }
   
    void SagSchemaMapper::readMappingFile(string mapFile) {
        // read file, store content

        // or get fieldnames from Sag-Reader?
        //get_datafileFieldNames;

        // read (column) mapper for fields from dataFile to fields in database,
        // assume simple ascii file:
        // 1. column = name in dataFile
        // 2. column = name of field in database table
        // automatic type assignment based on type in data file in SchemaMapper
        // (so must make sure beforehand that database table has correct types for its fields)

        string fileName;
        ifstream fileStream;
        string line;
        string name;
        string type;
        stringstream ss;
        
        DataField dataField;
        
        //fileName = string("testdata.fieldmap");
        fileStream.open(mapFile.c_str(), ios::in);
        
        if (!fileStream) {
            cout << "ERROR: Cannot open file '" << mapFile << "'. Maybe it does not exist?" << endl;
            abort(); 
        }

        datafileFields.clear();
        databaseFields.clear();

        char *piece = NULL;
        char linechar[1024] = "";

        while ( getline (fileStream, line) ){
            //cout << "line: " << line << ", schema size: " << line.size() << endl;
 
            // skip empty lines and lines starting with #
            if (line.substr(0,1) != "#" && line.size() > 0) {

                // use here either streaming or strtok ...
                // but streaming is easier for handling tabs and whitespace

                dataField.name = "";
                dataField.type = "";

                ss.clear();
                ss << line;
                ss >> name;
                ss >> type;

                //cout << "name, type (file): '" << name << "', '" << type << "'" << endl;
                dataField.name = name.c_str();
                dataField.type = type.c_str();
                datafileFields.push_back(dataField);

                ss >> name;
                ss >> type;
                //cout << "name, type (db): '" << name << "', '" << type << "'" << endl;

                dataField.name = name.c_str();
                dataField.type = type.c_str();
                databaseFields.push_back(dataField);

            }

        }

        fileStream.close();

        cout << "DataFields (file, database): " << datafileFields.size() << endl;
        for (int j=0; j<datafileFields.size(); j++) {
            cout << "  Fieldnames " << j << ":" << datafileFields[j].name << ", " << databaseFields[j].name << endl;
            cout << "  Fieldtypes " << j << ":" << datafileFields[j].type << ", " << databaseFields[j].type << endl;
        }

    }

    DBDataSchema::Schema * SagSchemaMapper::generateSchema(string dbName, string tblName) {
        DBDataSchema::Schema * returnSchema = new Schema();

        string datafileFieldName;
        string databaseFieldName;

        string dbtype_str;
        string dtype_str;

        DType dtype;
        DBType dbtype;

        //set database and table name
        returnSchema->setDbName(dbName);
        returnSchema->setTableName(tblName);

        //vector<SchemaItem*> arrSchemaItems;
        //vector<DataObjDesc*> vec2;

        //cout << "Sizes: " << datafileFields.size() << ", " << databaseFields.size() << endl;

        //setup schema items and add them to the schema
        for (int j=0; j<databaseFields.size(); j++) {

            datafileFieldName = datafileFields[j].name;
            databaseFieldName = databaseFields[j].name;

            dtype_str  = datafileFields[j].type;    // data file type
            dtype = DBDataSchema::convDType(dtype_str);

            dbtype_str = databaseFields[j].type;    // database field type
            if (dbtype_str == "DOUBLE") {
                dbtype_str = "REAL";
            }
            dbtype = getDBType(dbtype_str); 
            
            //first create the data object describing the input data:
            DataObjDesc* colObj = new DataObjDesc();
            colObj->setDataObjName(datafileFieldName);	// column name (data file)
            colObj->setDataObjDType(dtype);	// data file type
            colObj->setIsConstItem(false, false); // not a constant
            colObj->setIsHeaderItem(false);	// not a header item
        
            //then describe the SchemaItem which represents the data on the server side
            SchemaItem* schemaItem = new SchemaItem();
            schemaItem->setColumnName(databaseFieldName);	// field name in Database
            schemaItem->setColumnDBType(dbtype);		// database field type
            schemaItem->setDataDesc(colObj);		// link to data object

            //add schema item to the schema
            returnSchema->addItemToSchema(schemaItem);

        }

        // add some more schema items manually, for snapshot number, id, ...
        // => no, can add them to the file as well and process with the loop above

        return returnSchema;
    }

    DBType SagSchemaMapper::getDBType(string thisDBType) {
        
        if (thisDBType == "CHAR") {
            return DBT_CHAR;
        }
        if (thisDBType == "BIT") {
            return DBT_BIT;
        }
        if (thisDBType == "BIGINT") {
            return DBT_BIGINT;
        }
        if (thisDBType == "MEDIUMINT") {
            return DBT_MEDIUMINT;
        }
        if (thisDBType == "INTEGER") {
            return DBT_INTEGER;
        }
        if (thisDBType == "SMALLINT") {
            return DBT_SMALLINT;
        }
        if (thisDBType == "TINYINT") {
            return DBT_TINYINT;
        }
        if (thisDBType == "FLOAT") {
            return DBT_FLOAT;
        }
        if (thisDBType == "REAL") {
            return DBT_REAL;
        }
        if (thisDBType == "DATE") {
            return DBT_DATE;
        }
        if (thisDBType == "TIME") {
            return DBT_TIME;
        }
        if (thisDBType == "ANY") {
            return DBT_ANY;
        }
        if (thisDBType == "UBIGINT") {
            return DBT_UBIGINT;
        }
        if (thisDBType == "UMEDIUMINT") {
            return DBT_UMEDIUMINT;
        }
        if (thisDBType == "UINTEGER") {
            return DBT_UINTEGER;
        }
        if (thisDBType == "USMALLINT") {
            return DBT_USMALLINT;
        }
        if (thisDBType == "UTINYINT") {
            return DBT_UTINYINT;
        }
        if (thisDBType == "UFLOAT") {
            return DBT_UFLOAT;
        }
        if (thisDBType == "UREAL") {
            return DBT_UREAL;
        }
    
        // error: type not known
        return (DBType)0;    
    }
 

}
