#include "catalog.h"
#include "query.h"
#include <stdlib.h>


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
  	int attrCntReal;
	AttrDesc *attrs;
  	Status status;
	//Get our relation info, if issue getting info, return the error
	status = attrCat->getRelInfo(relation, attrCntReal, attrs);
	if(status != OK) return status;
	//Check to make sure that our attribute counts match, if not return error
	if(attrCntReal != attrCnt) return UNIXERR;
	//get our record size
  	int recordLength = 0;
  	for(int i = 0; i < attrCnt; i++) {
    	recordLength += attrs[i].attrLen;
	}
	//open an insertFileScan
  	InsertFileScan rel(relation, status);
  	if(status != OK) return status;
	//Create a pointer for our insert data
  	char* data = new (std::nothrow) char[recordLength];
	if (!data) return INSUFMEM;
	//create placeholders for our values
	int offset = 0;
	int intVal = 0;
	float floatVal = 0;
	//iterate and look for matches
	for(int i = 0; i < attrCnt; i++) {
		bool end = true;
		for(int j = 0; j < attrCnt; j++) {
			//if match 
			if(strcmp(attrs[i].attrName, attrList[j].attrName) == 0) {
				//don't need to check null value since implemented in the parser
				//get the attributes offset
				offset = attrs[i].attrOffset;
				//if attribute is a string, copy
				if(attrList[j].attrType == STRING) {
					memcpy((char *)data + offset, (char *)attrList[j].attrValue, attrs[i].attrLen);
				}
				//if attribute integer, convert to integer and copy
				else if(attrList[j].attrType == INTEGER) {
					intVal = atoi((char *)attrList[j].attrValue);
				 	memcpy((char *)data + offset, &intVal, attrs[i].attrLen);
				}
				//attribute must be float, convert to float and copy		 		
				else {
					floatVal = atof((char *)attrList[j].attrValue);		
					memcpy((char *)data + offset, &floatVal, attrs[i].attrLen);
				}
				end = false;
				break;
			}
		}
		//if no match, then release mem and delete the data
		if (end){
			free(attrs);
			delete data;
			return UNIXERR;
		}
	}
	//create record
  	Record dataRecord;
  	dataRecord.data = (void *) data;
  	dataRecord.length = recordLength;
  	RID insertDataRecord;
	//Insert record
  	status = rel.insertRecord(dataRecord, insertDataRecord);
	//clean mem
	delete [] data;
	free(attrs);

	return status;
}