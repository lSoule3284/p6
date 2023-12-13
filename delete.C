#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6
    Status executeStatus;
    RID recordID;
    int countOfDeletedTuples = 0;
    HeapFileScan scan(relation, executeStatus);
    
    if (executeStatus != OK) {
        return executeStatus;
        }

    AttrDesc descriptor;
    const char* comparisonValue;

    if(attrName.empty()) {
        executeStatus = scan.startScan(0, 0, STRING, NULL, EQ);
        if (executeStatus != OK) {
            return executeStatus;
            }
        while (scan.scanNext(recordID) == OK) {
            executeStatus = scan.deleteRecord();
            if (executeStatus != OK) { 
            return executeStatus; 
            }
            countOfDeletedTuples++;
        }
        return OK;
    }

    executeStatus = attrCat->getInfo(relation, attrName, descriptor);
    
    if (executeStatus != OK) { 
        return executeStatus; 
        }

    switch (type) {
        case STRING:
            comparisonValue = attrValue;
            break;
        case INTEGER: {
            int intValue = *(reinterpret_cast<const int*>(attrValue));
            comparisonValue = reinterpret_cast<const char*>(&intValue);
            break;
        }
        case FLOAT: {
            float floatValue = *(reinterpret_cast<const float*>(attrValue));
            comparisonValue = reinterpret_cast<const char*>(&floatValue);
            break;
        }
    }

    executeStatus = scan.startScan(descriptor.attrOffset, descriptor.attrLen, type, comparisonValue, op);
    
    if (executeStatus != OK) { 
        return executeStatus; 
        }
    while (scan.scanNext(recordID) == OK) {
        executeStatus = scan.deleteRecord();
        if (executeStatus != OK) { 
            return executeStatus; 
        }
        countOfDeletedTuples++;
    }

    return OK;
}


