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
    int countOfDeletedTuples = 0; // Counter for deleted records
    HeapFileScan scan(relation, executeStatus); // Scanner for the relation

    // Check for errors in opening the relation
    if (executeStatus != OK) {
        return executeStatus; 
        }

    AttrDesc descriptor; // Descriptor for attribute information
    const char* comparisonValue; // Pointer for the value used in comparison

    // Check if attribute name is empty, indicating deletion of all tuples
    if(attrName.empty()) {
        executeStatus = scan.startScan(0, 0, STRING, NULL, EQ);
        if (executeStatus != OK) {
            return executeStatus; 
            }
        // Delete all records
        while (scan.scanNext(recordID) == OK) {
            executeStatus = scan.deleteRecord();
            if (executeStatus != OK) {
                return executeStatus;
                }
            countOfDeletedTuples++;
        }
        return OK;
    }

    // Get information about the attribute
    executeStatus = attrCat->getInfo(relation, attrName, descriptor);
    if (executeStatus != OK) {
        return executeStatus; 
        }

    int intValue = atoi(attrValue);
    float floatValue = atof(attrValue);

    // Determine the type of the attribute and set the comparison value accordingly
    switch (type) {
        case STRING:
            comparisonValue = attrValue; // Direct assignment for string type
            break;
        case INTEGER:
            comparisonValue = (char*)&intValue; // Pointer to integer value
            break;
        case FLOAT:
            comparisonValue = (char*)&floatValue; // Pointer to float value
            break;
    }

    // Start the scan with the given attribute details
    executeStatus = scan.startScan(descriptor.attrOffset, descriptor.attrLen, type, comparisonValue, op);
    
    if (executeStatus != OK) {
        return executeStatus;
        }

    // Loop through the records and delete those that meet the criteria
    while (scan.scanNext(recordID) == OK) {
        executeStatus = scan.deleteRecord();
        if (executeStatus != OK) {
            return executeStatus;
            }

        countOfDeletedTuples++;
    }

    return OK;
}
