#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result,
			const int projCnt,
			const AttrDesc projNames[],
			const AttrDesc *attrDesc,
			const Operator op,
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 * A selection is implemented using a filtered HeapFileScan.  The result of the selection is stored in the result relation table
 * The project list is defined by the parameters projCnt and projNames.  Projection should be done on the fly as each result tuple is being appended to the result table.  
 * we convert it to the proper type based on the type of attr. If attr is NULL, an unconditional scan of the input table should be performed.
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result,
        const int projCnt,
        const attrInfo projNames[],
        const attrInfo *attr,
        const Operator op,
        const char *attrValue)
{
    Status status;
    AttrDesc attrDesc;
    AttrDesc* tempAttrDesc;
    int reclen = 0;
    Operator tempEQ;
    const char* filter;
    tempAttrDesc = new AttrDesc[projCnt]; //hold desc of attributes
    int tempoInt; //holders to convert attr to proper types
    float tempoFloat; //holders to convert attr to proper types

    //loop to find the lngth of the records
    for (int i = 0; i < projCnt; i++) {
        Status status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, tempAttrDesc[i]);
        if (status != OK) {
            delete[] tempAttrDesc;
            return status; 
            }
        reclen += tempAttrDesc[i].attrLen;
    }
    //begin checking attributes to initiate our SELECT
    if (attr != NULL) {
        status = attrCat->getInfo(string(attr->relName), string(attr->attrName), attrDesc);
        if (status != OK) {
            cerr << "Error: Status produced an error before case switching." << endl;
            return status;
            }
        switch (attr->attrType) {
            //ensure proper data types
            case FLOAT:
                tempoFloat = atof(attrValue);
                filter = (char*)&tempoFloat;
                break;
            case INTEGER:
                tempoInt = atoi(attrValue);
                filter = (char*)&tempoInt;
                break;
            case STRING:
                filter = attrValue;
                break;
            //in case no proper data type found
            default:
                cerr << "Error: Improper data type found." << endl;
        }
        tempEQ = op;

    } else {// having a null indicates that we will be missing WHERE clause
        //hence, we set everything to the default values for our unconditional scan
        attrDesc.attrOffset = 0;
        attrDesc.attrLen = 0;
        attrDesc.attrType = STRING;
        strcpy(attrDesc.relName, projNames[0].relName);
        strcpy(attrDesc.attrName, projNames[0].attrName);  
        filter = NULL;
        tempEQ = EQ;
    }
    //relay onto 2nd select function
    status = ScanSelect(result, projCnt, tempAttrDesc, &attrDesc, tempEQ, filter, reclen);
    if (status != OK) { 
        return status; 
        }
    //OK on success
    return OK;
}


/*
 * Scan our relation based on the provided filter.
 * Then adds it to a relation named "result"
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status ScanSelect(const string & result,
        const int projCnt,
        const AttrDesc projNames[],
        const AttrDesc *attrDesc,
        const Operator op,
        const char *filter,
        const int reclen)
{
    #include "stdio.h"
    #include "stdlib.h"
    int tupilecount = 0; //temp variable to count tuples
    Status status;
    int offset = 0;
    InsertFileScan resultRel(result, status); //open result relation
    if (status != OK) { 
        return status; //return false if not opened properly
        }
    char outputData[reclen]; //pointer of the location of reclen
    Record returnedRec; 
    returnedRec.data = (void *) outputData; //cast unto data
    returnedRec.length = reclen;
    // prepare for scan on outer table
    HeapFileScan relScan(attrDesc->relName, status);
    if (status != OK) {
        delete[] outputData;
        return status; 
        }
    status = relScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, filter, op);
    if (status != OK) { 
        delete[] outputData;
        return status; 
        }
    // initiate outer table scan
    RID relRID;
    Record relRec;
    while (relScan.scanNext(relRID) == OK) {
        status = relScan.getRecord(relRec);
        offset = 0;
        if (status != OK) {
        return status; 
        }
        //copy all data from relREC into our offset buffer using the projNames for reference
        for (int i = 0; i < projCnt; i++) {
            memcpy(outputData + offset, (char *)relRec.data + projNames[i].attrOffset, projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }
        //insert the new record into our output buffer
        RID outRID;
        status = resultRel.insertRecord(returnedRec, outRID);
        if (status != OK) {
        return status; 
        }
        tupilecount = tupilecount + 1;
    }
    return OK;
}
