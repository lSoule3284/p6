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
 *
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
    int recordlen = 0;
    Operator tempEQ;
    const char* filter;
    tempAttrDesc = new AttrDesc[projCnt]; //hold desc of attributes

    //loop to find the lngth of the records
    for (int i = 0; i < projCnt; i++) {
        Status status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, tempAttrDesc[i]);
        if (status != OK) {
            delete[] tempAttrDesc;
            return status; 
            }
        recordlen += tempAttrDesc[i].attrLen;
    }

    //begin checking attributes to initiate our SELECT
    if (attr != NULL) {
        status = attrCat->getInfo(string(attr->relName), string(attr->attrName), attrDesc);
        if (status != OK) {
        cerr << "Error: Status produced an error before case switching." << endl;
        return status;
        }
        int tempoInt;
        float tempoFloat;
        switch (attr->attrType) {
            //ensure proper data types
            case INTEGER:
                if (!convertToInt(attrValue, tempoInt)) {
                    return ERR_BAD_VALUE;
                    }
                filter = (char*)&tempoInt;
                break;
            case FLOAT:
                if (!convertToFloat(attrValue, tempoFloat)) {
                    return ERR_BAD_VALUE;
                    }
                filter = (char*)&tempoFloat;
                break;
            case STRING:
                filter = attrValue;
                break;
            //in case no proper data type found
            default:
                return ERR_BAD_VALUE;
        }
        tempEQ = op;

    } else {// having a null indicates that we will be missing WHERE clause
        //hence, we set everything to default values
        attrDesc.attrOffset = 0;
        attrDesc.attrLen = 0;
        attrDesc.attrType = STRING;
        strcpy(attrDesc.relName, projNames[0].relName);
        strcpy(attrDesc.attrName, projNames[0].attrName);  
        filter = NULL;
        tempEQ = EQ;
    }
    status = ScanSelect(result, projCnt, tempAttrDesc, &attrDesc, tempEQ, filter, recordlen);
    if (status != OK) { 
        return status; 
        }
    return OK;
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;


}
