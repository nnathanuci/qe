#include <fstream>
#include <iostream>
#include <cassert>
#include <sstream>

#include <vector>

#include "qe.h"

using namespace std;

// Global Initialization
RM *rm = RM::Instance();
IX_Manager *ixManager = IX_Manager::Instance();

const int success = 0;

// Number of tuples in each relation
const int tuplecount = 100;

// Buffer size and character buffer size
const unsigned bufsize = 500;

void cleanup() // {{{
{
  const char *files[5] = { "systemcatalog", "t1.A", "t1.B", "t1.C", "t1" };

  for(int i = 0; i < 5; i++)
    remove(files[i]);

} // }}}

void prepareTuple(const int a, const int b, const float c, void *buf) // {{{
{    
    int offset = 0;
    
    memcpy((char *)buf + offset, &a, sizeof(int));
    offset += sizeof(int);
    
    memcpy((char *)buf + offset, &b, sizeof(int));
    offset += sizeof(int);
    
    memcpy((char *)buf + offset, &c, sizeof(float));
    offset += sizeof(float);
} // }}}

void prepareTuple(const int a, const int b, const float c, string s1, string s2, void *buf) // {{{
{    
    int offset = 0;
    unsigned int len = 0;
    
    memcpy((char *)buf + offset, &a, sizeof(int));
    offset += sizeof(int);
    
    memcpy((char *)buf + offset, &b, sizeof(int));
    offset += sizeof(int);
    
    memcpy((char *)buf + offset, &c, sizeof(float));
    offset += sizeof(float);

    len = s1.length();
    memcpy((char *)buf + offset, &len, sizeof(len));
    offset += sizeof(len);
    memcpy((char *)buf + offset, s1.c_str(), len);
    offset += len;

    len = s2.length();
    memcpy((char *)buf + offset, &len, sizeof(len));
    offset += sizeof(len);
    memcpy((char *)buf + offset, s2.c_str(), len);
    offset += len;
} // }}}

void createTable() // {{{
{
    // Functions Tested;
    // 1. Create Table
    cout << "****t1 Create Table****" << endl;

    vector<Attribute> attrs;

    Attribute attr;
    attr.name = "A";
    attr.type = TypeInt;
    attr.length = 4;
    attrs.push_back(attr);

    attr.name = "B";
    attr.type = TypeInt;
    attr.length = 4;
    attrs.push_back(attr);

    attr.name = "C";
    attr.type = TypeReal;
    attr.length = 4;
    attrs.push_back(attr);

    attr.name = "D";
    attr.type = TypeVarChar;
    attr.length = 100;
    attrs.push_back(attr);

    attr.name = "E";
    attr.type = TypeVarChar;
    attr.length = 100;
    attrs.push_back(attr);


    RC rc = rm->createTable("t1", attrs);
    assert(rc == success);
    cout << "****t1 Table Created!****" << endl;
} // }}}

void populateTable(vector<RID> &rids) // {{{
{
    // Functions Tested
    // 1. InsertTuple
    RID rid;
    void *buf = malloc(bufsize);

    for(int i = 0; i < tuplecount; ++i)
    {
        memset(buf, 0, bufsize);
        
        // Prepare the tuple data for insertion
        // a in [0,99], b in [0, 9], c in [0, 49.0]
        int a = i;
        int b = i % 10;
        float c = (float)(i % 50);
        stringstream s1;
        stringstream s2;

        s1 << a;
        s2 << b;
        
        prepareTuple(a, b, c, s1.str(), s2.str(), buf);

        RC rc = rm->insertTuple("t1", buf, rid);
        assert(rc == success);
        rids.push_back(rid);
    }
    
    free(buf);
} // }}}

void testcase1()
{
    // Functions Tested;
    // 1. Filter -- TableScan as input, on Integer Attribute
    cout << "****In Test Case 1****" << endl;
    
    TableScan *ts = new TableScan(*rm, "t1");
    
    vector<string> attrNames;
    vector<Attribute> proj_attrs;

    {
        Attribute attr;

        attr.name = "D";
        attr.type = TypeVarChar;
        attr.length = 100;
        proj_attrs.push_back(attr);

        attr.name = "B";
        attr.type = TypeInt;
        attr.length = 4;
        proj_attrs.push_back(attr);

        attrNames.push_back("t1.D");
        attrNames.push_back("t1.B");
    }

    // Create Projector 
    Project project(ts, attrNames);
    
    // Go over the data through iterator
    void *data = malloc(bufsize);
    cout << "D B" << endl;
    while(project.getNextTuple(data) != QE_EOF)
    {
        unsigned int offset = 0;
        unsigned int len = 0;

        qe_dump_data(data, proj_attrs);
        
        memset(data, 0, bufsize);
    }
   
    free(data);
    return;
}

int main()
{
    cleanup();

    // create table for selections
    vector<RID> t1RIDs;
    createTable();
    populateTable(t1RIDs);
    
   
    // Test Cases
    testcase1();

    return 0;
}

