#include <iostream>
#include "qe.h"

using namespace std;

void qe_dump_tuple_element(const void *data, const Attribute &a)
{
    char *data_ptr = (char *) data;

    cout << "(";

    if(a.type == TypeInt)
    {
        cout << (*(int *) data_ptr);
    }
    else if(a.type == TypeReal)
    {
        cout << (*(float *) data_ptr);
    }
    else if(a.type == TypeVarChar)
    {
        unsigned int len = (*(unsigned *) data_ptr);
        data_ptr += sizeof(unsigned);
        cout << '"' << string((char *) data_ptr, len) << '"';
    }

    cout << ")" << endl;

}
void qe_dump_tuple(const void *data, const vector<Attribute> &attrs)
{
    unsigned char *data_ptr = (unsigned char *) data;

    //cout << "[Begin Data Dump]" << endl;
    //for (unsigned int i = 0; i < attrs.size(); i++)
    //{
    //    Attribute a = attrs[i];

    //    cout << "(" << a.name;
    //    if(a.type == TypeInt)
    //        cout << " int";
    //    else if(a.type == TypeReal)
    //        cout << " float";
    //    else if(a.type == TypeVarChar)
    //        cout << " char[" << a.length << "]";

    //    if ((i+1) != attrs.size())
    //        cout << ",";
    //}

    //cout << ")" << endl;
    cout << "(";

    for (unsigned int i = 0; i < attrs.size(); i++)
    {
        Attribute a = attrs[i];

        if(a.type == TypeInt)
        {
            cout << (*(int *) data_ptr);
            data_ptr += sizeof(int);
        }
        else if(a.type == TypeReal)
        {
            cout << (*(float *) data_ptr);
            data_ptr += sizeof(float);
        }
        else if(a.type == TypeVarChar)
        {
            unsigned int len = (*(unsigned *) data_ptr);
            data_ptr += sizeof(unsigned);
            cout << '"' << string((char *) data_ptr, len) << '"';
            data_ptr += len;
        }

        if ((i+1) != attrs.size())
            cout << ",";
    }

    cout << ")" << endl;
}

void qe_dump_attribute(Attribute &a)
{
    cout << "name: " << a.name << " type: ";

    if(a.type == TypeInt)
        cout << "int   ";
    else if(a.type == TypeReal)
        cout << "float ";
    else if(a.type == TypeVarChar)
        cout << "char  ";

    cout << " len: " << a.length << endl;
}

void qe_dump_attributes(vector<Attribute> &attrs)
{
    cout << "[Begin Attributes]" << endl;

    for (unsigned int i=0; i < attrs.size(); i++)
    {
        cout << "  ";
        qe_dump_attribute(attrs[i]);
    }

    cout << "[End Attributes]" << endl;
}
void qe_getAttribute(string &name, vector<Attribute> &attrs, Attribute &a)
{
    for (unsigned int i = 0; i < attrs.size(); i++)
    {
        if(name == attrs[i].name)
        {
             a = attrs[i];
             return;
        }
    }
}

void qe_dump_condition(Condition &c, vector<Attribute> &attrs)
{
    Attribute lhs;
    qe_getAttribute(c.lhsAttr, attrs, lhs);

    cout << "[Begin Condition]" << endl;
    cout << "  left attr: " << c.lhsAttr << endl;

    if(lhs.type == TypeInt)
        cout << "  type: " << "int" << endl;
    else if(lhs.type == TypeReal)
        cout << "  type: " << "float" << endl;
    else if(lhs.type == TypeVarChar)
        cout << "  type: " << "char" << endl;

    cout << "  op: ";

    if (c.op == EQ_OP)
        cout << "EQ_OP (==)" << endl;

    if (c.op == NE_OP)
        cout << "NE_OP (!=)" << endl;

    if (c.op == GT_OP)
        cout << "GT_OP (>)" << endl;

    if (c.op == GE_OP)
        cout << "GE_OP (>=)" << endl;

    if (c.op == LT_OP)
        cout << "LT_OP (<)" << endl;

    if (c.op == LE_OP)
        cout << "LE_OP (<=)" << endl;

    if (c.op == NO_OP)
        cout << "NO_OP" << endl;

    if (c.bRhsIsAttr)
    {
        Attribute rhs;
        qe_getAttribute(c.rhsAttr, attrs, rhs);

        cout << "  left attr: " << c.rhsAttr << endl;

        if(rhs.type == TypeInt)
            cout << "  type: " << "int" << endl;
        else if(rhs.type == TypeReal)
            cout << "  type: " << "float" << endl;
        else if(rhs.type == TypeVarChar)
            cout << "  type: " << "char" << endl;
    }
    else
    {
        cout << "right attr: [value]" << endl;

        if(c.rhsValue.type == TypeInt)
        {
            cout << "  type: " << "int" << endl;
            cout << "  value: " << (*(int *) c.rhsValue.data) << endl;
        }
        else if(c.rhsValue.type == TypeReal)
        {
            cout << "  type: " << "float" << endl;
            cout << "  value: " << (*(float *) c.rhsValue.data) << endl;
        }
        else if(c.rhsValue.type == TypeVarChar)
        {
            int len = (*(int *) c.rhsValue.data);
            string s = string(((char *) c.rhsValue.data) + sizeof(int), len);
            cout << "  type: " << "char" << endl;
            cout << "  value: " << s << endl;
        }
    }

    cout << "[End Condition]" << endl;
}
