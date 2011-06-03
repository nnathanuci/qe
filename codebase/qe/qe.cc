#include <iostream>
#include "qe.h"

using namespace std;

void qe_dump_tuple_element(const void *data, const Attribute &a) // {{{
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

} // }}}

void qe_dump_tuple(const void *data, const vector<Attribute> &attrs) // {{{
{
    char *data_ptr = (char *) data;

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
} // }}}

void qe_dump_attribute(Attribute &a) // {{{
{
    cout << "name: " << a.name << " type: ";

    if(a.type == TypeInt)
        cout << "int   ";
    else if(a.type == TypeReal)
        cout << "float ";
    else if(a.type == TypeVarChar)
        cout << "char  ";

    cout << " len: " << a.length << endl;
} // }}}

void qe_dump_attributes(vector<Attribute> &attrs) // {{{
{
    cout << "[Begin Attributes]" << endl;

    for (unsigned int i=0; i < attrs.size(); i++)
    {
        cout << "  ";
        qe_dump_attribute(attrs[i]);
    }

    cout << "[End Attributes]" << endl;
} // }}}

void qe_get_attribute(string &name, vector<Attribute> &attrs, Attribute &a) // {{{
{
    for (unsigned int i = 0; i < attrs.size(); i++)
    {
        if(name == attrs[i].name)
        {
             a = attrs[i];
             return;
        }
    }
} // }}}

void qe_dump_condition(Condition &c, vector<Attribute> &attrs) // {{{
{
    Attribute lhs;
    qe_get_attribute(c.lhsAttr, attrs, lhs);

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
        qe_get_attribute(c.rhsAttr, attrs, rhs);

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
} // }}}

unsigned qe_get_tuple_size(const void *tuple, const vector<Attribute> &attrs) // {{{
{
    unsigned int offset;
    char *tuple_ptr = (char *) tuple;

    offset = 0;

    for (unsigned int i = 0; i < attrs.size(); i++)
    {
        if (attrs[i].type == TypeInt)
            offset += sizeof(int);
        else if (attrs[i].type == TypeReal)
            offset += sizeof(float);
        else if (attrs[i].type == TypeVarChar)
            offset += sizeof(unsigned) + (*(unsigned *) ((char *) tuple_ptr + offset));
    }

    return offset;
} // }}}

unsigned qe_get_tuple_size(const void *tuple, const AttrType &t) // {{{
{
    unsigned int offset;
    char *tuple_ptr = (char *) tuple;

    offset = 0;

    if (t == TypeInt)
        offset += sizeof(int);
    else if (t == TypeReal)
        offset += sizeof(float);
    else if (t == TypeVarChar)
        offset += sizeof(unsigned) + (*(unsigned *) ((char *) tuple_ptr + offset));

    return offset;
} // }}}

unsigned qe_get_tuple_size(const void *tuple, const Attribute &attr) // {{{
{
    return qe_get_tuple_size(tuple, attr.type);
} // }}}

void qe_get_tuple_element(const void *tuple, const vector<Attribute> &attrs, const string &name, void *value) // {{{
{
    char *tuple_ptr = (char *) tuple;

    for (unsigned int offset = 0, i = 0; i < attrs.size(); i++)
    {
        if (attrs[i].name == name)
        {
            /* copy in the value to value. */
            if (attrs[i].type == TypeInt)
                memcpy(value, tuple_ptr + offset, sizeof(int));
            else if (attrs[i].type == TypeReal)
                memcpy(value, tuple_ptr + offset, sizeof(float));
            else if (attrs[i].type == TypeVarChar)
                memcpy(value, tuple_ptr + offset, sizeof(unsigned) + (*(unsigned *) ((char *) tuple_ptr) + offset));
        
            /* we found the attribute, we're done. */
            return;
        }
        else
        {
            if (attrs[i].type == TypeInt)
                offset += sizeof(int);
            else if (attrs[i].type == TypeReal)
                offset += sizeof(float);
            else if (attrs[i].type == TypeVarChar)
                offset += sizeof(unsigned) + (*(unsigned *) ((char *) tuple_ptr + offset));
        }
    }
} // }}}

void qe_get_tuple_element(const void *tuple, const vector<Attribute> &attrs, const Attribute &attr, void *value) // {{{
{
   qe_get_tuple_element(tuple, attrs, attr.name, value);
} // }}}

void qe_get_tuple_element(const void *tuple, const AttrType &t, void *value) // {{{
{
    char *tuple_ptr = (char *) tuple;

    /* copy in the value to value. */
    if (t == TypeInt)
        memcpy(value, tuple_ptr, sizeof(int));
    else if (t == TypeReal)
        memcpy(value, tuple_ptr, sizeof(float));
    else if (t == TypeVarChar)
        memcpy(value, tuple_ptr, sizeof(unsigned) + (*(unsigned *) ((char *) tuple_ptr)));
} // }}}

void qe_get_tuple_element(const void *tuple, const Attribute &a, void *value) // {{{
{
    qe_get_tuple_element(tuple, a.type, value);
} // }}}


int qe_cmp_values(const CompOp &op, const void *lhs_value, const void *rhs_value, const AttrType &lhs_attr_type, const AttrType &rhs_attr_type) // {{{
{
        char *left_value = (char *) lhs_value;
        char *right_value = (char *) rhs_value;

        if (lhs_attr_type == TypeInt)
        {
            if (rhs_attr_type == TypeReal)
            {
                int lhs = *(int *) left_value;
                float rhs = *(float *) right_value;
     
                return (QE_VALUE_COMP_OP(op, lhs, rhs));
            }
            else if (rhs_attr_type == TypeInt)
            {
                int lhs = *(int *) left_value;
                int rhs = *(int *) right_value;
     
                return (QE_VALUE_COMP_OP(op, lhs, rhs));
            }
        }
        else if (lhs_attr_type == TypeReal)
        {
            if (rhs_attr_type == TypeReal)
            {
                float lhs = *(float *) left_value;
                float rhs = *(float *) right_value;
     
                return (QE_VALUE_COMP_OP(op, lhs, rhs));
            }
            else if (rhs_attr_type == TypeInt)
            {
                float lhs = *(float *) left_value;
                int rhs = *(int *) right_value;
     
                return (QE_VALUE_COMP_OP(op, lhs, rhs));
            }
        }
        else if (lhs_attr_type == TypeVarChar && rhs_attr_type == TypeVarChar)
        {
            string lhs(((char *) left_value) + sizeof(unsigned), (*(unsigned *) left_value));
            string rhs(((char *) right_value) + sizeof(unsigned), (*(unsigned *) right_value));
     
            return (QE_VALUE_COMP_OP(op, lhs, rhs));
        }

        return 0;
} // }}}
