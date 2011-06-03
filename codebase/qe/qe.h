#ifndef _qe_h_
#define _qe_h_

#include <vector>

#include "../pf/pf.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

# define QE_EOF (-1)  // end of the index scan

#define QE_VALUE_COMP_OP(op, lhs, rhs) \
  (  ((op) == EQ_OP && (lhs) == (rhs)) || \
     ((op) == LT_OP && ((lhs) < (rhs))) || \
     ((op) == GT_OP && ((lhs) > (rhs))) || \
     ((op) == LE_OP && ((lhs) <= (rhs))) || \
     ((op) == GE_OP && ((lhs) >= (rhs))) || \
     ((op) == NE_OP && ((lhs) != (rhs))) || \
     ((op) == NO_OP)  )

using namespace std;

typedef enum{ MIN = 0, MAX, SUM, AVG, COUNT } AggregateOp;

// The following functions use  the following 
// format for the passed data.
//    For int and real: use 4 bytes
//    For varchar: use 4 bytes for the length followed by
//                          the characters

struct Value {
    AttrType type;          // type of value               
    void     *data;         // value                       
};


struct Condition {
    string lhsAttr;         // left-hand side attribute                     
    CompOp  op;             // comparison operator                          
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string rhsAttr;         // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator { // {{{
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;

        /* uses getAttributes to first get the attr vector, and then finds by name. */
        RC getAttribute(string name, Attribute &a)
        {
            vector<Attribute> attrs;
            getAttributes(attrs);
            for (unsigned int i=0; i < attrs.size(); i++)
            {
                if(name == attrs[i].name)
                {
                    a = attrs[i];
                    return 0;
                }
            }

            return 1;
        }

        /* finds the attribute when supplied an attribute vector by name. */
        RC getAttribute(const vector<Attribute> &attrs, string name, Attribute &a)
        {
            for (unsigned int i=0; i < attrs.size(); i++)
            {
                if(name == attrs[i].name)
                {
                    a = attrs[i];
                    return 0;
                }
            }

            return 1;
        }



        virtual ~Iterator() {};
}; // }}}


class TableScan : public Iterator // {{{
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RM &rm;
        RM_ScanIterator *iter;
        string tablename;
        vector<Attribute> attrs;
        vector<string> attrNames;
        
        TableScan(RM &rm, const string tablename, const char *alias = NULL):rm(rm)
        {
            // Get Attributes from RM
            rm.getAttributes(tablename, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs[i].name);
            }
            // Call rm scan to get iterator
            iter = new RM_ScanIterator();
            rm.scan(tablename, attrNames, *iter);

            // Store tablename
            this->tablename = tablename;
            if(alias) this->tablename = alias;
        };
       
        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tablename, attrNames, *iter);
        };
       
        RC getNextTuple(void *data)
        {
            RID rid;
            return iter->getNextTuple(rid, data);
        };
        
        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;
            
            // For attribute in vector<Attribute>, name it as rel.attr (e.g. "emptable.empid")
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tablename;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };
        
        ~TableScan() 
        {
            iter->close();
        };
}; // }}}


class IndexScan : public Iterator // {{{
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RM &rm;
        IX_IndexScan *iter;
        IX_IndexHandle handle;
        string tablename;
        vector<Attribute> attrs;
        
        IndexScan(RM &rm, const IX_IndexHandle &indexHandle, const string tablename, const char *alias = NULL):rm(rm)
        {
            // Get Attributes from RM
            rm.getAttributes(tablename, attrs);
                     
            // Store tablename
            this->tablename = tablename;
            if(alias) this->tablename = string(alias);
            
            // Store Index Handle
            iter = NULL;
            this->handle = indexHandle;
        };
       
        // Start a new iterator given the new compOp and value
        void setIterator(CompOp compOp, void *value)
        {
            if(iter != NULL)
            {
                iter->CloseScan();
                delete iter;
            }
            iter = new IX_IndexScan();
            iter->OpenScan(handle, compOp, value);
        };
       
        RC getNextTuple(void *data)
        {
            RID rid;
            int rc = iter->GetNextEntry(rid);
            if(rc == 0)
            {
                rc = rm.readTuple(tablename.c_str(), rid, data);
            }
            return rc;
        };
        
        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr (e.g. "emptable.empid")
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tablename;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };
        
        ~IndexScan() 
        {
            iter->CloseScan();
        };
}; // }}}


class Filter : public Iterator { // {{{
    // Filter operator
    Iterator *iter;
    Condition cond;
    vector<Attribute> attrs;

    Attribute lhs_attr;
    Attribute rhs_attr;
    AttrType rhs_type;

    char rhs_value[PF_PAGE_SIZE];

    public:
        Filter(Iterator *input,                         // Iterator of input R
               const Condition &condition               // Selection condition 
        );
        ~Filter();
        
        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr (e.g. "emptable.empid")
        void getAttributes(vector<Attribute> &attrs) const;
}; // }}}


class Project : public Iterator { // {{{
    // Projection operator
    Iterator *iter;
    vector<string> attr_names;
    vector<Attribute> tuple_attrs;

    public:
        Project(Iterator *input,                            // Iterator of input R
                const vector<string> &attrNames);           // vector containing attribute names
        ~Project();
        
        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr (e.g. "emptable.empid")
        void getAttributes(vector<Attribute> &attrs) const;
}; // }}}


class NLJoin : public Iterator { // {{{
    Iterator *left_iter;
    TableScan *right_iter;
    Condition cond;
    unsigned n_buffer_pages;
    vector<Attribute> left_attrs;
    vector<Attribute> right_attrs;
    vector<Attribute> join_attrs;
    bool next_left_tuple_ready;

    Attribute lhs_attr;
    Attribute rhs_attr;

    char left_tuple[PF_PAGE_SIZE];
    char lhs_value[PF_PAGE_SIZE];
    unsigned int left_size;

    // Nested-Loop join operator
    public:
        NLJoin(Iterator *leftIn,                             // Iterator of input R
               TableScan *rightIn,                           // TableScan Iterator of input S
               const Condition &condition,                   // Join condition
               const unsigned numPages                       // Number of pages can be used to do join (decided by the optimizer)
        );
        ~NLJoin();
        
        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr (e.g. "emptable.empid")
        void getAttributes(vector<Attribute> &attrs) const;
}; // }}}


class INLJoin : public Iterator { // {{{
    // Index Nested-Loop join operator
    Iterator *left_iter;
    IndexScan *right_iter;
    Condition cond;
    unsigned n_buffer_pages;
    vector<Attribute> left_attrs;
    vector<Attribute> right_attrs;
    vector<Attribute> join_attrs;
    bool next_left_tuple_ready;

    Attribute lhs_attr;
    Attribute rhs_attr;

    char left_tuple[PF_PAGE_SIZE];
    char lhs_value[PF_PAGE_SIZE];
    unsigned int left_size;


    public:
        INLJoin(Iterator *leftIn,                               // Iterator of input R
                IndexScan *rightIn,                             // IndexScan Iterator of input S
                const Condition &condition,                     // Join condition
                const unsigned numPages                         // Number of pages can be used to do join (decided by the optimizer)
        );
        
        ~INLJoin();

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr (e.g. "emptable.empid")
        void getAttributes(vector<Attribute> &attrs) const;
}; // }}}

    typedef struct qe_hash_join_key // {{{
    {
        AttrType type;
        float float_v;
        int int_v;
        unsigned int s_len;
        string s;
    
        qe_hash_join_key()
        {
            type = TypeInt;
            float_v = 0;
            int_v = 0;
            s_len = 0;
            s = "";
        }

        /* need to specify a weak comparison operator. */
        bool operator<(const qe_hash_join_key &r) const
        {
            if (type == TypeInt && r.type == TypeInt)
                 return (int_v < r.int_v);
            else if (type == TypeInt && r.type == TypeReal)
                 return (int_v < r.float_v);
            if (type == TypeReal && r.type == TypeInt)
                 return (float_v < r.int_v);
            else if (type == TypeReal && r.type == TypeReal)
                 return (float_v < r.float_v);
            else if (type == TypeReal && r.type == TypeReal)
                 return (s < r.s);
    
            /* any other combination is invalid. */
            return 0;
        }
    } qe_hash_join_key; // }}}

class HashJoin : public Iterator { // {{{
    // Index Nested-Loop join operator
    Iterator *left_iter;
    Iterator *right_iter;
    Condition cond;
    unsigned n_buffer_pages;
    vector<Attribute> left_attrs;
    vector<Attribute> right_attrs;
    vector<Attribute> join_attrs;
    map<qe_hash_join_key, vector<char *> > hash;

    Attribute lhs_attr;
    Attribute rhs_attr;

    bool left_values_read;

    char right_tuple[PF_PAGE_SIZE];
    unsigned int right_size;
    unsigned int next_index;
    vector<char *> *values;

    // Hash join operator
    public:
        HashJoin(Iterator *leftIn,                                // Iterator of input R
                 Iterator *rightIn,                               // Iterator of input S
                 const Condition &condition,                      // Join condition
                 const unsigned numPages                          // Number of pages can be used to do join (decided by the optimizer)
        );
        
        ~HashJoin();

        RC hashLeftTable();
        void clearHash();

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr (e.g. "emptable.empid")
        void getAttributes(vector<Attribute> &attrs) const;
}; // }}}

class Aggregate : public Iterator { // {{{
    // Aggregation operator
    public:
        Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  AggregateOp op                                // Aggregate operation
        );

        // Extra Credit
        Aggregate(Iterator *input,                              // Iterator of input R
                  Attribute aggAttr,                            // The attribute over which we are computing an aggregate
                  Attribute gAttr,                              // The attribute over which we are grouping the tuples
                  AggregateOp op                                // Aggregate operation
        );     

        ~Aggregate();
        
        RC getNextTuple(void *data) {return QE_EOF;};
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr) (e.g. "MAX(emptable.empid)"))"
        void getAttributes(vector<Attribute> &attrs) const;
}; // }}}

// debug functions
void qe_get_attribute(const string &name, const vector<Attribute> &attrs, Attribute &a);
void qe_dump_condition(Condition &c, vector<Attribute> &attrs);
void qe_dump_attribute(Attribute &a);
void qe_dump_attributes(vector<Attribute> &attrs);
void qe_dump_tuple(const void *data, const vector<Attribute> &attrs);
unsigned qe_get_tuple_size(const void *tuple, const vector<Attribute> &attrs);
void qe_get_tuple_element(const void *tuple, const vector<Attribute> &attrs, const string &name, void *value);
int qe_cmp_values(const CompOp &op, const void *lhs_value, const void *rhs_value, const AttrType &lhs_attr_type, const AttrType &rhs_attr_type);
unsigned qe_get_tuple_size(const void *tuple, const Attribute &attr);
unsigned qe_get_tuple_size(const void *tuple, const AttrType &t);
void qe_get_tuple_element(const void *tuple, const Attribute &a, void *value);
void qe_get_tuple_element(const void *tuple, const AttrType &t, void *value);
void qe_get_tuple_element(const void *tuple, const vector<Attribute> &attrs, const Attribute &attr, void *value);
void qe_dump_tuple_element(const void *data, const Attribute &a);

#endif
