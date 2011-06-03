#include "qe.h"
#include <map>
#include <vector>
#include <cstdlib>

typedef struct qe_hash_join_key
{
    AttrType type;
    float float_v;
    int int_v;
    unsigned int s_len;
    string s;

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
} qe_hash_join_key;

void qe_hash_join_clear_map(map<qe_hash_join_key, vector<char *> > &hash) // {{{
{
    /* make sure the map is clear, by deleting all the entries in the vector. */
    map<qe_hash_join_key, vector<char *> >::const_iterator iter;

    for(iter = hash.begin(); iter != hash.end(); iter++)
    {
        for (unsigned int i = 0; i < ((*iter).second).size(); i++)
            free(((*iter).second)[i]);
    }

    hash.clear();

} // }}}

HashJoin::HashJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned numPages) // {{{
{
    left_iter = leftIn;
    right_iter = rightIn;
    cond = condition;
    n_buffer_pages = numPages;

    left_iter->getAttributes(left_attrs);
    right_iter->getAttributes(right_attrs);
    getAttributes(join_attrs);

    /* get the attribute by name. */
    getAttribute(left_attrs, condition.lhsAttr, lhs_attr);

    if (condition.bRhsIsAttr)
        getAttribute(right_attrs, condition.rhsAttr, rhs_attr);

    /* signal to read in the left table. */
    left_table_hashed = false;

    // qe_dump_condition(cond, join_attrs); /* XXX: debug */
} // }}}

RC HashJoin::getNextTuple(void *join_data) // {{{
{
    static map<qe_hash_join_key, vector<char *> > hash;
    unsigned char right_tuple[PF_PAGE_SIZE];
    static unsigned int right_vector_index;

    RC rc;

    /* load the left relation into a hash table/memory. */
    if(!left_table_hashed)
    {
         unsigned char left_tuple[PF_PAGE_SIZE];

         /* clear the map (incase of remnant data). */
         qe_hash_join_clear_map(hash);

         while (!(rc = left_iter->getNextTuple(left_tuple)))
         {
             qe_hash_join_key k;
             char lhs_value[PF_PAGE_SIZE];

             /* get the join value and store it in lhs_value. */
             qe_get_tuple_element(left_tuple, lhs_attr, lhs_value);

             /* copy in key value to hash key. */
             if (lhs_attr.type == TypeInt)
             {
                 k.int_v = *(int *) lhs_value;
             }
             else if (lhs_attr.type ==TypeReal)
             {
                 k.float_v = *(float *) lhs_value;
             }
             else if (lhs_attr.type ==TypeVarChar)
             {
                 unsigned int lhs_value_size = qe_get_tuple_size(lhs_value, lhs_attr);
                 k.s_len = lhs_value_size;
                 k.s = string(lhs_value + sizeof(k.s_len), k.s_len);
             }

             /* key already exists in map (a duplicate value). */
             {
                 unsigned int left_tuple_size = qe_get_tuple_size(left_tuple, left_attrs);
                 char *p;

                 if (!(p = (char *) malloc(left_tuple_size)))
                     return 1;

                 memcpy(p, left_tuple, left_tuple_size);

                 /* instantiate a vector if necessary (otherwise this will do nothing). */
                 hash[k];
                 hash[k].push_back(p);
             }
         }

         while (!(rc = right_iter->getNextTuple(right_tuple)))
         {
             char rhs_value[PF_PAGE_SIZE];
             qe_hash_join_key k;

             /* get the join value and store it in rhs_value. */
             qe_get_tuple_element(right_tuple, rhs_attr, rhs_value);

             /* copy in key value to hash key. */
             if (rhs_attr.type == TypeInt)
             {
                 k.int_v = *(int *) rhs_value;
             }
             else if (rhs_attr.type ==TypeReal)
             {
                 k.float_v = *(float *) rhs_value;
             }
             else if (rhs_attr.type ==TypeVarChar)
             {
                 unsigned int rhs_value_size = qe_get_tuple_size(rhs_value, rhs_attr);
                 k.s_len = rhs_value_size;
                 k.s = string(rhs_value + sizeof(k.s_len), k.s_len);
             }

         }

         /* if not EOF, then we have an error .*/
         if (rc != QE_EOF)
             return rc;

         left_table_hashed = true;
    }
    

    /* we're done scanning rhs relation, and therefore finished with the join. */
    qe_hash_join_clear_map(hash);

    return rc;
} // }}}

HashJoin::~HashJoin() // {{{
{
} // }}}

void HashJoin::getAttributes(vector<Attribute> &join_attrs) const // {{{
{
    vector<Attribute> right_attrs;

    left_iter->getAttributes(join_attrs);
    right_iter->getAttributes(right_attrs);

    for (unsigned int i = 0; i < right_attrs.size(); i++)
        join_attrs.push_back(right_attrs[i]);
        
    return;
} // }}}
