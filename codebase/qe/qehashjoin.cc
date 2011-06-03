#include "qe.h"
#include <map>
#include <vector>
#include <cstdlib>


void HashJoin::clearHash() // {{{
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

RC HashJoin::hashLeftTable() // {{{
{
    RC rc;
    unsigned char left_tuple[PF_PAGE_SIZE];

    while (!(rc = left_iter->getNextTuple(left_tuple)))
    {
        qe_hash_join_key k;
        char lhs_value[PF_PAGE_SIZE];

        /* get the value associated with the join attribute. */
        qe_get_tuple_element(left_tuple, left_attrs, lhs_attr.name, lhs_value);

        k.type = lhs_attr.type;

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

    return rc;
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

    /* hash the left table. */
    hashLeftTable();

    left_values_read = false;

    // qe_dump_condition(cond, join_attrs); /* XXX: debug */
} // }}}

RC HashJoin::getNextTuple(void *join_data) // {{{
{
    RC rc;

    /* don't have any tuples to join on yet, pull a tuple from the right, and hash it to get the left tuples. */
    if(!left_values_read) // {{{
    {
        char rhs_value[PF_PAGE_SIZE];
        qe_hash_join_key k;

        /* get the next tuple from the right relation. */
        rc = right_iter->getNextTuple(right_tuple);

        /* return if either error or EOF. */
        if(rc)
            return rc;

        //qe_dump_tuple(right_tuple, right_attrs); // XXX: debug
    
        /* calculate size of right tuple. */
        right_size = qe_get_tuple_size(right_tuple, right_attrs);

        /* get the value associated with the join attribute. */
        qe_get_tuple_element(right_tuple, right_attrs, rhs_attr.name, rhs_value);

        k.type = rhs_attr.type;

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

        /* no vector associated with this key, move on to the next tuple. */
        if (!hash.count(k))
            getNextTuple(join_data);

        /* we found something to join on, let's carry on. */
        values = &(hash[k]);
        left_values_read = true;

        /* iniitalize vector index iterator. */
        next_index = 0;
    } // }}}

    if (left_values_read)
    {
         /* no more values to read, we need to get another right tuple. */
         if (next_index >= values->size())
         {
             left_values_read = false;
             values = NULL;
             return getNextTuple(join_data);
         }

         /* join against the next tuple in the sequence, and return the joined tuples. */
         char *next_left_tuple = (*values)[next_index];
         unsigned int left_size = qe_get_tuple_size(next_left_tuple, left_attrs);
         memcpy((char *) join_data, next_left_tuple, left_size);
         memcpy((char *) join_data + left_size, right_tuple, right_size);

         /* increment since we're done with this value. */
         next_index++;

         /* return the tuple. */
         return 0;
    }

    /* if we come here, then we found nothing to join on. */
    return getNextTuple(join_data);
} // }}}

HashJoin::~HashJoin() // {{{
{
    clearHash();
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
