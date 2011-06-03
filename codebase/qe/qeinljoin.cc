#include "qe.h"

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition, const unsigned numPages) // {{{
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

    /* signal to pick up the next tuple from lhs relation. */
    next_left_tuple_ready = true;

    // qe_dump_condition(cond, join_attrs); /* XXX: debug */

} // }}}

RC INLJoin::getNextTuple(void *join_data) // {{{
{
    unsigned char right_tuple[PF_PAGE_SIZE];

    RC rc;

    /* left tuple which we use to scan the right relation. */
    if (next_left_tuple_ready)
    {
        if ((rc = left_iter->getNextTuple(left_tuple)))
            return rc;

        /* get the size of the tuple (used for joining). */
        left_size = qe_get_tuple_size(left_tuple, left_attrs);

        /* get the value associated with the join attribute. */
        qe_get_tuple_element(left_tuple, left_attrs, lhs_attr.name, lhs_value);

        /* setup an index on the right relation. */
        right_iter->setIterator(cond.op, lhs_value);

        /* unflag signal, wait until we've finished reading in the whole of the rhs relation, before this is flagged again. */
        next_left_tuple_ready = false;
    }


    while (!(rc = right_iter->getNextTuple(right_tuple)))
    {
        unsigned char rhs_value[PF_PAGE_SIZE];

        /* get the value associated with the join attribute. */
        qe_get_tuple_element(right_tuple, right_attrs, rhs_attr.name, rhs_value);

        if (qe_cmp_values(cond.op, lhs_value, rhs_value, lhs_attr.type, rhs_attr.type))
        {
             /* join the tuple and return. */
             unsigned int right_size = qe_get_tuple_size(right_tuple, right_attrs);
             memcpy((char *) join_data, left_tuple, left_size);
             memcpy((char *) join_data + left_size, right_tuple, right_size);

             return 0;
        }
    }

    if (rc == QE_EOF)
    {
        /* we're done scanning rhs relation, signal for a left tuple, and start over. */
        next_left_tuple_ready = true;
        return getNextTuple(join_data);
    }
    else
    {
        return rc;
    }

    return QE_EOF;
} // }}}

INLJoin::~INLJoin() // {{{
{
} // }}}

void INLJoin::getAttributes(vector<Attribute> &join_attrs) const // {{{
{
    vector<Attribute> right_attrs;

    left_iter->getAttributes(join_attrs);
    right_iter->getAttributes(right_attrs);

    for (unsigned int i = 0; i < right_attrs.size(); i++)
        join_attrs.push_back(right_attrs[i]);
        
    return;
} // }}}
