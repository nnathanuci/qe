#include "qe.h"

Filter::Filter(Iterator *input, const Condition &condition) // {{{
{
    iter = input;
    cond = condition;
    tuple_attrs.clear();

    /* need the full set of attributes to do selection. */
    iter->getAttributes(tuple_attrs);

    /* get the attribute by name. */
    getAttribute(tuple_attrs, condition.lhsAttr, lhs_attr);

    if (condition.bRhsIsAttr)
    {
        getAttribute(tuple_attrs, condition.rhsAttr, rhs_attr);
        rhs_type = rhs_attr.type;
    }
    else
    {
        rhs_type = condition.rhsValue.type;

        /* make copy of RHS value for comparison. */
        qe_get_tuple_element(condition.rhsValue.data, condition.rhsValue.type, rhs_value);
    }
} // }}}

RC Filter::getNextTuple(void *filter_tuple) // {{{
{
    char tuple[PF_PAGE_SIZE];
    char lhs_value[PF_PAGE_SIZE];
    /* rhs_value is a field, since it may already be specified as a value, see constructor. */

    RC rc;

    if ((rc = iter->getNextTuple(tuple)))
        return rc;

    qe_get_tuple_element(tuple, tuple_attrs, lhs_attr.name, lhs_value);

    if (cond.bRhsIsAttr)
        qe_get_tuple_element(tuple, tuple_attrs, rhs_attr.name, rhs_value);

    if (qe_cmp_values(cond.op, lhs_value, rhs_value, lhs_attr.type, rhs_type))
    {
        unsigned int tuple_size = qe_get_tuple_size(tuple, tuple_attrs);
        memcpy(filter_tuple, tuple, tuple_size);
        return 0;
    }
    
    /* didnt find a match, continue searching. */
    return getNextTuple(filter_tuple);
} // }}}

Filter::~Filter() // {{{
{
} // }}}

void Filter::getAttributes(vector<Attribute> &attrs) const // {{{
{
    iter->getAttributes(attrs);

    return;
} // }}}
