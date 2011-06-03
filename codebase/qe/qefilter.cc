#include "qe.h"

Filter::Filter(Iterator *input, const Condition &condition) // {{{
{
    iter = input;
    cond = condition;
    attrs.clear();

    /* need the full set of attributes to do selection. */
    getAttributes(attrs);

    /* get the attribute by name. */
    getAttribute(attrs, condition.lhsAttr, lhs_attr);

    if (condition.bRhsIsAttr)
    {
        getAttribute(attrs, condition.rhsAttr, rhs_attr);
        rhs_type = rhs_attr.type;
    }
    else
    {
        rhs_type = condition.rhsValue.type;

        /* make copy of RHS value for comparison. */
        if (condition.rhsValue.type == TypeInt)
            memcpy(rhs_value, condition.rhsValue.data, sizeof(int));
        else if (condition.rhsValue.type == TypeReal)
            memcpy(rhs_value, condition.rhsValue.data, sizeof(float));
        else if (condition.rhsValue.type == TypeVarChar)
            memcpy(rhs_value, condition.rhsValue.data, sizeof(unsigned) + (*(unsigned *) condition.rhsValue.data));
    }
} // }}}

RC Filter::getNextTuple(void *filter_tuple) // {{{
{
    unsigned char lhs_value[PF_PAGE_SIZE];
    /* rhs_value is a field, since it may already be specified as a value, see constructor. */

    RC rc;

    if ((rc = iter->getNextTuple(filter_tuple)))
        return rc;

    qe_get_tuple_element(filter_tuple, attrs, lhs_attr.name, lhs_value);

    if (cond.bRhsIsAttr)
        qe_get_tuple_element(filter_tuple, attrs, rhs_attr.name, rhs_value);

    if (qe_cmp_values(cond.op, lhs_value, rhs_value, lhs_attr.type, rhs_type))
        return 0;
    
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
