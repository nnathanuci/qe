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
    unsigned char *filter_tuple_ptr = (unsigned char *) filter_tuple;
    bool found_lhs = false;
    bool found_rhs = false;

    RC rc;

    if (rc = iter->getNextTuple(filter_tuple))
        return rc;

    for (unsigned int i=0; i < attrs.size(); i++)
    {
         if (attrs[i].name == lhs_attr.name)
         {
              found_lhs = true;

              qe_get_tuple_element(filter_tuple, attrs, lhs_attr.name, lhs_value);

              /* if rhs is just a value, or we've found the value, then we're done with checking the filter_tuple. */
              if (!cond.bRhsIsAttr || found_rhs)
                  break;
         }
         else if (cond.bRhsIsAttr && attrs[i].name == rhs_attr.name)
         {
             found_rhs = true;

             qe_get_tuple_element(filter_tuple, attrs, rhs_attr.name, rhs_value);

             /* if we've found lhs value, then we're done. */
             if (found_lhs)
                  break;
         }

         /* advance tuple filter_tuple. */
         if (attrs[i].type == TypeInt)
             filter_tuple_ptr += sizeof(int);
         else if (attrs[i].type == TypeReal)
             filter_tuple_ptr += sizeof(float);
         else if (attrs[i].type == TypeVarChar)
             filter_tuple_ptr += sizeof(unsigned) + (*(unsigned *) filter_tuple_ptr);
    }

    if (qe_cmp_values(cond.op, lhs_value, rhs_value, lhs_attr.type, rhs_attr.type))
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
