#include "qe.h"

//class Filter : public Iterator {
//    // Filter operator
//    Iterator *iter;
//    Condition condition;
//    vector<Attribute> attrs;
//
//    public:
//        Filter(Iterator *input,                         // Iterator of input R
//               const Condition &condition               // Selection condition 
//        );
//        ~Filter();
//        
//        RC getNextTuple(void *data);
//        // For attribute in vector<Attribute>, name it as rel.attr (e.g. "emptable.empid")
//        void getAttributes(vector<Attribute> &attrs) const;
//};

#define QEFILTER_VALUE_COMP_OP(op, lhs, rhs) \
  (  ((op) == EQ_OP && (lhs) == (rhs)) || \
     ((op) == LT_OP && ((lhs) < (rhs))) || \
     ((op) == GT_OP && ((lhs) > (rhs))) || \
     ((op) == LE_OP && ((lhs) <= (rhs))) || \
     ((op) == GE_OP && ((lhs) >= (rhs))) || \
     ((op) == NE_OP && ((lhs) != (rhs))) || \
     ((op) == NO_OP)  )

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

RC Filter::getNextTuple(void *data) // {{{
{
    unsigned char lhs_value[PF_PAGE_SIZE];
    unsigned char *data_ptr = (unsigned char *) data;
    bool found_lhs = false;
    bool found_rhs = false;

    if (iter->getNextTuple(data) == QE_EOF)
        return QE_EOF;

    for (unsigned int i=0; i < attrs.size(); i++)
    {
         if (attrs[i].name == lhs_attr.name)
         {
              found_lhs = true;

              /* make copy of lhs value for comparison. */
              if (attrs[i].type == TypeInt)
                   memcpy(lhs_value, data_ptr, sizeof(int));
              else if (attrs[i].type == TypeReal)
                   memcpy(lhs_value, data_ptr, sizeof(float));
              else if (attrs[i].type == TypeVarChar)
                   memcpy(lhs_value, data_ptr, sizeof(unsigned) + (*(unsigned *) data_ptr));

              /* if rhs is just a value, or we've found the value, then we're done with checking the data. */
              if (!cond.bRhsIsAttr || found_rhs)
                  break;
         }
         else if (cond.bRhsIsAttr && attrs[i].name == rhs_attr.name)
         {
             found_rhs = true;

              /* make copy of rhs value for comparison. */
              if (attrs[i].type == TypeInt)
                   memcpy(rhs_value, data_ptr, sizeof(int));
              else if (attrs[i].type == TypeReal)
                   memcpy(rhs_value, data_ptr, sizeof(float));
              else if (attrs[i].type == TypeVarChar)
                   memcpy(rhs_value, data_ptr, sizeof(unsigned) + (*(unsigned *) data_ptr));

             /* if we've found lhs value, then we're done. */
             if (found_lhs)
                  break;
         }

         /* advance tuple data. */
         if (attrs[i].type == TypeInt)
             data_ptr += sizeof(int);
         else if (attrs[i].type == TypeReal)
             data_ptr += sizeof(float);
         else if (attrs[i].type == TypeVarChar)
             data_ptr += sizeof(unsigned) + (*(unsigned *) data_ptr);
    }

    /* compare values, we don't care if we're comparing float with ints, etc. */
    if (lhs_attr.type == TypeInt)
    {
        if (rhs_type == TypeReal)
        {
            int lhs = *(int *) lhs_value;
            float rhs = *(float *) rhs_value;

            if (QEFILTER_VALUE_COMP_OP(cond.op, lhs, rhs))
                return 0;
        }
        else if (rhs_type == TypeInt)
        {
            int lhs = *(int *) lhs_value;
            int rhs = *(int *) rhs_value;

            if (QEFILTER_VALUE_COMP_OP(cond.op, lhs, rhs))
                return 0;
        }
    }
    else if (lhs_attr.type == TypeReal)
    {
        if (rhs_type == TypeReal)
        {
            float lhs = *(float *) lhs_value;
            float rhs = *(float *) rhs_value;

            if (QEFILTER_VALUE_COMP_OP(cond.op, lhs, rhs))
                return 0;
        }
        else if (rhs_type == TypeInt)
        {
            float lhs = *(float *) lhs_value;
            int rhs = *(int *) rhs_value;

            if (QEFILTER_VALUE_COMP_OP(cond.op, lhs, rhs))
                return 0;
        }
    }
    else if (lhs_attr.type == TypeVarChar && rhs_attr.type == TypeVarChar)
    {
        string lhs(((char *) lhs_value) + sizeof(unsigned), (*(unsigned *) lhs_value));
        string rhs(((char *) rhs_value) + sizeof(unsigned), (*(unsigned *) rhs_value));

        if (QEFILTER_VALUE_COMP_OP(cond.op, lhs, rhs))
            return 0;
    }
    
    /* didnt find a match, continue searching. */
    return getNextTuple(data);
} // }}}

Filter::~Filter() // {{{
{
} // }}}

void Filter::getAttributes(vector<Attribute> &attrs) const // {{{
{
    iter->getAttributes(attrs);

    return;
} // }}}
