#include "qe.h"

#define QENLJOIN_VALUE_COMP_OP(op, lhs, rhs) \
  (  ((op) == EQ_OP && (lhs) == (rhs)) || \
     ((op) == LT_OP && ((lhs) < (rhs))) || \
     ((op) == GT_OP && ((lhs) > (rhs))) || \
     ((op) == LE_OP && ((lhs) <= (rhs))) || \
     ((op) == GE_OP && ((lhs) >= (rhs))) || \
     ((op) == NE_OP && ((lhs) != (rhs))) || \
     ((op) == NO_OP)  )

NLJoin::NLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) // {{{
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

RC NLJoin::getNextTuple(void *join_data) // {{{
{
    /* static because we will reuse the value on subsequent calls. */
    static unsigned char left_tuple[PF_PAGE_SIZE];
    static unsigned char left_value[PF_PAGE_SIZE];
    unsigned char right_tuple[PF_PAGE_SIZE];

    RC rc;

    /* anchor tuple which we use to scan the right relation. */ // {{{
    if (next_left_tuple_ready)
    {
        if ((rc = left_iter->getNextTuple(left_tuple)))
            return rc;

        /* find the anchor attribute and copy the value from left_tuple into left_value. */
        for (unsigned int offset = 0, i = 0; i < left_attrs.size(); i++)
        {
            if (left_attrs[i].name == cond.lhsAttr)
            {
                /* copy in the value to left_value. */
                if (left_attrs[i].type == TypeInt)
                    memcpy(left_value, left_tuple + offset, sizeof(int));
                else if (left_attrs[i].type == TypeReal)
                    memcpy(left_value, left_tuple + offset, sizeof(float));
                else if (left_attrs[i].type == TypeVarChar)
                    memcpy(left_value, left_tuple + offset, sizeof(unsigned) + (*(unsigned *) ((char *) left_tuple) + offset));

                /* we found the attribute, we're done. */
                break;
            }
            else
            {
                if (left_attrs[i].type == TypeInt)
                    offset += sizeof(int);
                else if (left_attrs[i].type == TypeReal)
                    offset += sizeof(float);
                else if (left_attrs[i].type == TypeVarChar)
                    offset += sizeof(unsigned) + (*(unsigned *) ((char *) left_tuple + offset));
            }
        }

        /* reset the right table scan. */
        right_iter->setIterator();

        /* unflag signal, wait until we've finished reading in the whole of the rhs relation, before this is flagged again. */
        next_left_tuple_ready = false;

        //cout << "anchor: "; qe_dump_tuple(left_tuple, left_attrs); /* XXX: debug */
    } // }}}

    while (!(rc = right_iter->getNextTuple(right_tuple)))
    {
        unsigned char rhs_value[PF_PAGE_SIZE];
        //cout << "right: "; qe_dump_tuple(right_tuple, right_attrs); /* XXX: debug */

        /* find the rhs_value from the right_tuple. */ // {{{
        for (unsigned int offset = 0, i = 0; i < right_attrs.size(); i++)
        {
            if (right_attrs[i].name == cond.rhsAttr)
            {
                /* copy in the value to rhs_value. */
                if (right_attrs[i].type == TypeInt)
                    memcpy(rhs_value, right_tuple + offset, sizeof(int));
                else if (right_attrs[i].type == TypeReal)
                    memcpy(rhs_value, right_tuple + offset, sizeof(float));
                else if (right_attrs[i].type == TypeVarChar)
                    memcpy(rhs_value, right_tuple + offset, sizeof(unsigned) + (*(unsigned *) ((char *) right_tuple) + offset));

                /* we found the attribute, we're done. */
                break;
            }
            else
            {
                if (right_attrs[i].type == TypeInt)
                    offset += sizeof(int);
                else if (right_attrs[i].type == TypeReal)
                    offset += sizeof(float);
                else if (right_attrs[i].type == TypeVarChar)
                    offset += sizeof(unsigned) + (*(unsigned *) ((char *) right_tuple + offset));
            }
        } // }}}

        /* create the join tuple. */ // {{{
        {
             unsigned int left_size = 0;
             unsigned int right_size = 0;

             for (unsigned int i = 0; i < left_attrs.size(); i++)
             {
                     /* copy in the value to left_value. */
                     if (left_attrs[i].type == TypeInt)
                         left_size += sizeof(int);
                     else if (left_attrs[i].type == TypeReal)
                         left_size += sizeof(float);
                     else if (left_attrs[i].type == TypeVarChar)
                         left_size += sizeof(unsigned) + (*(unsigned *) ((char *) left_tuple) + left_size);
             }

             for (unsigned int i = 0; i < right_attrs.size(); i++)
             {
                     /* copy in the value to left_value. */
                     if (right_attrs[i].type == TypeInt)
                         right_size += sizeof(int);
                     else if (right_attrs[i].type == TypeReal)
                         right_size += sizeof(float);
                     else if (right_attrs[i].type == TypeVarChar)
                         right_size += sizeof(unsigned) + (*(unsigned *) ((char *) left_tuple) + right_size);
             }

             memcpy((char *) join_data, left_tuple, left_size);
             memcpy((char *) join_data + left_size, right_tuple, right_size);
        }
        // }}}
        
        /* compare values, we don't care if we're comparing float with ints, etc. */ // {{{
        if (lhs_attr.type == TypeInt)
        {
            if (rhs_attr.type == TypeReal)
            {
                int lhs = *(int *) left_value;
                float rhs = *(float *) rhs_value;
     
                if (QENLJOIN_VALUE_COMP_OP(cond.op, lhs, rhs))
                    return 0;
            }
            else if (rhs_attr.type == TypeInt)
            {
                int lhs = *(int *) left_value;
                int rhs = *(int *) rhs_value;
     
                if (QENLJOIN_VALUE_COMP_OP(cond.op, lhs, rhs))
                    return 0;
            }
        }
        else if (lhs_attr.type == TypeReal)
        {
            if (rhs_attr.type == TypeReal)
            {
                float lhs = *(float *) left_value;
                float rhs = *(float *) rhs_value;
     
                if (QENLJOIN_VALUE_COMP_OP(cond.op, lhs, rhs))
                    return 0;
            }
            else if (rhs_attr.type == TypeInt)
            {
                float lhs = *(float *) left_value;
                int rhs = *(int *) rhs_value;
     
                if (QENLJOIN_VALUE_COMP_OP(cond.op, lhs, rhs))
                    return 0;
            }
        }
        else if (lhs_attr.type == TypeVarChar && rhs_attr.type == TypeVarChar)
        {
            string lhs(((char *) left_value) + sizeof(unsigned), (*(unsigned *) left_value));
            string rhs(((char *) rhs_value) + sizeof(unsigned), (*(unsigned *) rhs_value));
     
            if (QENLJOIN_VALUE_COMP_OP(cond.op, lhs, rhs))
                return 0;
        } // }}}
    }

    if (rc == QE_EOF)
    {
        /* we're done scanning rhs relation, signal a new anchor, and start over. */
        next_left_tuple_ready = true;
        return getNextTuple(join_data);
    }
    else
    {
        return rc;
    }

    return QE_EOF;
} // }}}

NLJoin::~NLJoin() // {{{
{
} // }}}

void NLJoin::getAttributes(vector<Attribute> &join_attrs) const // {{{
{
    vector<Attribute> right_attrs;

    left_iter->getAttributes(join_attrs);
    right_iter->getAttributes(right_attrs);

    for (unsigned int i = 0; i < right_attrs.size(); i++)
        join_attrs.push_back(right_attrs[i]);
        
    return;
} // }}}
