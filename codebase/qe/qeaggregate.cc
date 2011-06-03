#include "qe.h"

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) // {{{
{
    iter = input;
    agg_attr = aggAttr;
    agg_op = op;
    is_group_agg = false;
} // }}}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute gAttr, AggregateOp op) // {{{
{
    iter = input;
    agg_attr = aggAttr;
    group_attr = gAttr;
    agg_op = op;
    is_group_agg = true;
} // }}}

RC Aggregate::getNextTuple(void *data) // {{{
{
    if (is_group_agg)
        return getNextTupleGroup(data);

    return getNextTupleAggOp(data);
}; // }}}

RC Aggregate::getNextTupleGroup(void *data) // {{{
{
    return QE_EOF;
}; // }}}

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const // {{{k
{
} // }}}

RC Aggregate::getNextTupleAggOp(void *data) // {{{
{
    return QE_EOF;
}; // }}}
