#include "qe.h"
#include <cfloat>

static string qe_agg_op_to_str(AggregateOp op) // {{{
{
    if (op == MIN)
        return string("MIN");
    else if (op == MAX)
        return string("MAX");
    else if (op == SUM)
        return string("SUM");
    else if (op == AVG)
        return string("AVG");
    else if (op == COUNT)
        return string("COUNT");

    return string("");
} // }}}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) // {{{
{
    iter = input;
    agg_attr = aggAttr;
    agg_op = op;
    is_group_agg = false;
    is_finished = false;
    iter->getAttributes(tuple_attrs);

    if (op == MIN)
        value = FLT_MAX;
    else if (op == MAX)
        value = FLT_MIN;
    else
        value = 0;
} // }}}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute gAttr, AggregateOp op) // {{{
{
    iter = input;
    agg_attr = aggAttr;
    group_attr = gAttr;
    agg_op = op;
    is_group_agg = true;
    is_finished = false;
    iter->getAttributes(tuple_attrs);

    if (op == MIN)
        value = FLT_MAX;
    else if (op == MAX)
        value = FLT_MIN;
    else
        value = 0;
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
void Aggregate::getAttributes(vector<Attribute> &attrs) const // {{{
{
    Attribute a;

    attrs.clear();

    /* the aggregate value is always a TypeReal. */
    a.type = TypeReal;
    a.name = qe_agg_op_to_str(agg_op)+"("+agg_attr.name+")";
    attrs.push_back(a);
} // }}}

Aggregate::~Aggregate() // {{{
{
}
// }}}

RC Aggregate::getNextTupleAggOp(void *data) // {{{
{
    char tuple[PF_PAGE_SIZE];
    float count = 0;

    RC rc;

    if (is_finished)
        return QE_EOF;

    qe_dump_attribute(agg_attr);
    qe_dump_attributes(tuple_attrs);
    while (!(rc = iter->getNextTuple(tuple)))
    {
        /* enough to store int/float. */
        char tuple_val[8];
        float v;

        /* keep a running counter to perform average. */
        count += 1;

        qe_get_tuple_element(tuple, tuple_attrs, agg_attr, tuple_val);
        v = (agg_attr.type == TypeReal) ? (*(float *) tuple_val) : (*(int *) tuple_val);
        qe_dump_tuple(tuple, tuple_attrs);

        if (agg_op == MIN)
             value = (v <= value) ? v : value;
        else if (agg_op == MAX)
             value = (v >= value) ? v : value;
        else if (agg_op == SUM || agg_op == AVG)
             value += v;
        else if (agg_op == COUNT)
             value += 1;
    }

    *(float *) data = (agg_op == AVG) ? value / count : value;
    is_finished = true;

    return 0;
}; // }}}
