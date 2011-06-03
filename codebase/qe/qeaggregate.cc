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
    hash.clear();

    /* construct aggregate values. */
    hashGroupValues();

    hash_iter = hash.begin();
} // }}}

RC Aggregate::getNextTupleAggOp(void *data) // {{{
{
    char tuple[PF_PAGE_SIZE];
    float count = 0;

    RC rc;

    if (is_finished)
        return QE_EOF;

    while (!(rc = iter->getNextTuple(tuple)))
    {
        /* enough to store int/float. */
        char tuple_val[8];
        float v;

        /* keep a running counter to perform average. */
        count += 1;

        qe_get_tuple_element(tuple, tuple_attrs, agg_attr, tuple_val);
        v = (agg_attr.type == TypeReal) ? (*(float *) tuple_val) : (*(int *) tuple_val);

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

RC Aggregate::getNextTuple(void *data) // {{{
{
    if (is_group_agg)
        return getNextTupleGroup(data);

    return getNextTupleAggOp(data);
}; // }}}


// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const // {{{
{
    Attribute a;

    attrs.clear();

    if (is_group_agg)
        attrs.push_back(group_attr);

    /* the aggregate value is always a TypeReal. */
    a.type = TypeReal;
    a.name = qe_agg_op_to_str(agg_op)+"("+agg_attr.name+")";
    attrs.push_back(a);
} // }}}

Aggregate::~Aggregate() // {{{
{
}
// }}}

RC Aggregate::hashGroupValues() // {{{
{
    char tuple[PF_PAGE_SIZE];
    RC rc;

    /* hash the values. */
    while (!(rc = iter->getNextTuple(tuple)))
    {
        qe_hash_key k;
        char tuple_group_val[PF_PAGE_SIZE];
        /* enough to store int/float. */
        char tuple_val[8];
        float v;

        /* read in the group attr. */
        qe_get_tuple_element(tuple, tuple_attrs, group_attr, tuple_group_val);

        k.type = group_attr.type;

        /* copy in group agg value to hash key. */
        if (k.type == TypeInt)
        {
            k.int_v = *(int *) tuple_group_val;
        }
        else if (k.type ==TypeReal)
        {
            k.float_v = *(float *) tuple_group_val;
        }
        else if (k.type ==TypeVarChar)
        {
            unsigned int tuple_group_val_size = qe_get_tuple_size(tuple_group_val, group_attr);
            k.s_len = tuple_group_val_size;
            k.s = string(tuple_group_val + sizeof(k.s_len), k.s_len);
        }

        if (!hash.count(k))
        {
             /* first is value, second is count. */
             hash[k].first = 0;
             hash[k].second = 0;

             if (agg_op == MIN)
                 hash[k].first = FLT_MAX;
             else if (agg_op == MAX)
                 hash[k].first = FLT_MIN;
        }

        /* keep a running counter to perform average. */
        hash[k].second += 1;

        qe_get_tuple_element(tuple, tuple_attrs, agg_attr, tuple_val);
        v = (agg_attr.type == TypeReal) ? (*(float *) tuple_val) : (*(int *) tuple_val);

        if (agg_op == MIN)
             hash[k].first = (v <= hash[k].first) ? v : hash[k].first;
        else if (agg_op == MAX)
             hash[k].first = (v >= hash[k].first) ? v : hash[k].first;
        else if (agg_op == SUM || agg_op == AVG)
             hash[k].first += v;
        else if (agg_op == COUNT)
             hash[k].first += 1;
    }

    return rc;
}; // }}}

RC Aggregate::getNextTupleGroup(void *data) // {{{
{
    if (hash_iter != hash.end())
    {
        qe_hash_key k = (*hash_iter).first;
        float value = ((*hash_iter).second).first;
        float count = ((*hash_iter).second).second;
        float data_value = (agg_op == AVG) ? value / count : value;
        unsigned offset = 0;

        if (k.type == TypeInt)
        {
            memcpy(data, &k.int_v, sizeof(k.int_v));
            offset += sizeof(k.int_v);
        }
        else if (k.type == TypeReal)
        {
            memcpy(data, &k.float_v, sizeof(k.float_v));
            offset += sizeof(k.float_v);
        }
        else if (k.type == TypeVarChar)
        {
            memcpy((char *) data, &k.s_len, sizeof(k.s_len));
            memcpy((char *) data + sizeof(unsigned), k.s.c_str(), k.s_len);
            offset += sizeof(unsigned) + k.s_len;
        }

        memcpy((char *) data + offset, &data_value, sizeof(data_value));

        /* advance for next tuple. */
        hash_iter++;

        return 0;
    }

    return QE_EOF;
}; // }}}
