#include "qe.h"

Project::Project(Iterator *input, const vector<string> &attrNames) // {{{
{
    iter = input;
    attr_names = attrNames;
    input->getAttributes(tuple_attrs);
} // }}}

RC Project::getNextTuple(void *project_tuple) // {{{
{
    char *project_tuple_ptr = (char *) project_tuple;
    char tuple[PF_PAGE_SIZE];

    RC rc;

    if ((rc = iter->getNextTuple(tuple)))
        return rc;

    for (unsigned int i=0; i < attr_names.size(); i++)
    {
        Attribute a;

        qe_get_attribute(attr_names[i], tuple_attrs, a);
        qe_get_tuple_element(tuple, tuple_attrs, attr_names[i], project_tuple_ptr);
        project_tuple_ptr += qe_get_tuple_size(project_tuple_ptr, a);
    }

    return 0;
} // }}}

Project::~Project() // {{{
{
} // }}}

void Project::getAttributes(vector<Attribute> &attrs) const // {{{
{
    attrs.clear();

    for (unsigned int i = 0; i < attr_names.size(); i++)
    {
        Attribute a;
        qe_get_attribute(attr_names[i], tuple_attrs, a);
        attrs.push_back(a);
    }

    return;
} // }}}
