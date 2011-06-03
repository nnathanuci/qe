#include "qe.h"

Project::Project(Iterator *input, const vector<string> &attrNames) // {{{
{
    iter = input;
    attr_names = attrNames;
    getAttributes(attrs);
} // }}}

RC Project::getNextTuple(void *project_tuple) // {{{
{
    unsigned char *project_tuple_ptr = (unsigned char *) project_tuple;
    unsigned char tuple[PF_PAGE_SIZE];

    RC rc;

    if (rc = iter->getNextTuple(tuple))
        return rc;

    for (unsigned int i=0; i < attr_names.size(); i++)
    {
        Attribute a;

        qe_getAttribute(attr_names[i], attrs, a);
        qe_get_tuple_element(tuple, attrs, attr_names[i], project_tuple_ptr);
        project_tuple_ptr += qe_get_tuple_size(project_tuple_ptr, a);
    }

    return 0;
} // }}}

Project::~Project() // {{{
{
} // }}}

void Project::getAttributes(vector<Attribute> &attrs) const // {{{
{
    iter->getAttributes(attrs);

    return;
} // }}}
