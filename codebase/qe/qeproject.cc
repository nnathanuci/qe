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

    /* need to pack the attributes in the order of the names, and so we need to enumerate the attribute vector for each name. */
    for (unsigned int i=0; i < attr_names.size(); i++)
    {
        unsigned int offset = 0;

        for (unsigned int j=0; j < attrs.size(); j++)
        {
             if (attr_names[i] == attrs[j].name)
             {
                  /* projected attribute is appended to the tuple that is returned (project_tuple) */

                  if (attrs[j].type == TypeInt)
                  {
                       memcpy(project_tuple_ptr, (char *) tuple + offset, sizeof(int));
                       project_tuple_ptr += sizeof(int);
                  }
                  else if (attrs[j].type == TypeReal)
                  {
                       memcpy(project_tuple_ptr, (char *) tuple + offset, sizeof(float));
                       project_tuple_ptr += sizeof(float);
                  }
                  else if (attrs[j].type == TypeVarChar)
                  {
                       unsigned int len = (*(unsigned *) ((char *) tuple + offset));
                       memcpy(project_tuple_ptr, (char *) tuple + offset, sizeof(unsigned) + len);
                       project_tuple_ptr += sizeof(unsigned);
                       offset += sizeof(unsigned);
                       memcpy(project_tuple_ptr, (char *) tuple + offset, len);
                       project_tuple_ptr += len;
                  }

                  /* finished with this attribute, scan for the next attribute/name pair. */
                  break;
             }
             else
             {
                  /* skip the attribute since it doesnt match the name we want. */
                  if (attrs[j].type == TypeInt)
                      offset += sizeof(int);
                  else if (attrs[j].type == TypeReal)
                      offset += sizeof(float);
                  else if (attrs[j].type == TypeVarChar)
                      offset += sizeof(unsigned) + (*(unsigned *) ((char *) tuple + offset));
             }
        }
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
