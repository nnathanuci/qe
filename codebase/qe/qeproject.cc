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

Project::Project(Iterator *input, const vector<string> &attrNames)
{
    iter = input;
    attr_names = attrNames;
    getAttributes(attrs);
}

RC Project::getNextTuple(void *project_data) // {{{
{
    unsigned char *project_data_ptr = (unsigned char *) project_data;
    unsigned char data[PF_PAGE_SIZE];

    if (iter->getNextTuple(data) == QE_EOF)
        return QE_EOF;

    /* need to pack the attributes in the order of the names, and so we need to enumerate the attribute vector for each name. */
    for (unsigned int i=0; i < attr_names.size(); i++)
    {
        unsigned int offset = 0;

        for (unsigned int j=0; j < attrs.size(); j++)
        {
             if (attr_names[i] == attrs[j].name)
             {
                  /* projected attribute is appended to the data that is returned (project_data) */

                  if (attrs[j].type == TypeInt)
                  {
                       memcpy(project_data_ptr, (char *) data + offset, sizeof(int));
                       project_data_ptr += sizeof(int);
                  }
                  else if (attrs[j].type == TypeReal)
                  {
                       memcpy(project_data_ptr, (char *) data + offset, sizeof(float));
                       project_data_ptr += sizeof(float);
                  }
                  else if (attrs[j].type == TypeVarChar)
                  {
                       unsigned int len = (*(unsigned *) ((char *) data + offset));
                       memcpy(project_data_ptr, (char *) data + offset, sizeof(unsigned) + len);
                       project_data_ptr += sizeof(unsigned);
                       offset += sizeof(unsigned);
                       memcpy(project_data_ptr, (char *) data + offset, len);
                       project_data_ptr += len;
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
                      offset += sizeof(unsigned) + (*(unsigned *) ((char *) data + offset));
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
