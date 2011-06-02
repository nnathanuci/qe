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

Filter::Filter(Iterator *input, const Condition &condition) // {{{
{
    iter = input;
    cond = condition;
    vector<Attribute> attrs;
    input->getAttributes(attrs);
    for (unsigned int i=0; i < attrs.size(); i++)
    {
         cout << "attribute " << i << ": " << attrs[i].name << endl;
    }
} // }}}

RC Filter::getNextTuple(void *data) // {{{
{
    return QE_EOF;
} // }}}

Filter::~Filter() // {{{
{
} // }}}
