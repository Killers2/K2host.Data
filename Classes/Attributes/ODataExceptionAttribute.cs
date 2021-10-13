/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System;
using K2host.Data.Enums;
using K2host.Data.Interfaces;

namespace K2host.Data.Attributes
{

    public class ODataExceptionAttribute : ODataAttribute
    {

        readonly ODataExceptionType DataExceptionType;

        public ODataExceptionType ODataExceptionType { get { return DataExceptionType; } }

        public ODataExceptionAttribute(ODataExceptionType e)
        {
            DataExceptionType = e;
        }

    }

}
