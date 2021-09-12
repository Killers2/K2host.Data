/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using K2host.Data.Enums;
using System.ComponentModel;

namespace K2host.Data.Classes
{

    public class TSQLDataException : DescriptionAttribute
    {
        
        readonly ODataExceptionType v;

        public ODataExceptionType ODataExceptionType { get { return v; } }

        public TSQLDataException(ODataExceptionType v) : base(v.ToString())
        {
            this.v = v;
        }
        

    }


}
