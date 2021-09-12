/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;

namespace K2host.Data.Classes
{
    /// <summary>
    /// This class helps creates an exception based on the ODataObjects. 
    /// </summary>
    public class ODataException : Exception
    {

        public ODataException()
        {
        }

        public ODataException(string message)
            : base(message)
        {
        }

        public ODataException(string message, Exception inner)
            : base(message, inner)
        {
        }

    }

}
