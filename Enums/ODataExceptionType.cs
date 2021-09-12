/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/


using System;

namespace K2host.Data.Enums
{
    /// <summary>
    /// This is used on the TSQLDataException
    /// </summary>
    [Flags]
    public enum ODataExceptionType
    {
        NON_INSERT = 1,
        NON_UPDATE = 2,
        NON_SELECT = 4,
        NON_DELETE = 8,
        NON_CREATE = 16,
    }
}
