/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System;
using System.Collections.Generic;
using System.Data.Common;

using K2host.Data.Classes;

namespace K2host.Data.Interfaces
{

    public interface IDataQuery : IDisposable
    {

        /// <summary>
        /// The list of parameters for parameter based queries
        /// </summary>
        IEnumerable<DbParameter> Parameters { get; set; }

        /// <summary>
        /// The object you are selecting from mapped in the database (table name).
        /// </summary>
        Type From { get; set; }

        /// <summary>
        /// Optional: The condition list based on the the when is question
        /// </summary>
        ODataCondition[] Where { get; set; }

    }

}
