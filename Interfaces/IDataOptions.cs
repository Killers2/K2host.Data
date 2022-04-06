/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using K2host.Data.Classes;
using K2host.Data.Enums;
using System;
using System.Collections.Generic;

namespace K2host.Data.Interfaces
{

    public interface IDataOptions : IDisposable
    {

        /// <summary>
        /// The factories used to build the string queries for the databases.
        /// </summary>
        Dictionary<OConnectionDbType, ISqlFactory> SqlFactories { get; set; }

        /// <summary>
        /// The property converters in the context of the domain.
        /// </summary>
        List<IDataPropertyConverter> PropertyConverters { get; set; }

        /// <summary>
        /// The database connection in the context of the domain.
        /// </summary>
        Dictionary<string, IDataConnection> Connections { get; set; }
        
        /// <summary>
        /// The current primary connection used globally
        /// </summary>
        IDataConnection Primary { get; set; }

        /// <summary>
        /// The migration tool using the current primary connection
        /// </summary>
        ODataMigrationTool MigrationTool { get; set; }

    }

}
