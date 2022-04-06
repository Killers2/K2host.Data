/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System;
using System.Collections.Generic;
using K2host.Data.Enums;
using K2host.Data.Interfaces;

namespace K2host.Data.Classes
{
    public class ODataOptions : IDataOptions
    {

        /// <summary>
        /// The factories used to build the string queries for the databases.
        /// </summary>
        public Dictionary<OConnectionDbType, ISqlFactory> SqlFactories { get; set; } = new();

        /// <summary>
        /// The property converters in the context of the domain.
        /// </summary>
        public List<IDataPropertyConverter> PropertyConverters { get; set; } = new();

        /// <summary>
        /// The database connection in the context of the domain.
        /// </summary>
        public Dictionary<string, IDataConnection> Connections { get; set; }

        /// <summary>
        /// The current primary connection used globally
        /// </summary>
        public IDataConnection Primary { get; set; }

        /// <summary>
        /// The migration tool using the current primary connection
        /// </summary>
        public ODataMigrationTool MigrationTool { get; set; }


        #region Deconstuctor

        private bool IsDisposed = false;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!IsDisposed)
                if (disposing)
                {




                }
            IsDisposed = true;
        }

        #endregion

    }
}
