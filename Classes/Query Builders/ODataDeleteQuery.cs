/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Text;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Data;

using Oracle.ManagedDataAccess.Client;
using MySql.Data.MySqlClient;

using K2host.Core;
using K2host.Data.Interfaces;
using K2host.Data.Extentions.ODataConnection;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where cases on the ODataObject
    /// </summary>
    public class ODataDeleteQuery : IDataQuery
    {
        /// <summary>
        /// The list of parameters for parameter based queries
        /// </summary>
        public IEnumerable<DbParameter> Parameters { get; set; } = Array.Empty<DbParameter>();

        /// <summary>
        /// The object you are selecting from mapped in the database (table name).
        /// </summary>
        public Type From { get; set; }
        
        /// <summary>
        /// Optional: The condition list based on the the when is question
        /// </summary>
        public ODataCondition[] Where { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataDeleteQuery() 
        {
            From    = null;
            Where   = null;
        }

        /// <summary>
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return ODataContext
                .Connection()
                .GetFactory()
                .DeleteQueryBuildString(this);
        }

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
