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
using System.Data;
using System.Linq;

using Oracle.ManagedDataAccess.Client;
using MySql.Data.MySqlClient;

using K2host.Core;
using K2host.Data.Extentions.ODataConnection;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where cases on the ODataObject
    /// </summary>
    public class ODataInsertQuery : IDisposable
    {
        /// <summary>
        /// The list of parameters for parameter based queries
        /// </summary>
        public IEnumerable<DbParameter> Parameters { get; set; } = Array.Empty<DbParameter>();

        /// <summary>
        /// Optional: Enables Primary Key inserting.
        /// </summary>
        public bool UseIdentityInsert { get; set; }

        /// <summary>
        /// The object you are selecting from mapped in the database (table name).
        /// </summary>
        public Type To { get; set; }

        /// <summary>
        /// The prefix to the start of this segment condition.
        /// You can only have use values from either Fields, ValueSets, Select.
        /// </summary>
        public ODataFieldSet[] Fields { get; set; }

        /// <summary>
        /// The prefix to the start of this segment condition.
        /// You can only have use values from either Fields, ValueSets, Select.
        /// </summary>
        public List<ODataFieldSet[]> ValueSets { get; set; }

        /// <summary>
        /// The prefix to the start of this segment condition.
        /// You can only have use values from either Fields, ValueSets, Select.
        /// </summary>
        public ODataSelectQuery Select { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataInsertQuery() 
        {
            To                  = null;
            Fields              = null;
            ValueSets           = new List<ODataFieldSet[]>();
            Select              = null;
            UseIdentityInsert   = false;
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
                .InsertQueryBuildString(this);
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
