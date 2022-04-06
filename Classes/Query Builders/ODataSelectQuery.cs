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
using K2host.Data.Enums;
using K2host.Data.Interfaces;
using K2host.Data.Extentions.ODataConnection;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where cases on the ODataObject
    /// </summary>
    public class ODataSelectQuery : IDataQuery
    {

        /// <summary>
        /// The list of parameters for parameter based queries
        /// </summary>
        public IEnumerable<DbParameter> Parameters { get; set; } = Array.Empty<DbParameter>();

        /// <summary>
        /// Optional: Enables placing the object / table name in front of the field xxxx.[xxxxx].
        /// </summary>
        public bool UseFieldPrefixing { get; set; }

        /// <summary>
        /// Optional: Enables placing a default alias name table.[field] AS [table.field].
        /// </summary>
        public bool UseFieldDefaultAlias { get; set; }
        
        /// <summary>
        /// Optional: Enables the join as a filter and dosen't return the objects.
        /// </summary>
        public bool IncludeJoinFields { get; set; }
        
        /// <summary>
        /// Optional: Enables placing a default alias name table.[field] AS [table.field].
        /// </summary>
        public bool IncludeApplyFields { get; set; }
       
        /// <summary>
        /// Optional: Disables the deleted flag on any query to view old items
        /// </summary>
        public bool IncludeRemoved { get; set; }

        /// <summary>
        /// Optional: Make this select distinct rows.
        /// </summary>
        public bool IsSubQuery { get; set; }

        /// <summary>
        /// Optional: Make this select distinct rows.
        /// </summary>
        public bool Distinct { get; set; }

        /// <summary>
        /// Optional: Select the top X rows.
        /// </summary>
        public int Top { get; set; }

        /// <summary>
        /// This helps define a query in a query of a suff function.
        /// </summary>
        public ODataStuffFunction Stuff { get; set; }

        /// <summary>
        /// The prefix to the start of this segment condition.
        /// You can only have either a subqueries, cases or a column.
        /// </summary>
        public ODataFieldSet[] Fields { get; set; }

        /// <summary>
        /// The object you are selecting from mapped in the database (table name).
        /// </summary>
        public Type From { get; set; }

        /// <summary>
        /// Optional: Add Join sets the the select query
        /// </summary>
        public ODataJoinSet[] Joins { get; set; }
        
        /// <summary>
        /// Optional: Add applies to the query
        /// </summary>
        public ODataApplySet[] Applies { get; set; }

        /// <summary>
        /// Optional: The condition list based on the the when is question
        /// </summary>
        public ODataCondition[] Where { get; set; }

        /// <summary>
        /// Optional: The group by fields if required
        /// </summary>
        public ODataGroupSet[] Group { get; set; }

        /// <summary>
        /// Optional: The having by fields if required for this statment
        /// </summary>
        public ODataHavingSet[] Having { get; set; }

        /// <summary>
        /// Optional: If listed you can add an order
        /// </summary>
        public ODataOrder[] Order { get; set; }

        /// <summary>
        /// Optional: Add a take skip for paging
        /// </summary>
        public ODataTakeSkip TakeSkip { get; set; }
 
        /// <summary>
        /// Optional: Add a "for path"
        /// </summary>
        public ODataForPath ForPath { get; set; }

        /// <summary>
        /// Optional: The suffix alias is this a sub query.
        /// </summary>
        public string Alias { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataSelectQuery() 
        {

            From                    = null;
            Distinct                = false;
            Where                   = null;
            Order                   = null;
            Alias                   = string.Empty;
            UseFieldDefaultAlias    = true;
            UseFieldPrefixing       = true;
            IncludeJoinFields       = true;
            IncludeApplyFields      = true;
            IncludeRemoved          = false;

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
                .SelectQueryBuildString(this);
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
