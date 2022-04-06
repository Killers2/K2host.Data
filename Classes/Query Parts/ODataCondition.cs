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
using System.Linq;
using System.Reflection;
using System.Text;
using System.Data;

using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;

using K2host.Core;
using K2host.Data.Enums;
using K2host.Data.Extentions.ODataConnection;

using gd = K2host.Data.OHelpers;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where conditions on the ODataObject
    /// </summary>
    public class ODataCondition : IDisposable
    {

        /// <summary>
        /// The prefix to the start of this segment condition.
        /// </summary>
        public ODataFollower Prefix { get; set; }

        /// <summary>
        /// This helps contain the query with the bracket
        /// </summary>
        public ODataConditionContainer Container { get; set; }

        /// <summary>
        /// The field / column name of the table / type in question
        /// You can only have either a subqueries, cases or a column.
        /// </summary>
        public PropertyInfo Column { get; set; }
        
        /// <summary>
        /// The field / column name of the table / type in question
        /// You can only have either a subqueries, cases or a column.
        /// </summary>
        public string ColumnText { get; set; }

        /// <summary>
        /// The field / column as a case set of the table / type in question
        /// You can only have either a subqueries, cases or a column.
        /// </summary>
        public ODataCase[] Case { get; set; }

        /// <summary>
        /// The field / column as a sub query set of the table / type in question
        /// You can only have either a subqueries, cases or a column.
        /// </summary>
        public ODataSelectQuery SubQuery { get; set; }

        /// <summary>
        /// The operator of the query associated with the value(s)
        /// </summary>
        public ODataOperator Operator { get; set; }

        /// <summary>
        /// If using the "like" operator you can select the type of "like" operator.
        /// </summary>
        public ODataLikeOperator LikeOperator { get; set; }

        /// <summary>
        /// The value(s) in question, this can also be of type property info for joins and cross applies
        /// </summary>
        public object[] Values { get; set; }

        /// <summary>
        /// This is used to join another part to the query building.
        /// </summary>
        public ODataFollower FollowBy { get; set; }

        /// <summary>
        /// The function around the field column
        /// </summary>
        public ODataFunction FieldFunction { get; set; }

        /// <summary>
        /// The function around the value / value column
        /// </summary>
        public ODataFunction ValueFunction { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataCondition() 
        {

            Prefix          = ODataFollower.NONE;
            Container       = ODataConditionContainer.NONE;
            Column          = null;
            ColumnText      = string.Empty;
            Operator        = ODataOperator.NONE;
            LikeOperator    = ODataLikeOperator.NONE;
            Values          = Array.Empty<object>();
            FollowBy        = ODataFollower.NONE;
            FieldFunction   = ODataFunction.NONE;
            ValueFunction   = ODataFunction.NONE;
        }

        /// <summary>
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public string ToString(out IEnumerable<DbParameter> parameters, bool UsePrefix = false)
        {

            string output = ODataContext
                .Connection()
                .GetFactory()
                .ConditionBuildString(this, out IEnumerable<DbParameter> pta, UsePrefix);

            parameters = pta;

            return output;

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
