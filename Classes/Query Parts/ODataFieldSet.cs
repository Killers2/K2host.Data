/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;

using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;

using K2host.Core;
using K2host.Data.Enums;
using K2host.Data.Extentions.ODataConnection;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where conditions on the ODataObject
    /// </summary>
    public class ODataFieldSet : IDisposable
    {

        /// <summary>
        /// The prefix to the start of this segment condition.
        /// You can only have either a subqueries, cases or a column.
        /// </summary>
        public PropertyInfo Column { get; set; }
       
        /// <summary>
        /// The function around the column
        /// </summary>
        public ODataFunction Function { get; set; }
        
        /// <summary>
        /// The cast type around the column
        /// </summary>
        public ODataCast Cast { get; set; }

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
        /// The suffix column alias.
        /// For xml use single quotes and string name or path. ('@FirstName' or 'table/MiddleName')
        /// </summary>
        public string Alias { get; set; }

        /// <summary>
        /// The value(s) in question
        /// </summary>
        public object NewValue { get; set; }

        /// <summary>
        /// The suffix value to plus on the end of the column
        /// for example : SELECT ',' + Column FROM
        /// </summary>
        public object Prefix { get; set; }

        /// <summary>
        /// The suffix value to plus on the end of the column
        /// for example : SELECT Column + ',' FROM
        /// </summary>
        public object Suffix { get; set; }

        /// <summary>
        /// The data type of the value(s)
        /// <para>This can be SqlDbType, MySqlDbType or OracleDbType</para>
        /// </summary>
        public int DataType { get; set; }

        /// <summary>
        /// The data update operator
        /// </summary>
        public ODataUpdateOperator UpdateOperator { get; set; }
        
        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataFieldSet() 
        {

            Column          = null;
            Alias           = string.Empty;
            NewValue        = string.Empty;
            DataType        = (int)SqlDbType.NVarChar;
            UpdateOperator  = ODataUpdateOperator.EQUAL;
            Function        = ODataFunction.NONE;
            Cast            = ODataCast.NONE;
            Prefix          = null;
            Suffix          = null;

        }

        /// <summary>
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public string ToUpdateString(out IEnumerable<DbParameter> parameters)
        {

            string output = ODataContext
                .Connection()
                .GetFactory()
                .FieldSetUpdateBuildString(this, out IEnumerable<DbParameter> pta);

            parameters = pta;

            return output;

        }

        /// <summary>
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public string ToInsertString(out IEnumerable<DbParameter> parameters)
        {

            string output = ODataContext
                .Connection()
                .GetFactory()
                .FieldSetInsertBuildString(this, out IEnumerable<DbParameter> pta);

            parameters = pta;

            return output;

        }

        /// <summary>
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public string ToSelectString(out IEnumerable<DbParameter> parameters, bool UseFieldPrefixing = false, bool UseFieldDefaultAlias = false)
        {
            
            string output = ODataContext
                .Connection()
                .GetFactory()
                .FieldSetSelectBuildString(this, out IEnumerable<DbParameter> pta, UseFieldPrefixing, UseFieldDefaultAlias);

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
