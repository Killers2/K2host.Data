/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Data;
using System.Reflection;
using System.Text;

using K2host.Core;

using gd = K2host.Data.OHelpers;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where cases on the ODataObject
    /// </summary>
    public class ODataCase : IDisposable
    {

        /// <summary>
        /// The condition list based on the the when is question
        /// </summary>
        public ODataCondition[] When { get; set; }

        /// <summary>
        /// The value object or PropertyInfo or ODataPropertyInfo selected when then occurs
        /// </summary>
        public object Then { get; set; }

        /// <summary>
        /// The value object or PropertyInfo or ODataPropertyInfo selected when else occurs
        /// </summary>
        public object Else { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataCase() 
        {

            When = null;
            Then = null;
            Else = null;
        }

        /// <summary>
        /// This returns and builds the string representation of the case segment.
        /// </summary>
        /// <returns></returns>
        public string ToString(bool UseFieldPrefixing = false) 
        {

            StringBuilder output = new();

            output.Append("WHEN (");

            When.ForEach(condition => { 
                output.Append(condition.ToString(UseFieldPrefixing)); 
            });

            output.Append(") THEN ");

            if (Then.GetType().Name == "RuntimePropertyInfo")
                output.Append(gd.GetSqlRepresentation(SqlDbType.Structured, "[" + ((PropertyInfo)Then).Name) + "]");
            else if (Then.GetType().Name == "ODataPropertyInfo")
                output.Append(gd.GetSqlRepresentation(SqlDbType.Structured, ((PropertyInfo)Then).Name));
            else
                output.Append(gd.GetSqlRepresentation(gd.GetSqlDbType(Then.GetType()), Then));

            if (Else != null)
            {
                output.Append(" ELSE ");
                if (Else.GetType().Name == "RuntimePropertyInfo")
                    output.Append(gd.GetSqlRepresentation(SqlDbType.Structured, "[" + ((PropertyInfo)Else).Name) + "]");
                else if (Else.GetType().Name == "ODataPropertyInfo")
                    output.Append(gd.GetSqlRepresentation(SqlDbType.Structured, ((PropertyInfo)Else).Name));
                else
                    output.Append(gd.GetSqlRepresentation(gd.GetSqlDbType(Else.GetType()), Else));
            }

            return output.ToString();

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
