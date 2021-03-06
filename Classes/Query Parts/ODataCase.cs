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

using K2host.Data.Extentions.ODataConnection;

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
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public string ToString(out IEnumerable<DbParameter> parameters, bool UseFieldPrefixing = false)
        {

            string output = ODataContext
                .Connection()
                .GetFactory()
                .CaseBuildString(this, out IEnumerable<DbParameter> pta, UseFieldPrefixing);

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
