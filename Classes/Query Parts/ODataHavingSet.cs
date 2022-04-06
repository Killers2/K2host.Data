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

using K2host.Data.Enums;
using K2host.Data.Extentions.ODataConnection;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where conditions on the ODataObject
    /// </summary>
    public class ODataHavingSet : IDisposable
    {

        /// <summary>
        /// The function around the column
        /// </summary>
        public ODataFunction Function { get; set; }

        /// <summary>
        /// The prefix to the start of this segment group.
        /// </summary>
        public PropertyInfo Column { get; set; }
       
        /// <summary>
        /// The operator of the query associated with the value(s)
        /// </summary>
        public ODataOperator Operator { get; set; }
        
        /// <summary>
        /// The value in question
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// This is used to join another part to the having query building.
        /// </summary>
        public ODataFollower FollowBy { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataHavingSet() 
        {
            Function    = ODataFunction.NONE;
            Column      = null;
            Operator    = ODataOperator.NONE;
            Value       = null;
            FollowBy    = ODataFollower.NONE;
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
                .HavingSetBuildString(this, out IEnumerable<DbParameter> pta, UseFieldPrefixing);
            
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
