/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Reflection;
using System.Text;
using System.Data;

using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;

using K2host.Data.Extentions.ODataConnection;
using K2host.Data.Enums;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where conditions on the ODataObject
    /// </summary>
    public class ODataOrder : IDisposable
    {

        /// <summary>
        /// The field / column name of the table / type in question
        /// </summary>
        public PropertyInfo Column { get; set; }
        
        /// <summary>
        /// Optional: Add a function around the column
        /// </summary>
        public ODataFunction Function { get; set; }
        
        /// <summary>
        /// This will append the NEWID() to the order for selected random rows.
        /// </summary>
        public bool Random { get; set; }

        /// <summary>
        /// The operator of the query associated with the value(s)
        /// </summary>
        public ODataOrderType Order { get; set; }
   
        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataOrder() 
        {

            Column  = null;
            Order   = ODataOrderType.ASC;
        }
        
        /// <summary>
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public string ToString(string prefix = "")
        {
            return ODataContext
                .Connection()
                .GetFactory()
                .OrderBuildString(this, prefix);
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
