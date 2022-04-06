/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Text;
using System.Data;

using Oracle.ManagedDataAccess.Client;
using MySql.Data.MySqlClient;

using K2host.Data.Enums;
using K2host.Data.Extentions.ODataConnection;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where cases on the ODataObject
    /// </summary>
    public class ODataApplySet : IDisposable
    {

        /// <summary>
        /// The type of apply used for this query
        /// </summary>
        public ODataApplyType ApplyType { get; set; }

        /// <summary>
        /// The query wrapped in the apply type, which would have the linking within.
        /// </summary>
        public ODataSelectQuery Query { get; set; }

        /// <summary>
        /// The apply alias for the main query
        /// </summary>
        public string Alias { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataApplySet() 
        {

            ApplyType   = ODataApplyType.CROSS;
            Query       = null;
            Alias       = string.Empty;
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
                .ApplySetBuildString(this);
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
