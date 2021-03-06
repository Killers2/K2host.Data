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

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where conditions on the ODataObject
    /// </summary>
    public class ODataGroupSet : IDisposable
    {

        /// <summary>
        /// The prefix to the start of this segment group.
        /// </summary>
        public PropertyInfo Column { get; set; }
        
        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataGroupSet() 
        {
            Column  = null;
        }

        /// <summary>
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public string ToString(bool UseFieldPrefixing = false)
        {
            return ODataContext
                .Connection()
                .GetFactory()
                .GroupSetBuildString(this, UseFieldPrefixing);
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
