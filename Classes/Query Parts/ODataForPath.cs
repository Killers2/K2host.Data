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

using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;

using K2host.Data.Enums;
using K2host.Data.Extentions.ODataConnection;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where cases on the ODataObject
    /// </summary>
    public class ODataForPath : IDisposable
    {

        /// <summary>
        /// The For path type at the end of the query
        /// </summary>
        public ODataForPathType PathType { get; set; }

        /// <summary>
        /// if using XML you set the xml attribute for xsi
        /// </summary>
        public bool EnableXmlXSI { get; set; }

        /// <summary>
        /// The types path is required
        /// </summary>
        public string Path { get; set; }

        /// <summary>
        /// The types root is required
        /// </summary>
        public string Root { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataForPath() 
        {
            PathType        = ODataForPathType.XML;
            EnableXmlXSI    = false;
            Path            = string.Empty;
            Root            = string.Empty;

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
                .ForPathBuildString(this);
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
