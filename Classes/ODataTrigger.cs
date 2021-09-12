/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Data;

using K2host.Data.Interfaces;

namespace K2host.Data.Classes
{
    /// <summary>
    /// This class helps create a database connections string. 
    /// </summary>
    public class ODataTrigger : ODataObject<ODataTrigger>, IDataTrigger
    {

        /// <summary>
        /// The string type of which its either MS SQL Server or Agent
        /// </summary>
        [TSQLDataType(SqlDbType.NVarChar, 255)]
        public string Type { get; set; }

        /// <summary>
        /// The Connection string / sources to the database servers.
        /// </summary>
        [TSQLDataType(SqlDbType.NVarChar, -1)]
        public string Connection { get; set; }

        /// <summary>
        /// This is to determin weather the Uninstall process removes the setup or and uninstall locally.
        /// </summary>
        [TSQLDataType(SqlDbType.Bit)]
        public bool IsLocal { get; set; }

        /// <summary>
        /// This is to authenticate transactions / triggers from database servers.
        /// </summary>
        [TSQLDataType(SqlDbType.NVarChar, 1024)]
        public string AuthenticationKey { get; set; }

        /// <summary>
        /// This will either by the setup <see cref="IDataTrigger"/> or a Json string form from an Agent install
        /// </summary>
        [TSQLDataType(SqlDbType.VarBinary, -1)]
        public byte[] Configuration { get; set; }

        /// <summary>
        /// Creates the instance of this object
        /// </summary>
        /// <param name="connectionString"></param>
        public ODataTrigger(string connectionString)
            : base(connectionString)
        {
            Type                = string.Empty;
            Connection          = string.Empty;
            IsLocal             = false;
            AuthenticationKey   = string.Empty;
            Configuration       = Array.Empty<byte>();
        }


    }

}
