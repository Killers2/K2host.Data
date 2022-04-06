/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Data;
using K2host.Data.Attributes;
using K2host.Data.Interfaces;

using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;

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
        [ODataType(SqlDbType.NVarChar, MySqlDbType.VarString, OracleDbType.Varchar2, 255)]
        public string Type { get; set; } = string.Empty;

        /// <summary>
        /// The Connections / sources to the database servers.
        /// </summary>
        [ODataType(SqlDbType.NVarChar, MySqlDbType.VarString, OracleDbType.Varchar2, -1)]
        public string Connection { get; set; } = string.Empty;

        /// <summary>
        /// This is to determin weather the Uninstall process removes the setup or and uninstall locally.
        /// </summary>
        [ODataType(SqlDbType.Bit, MySqlDbTypeExt.Boolean, OracleDbType.Boolean)]
        public bool IsLocal { get; set; } = false;

        /// <summary>
        /// This is to authenticate transactions / triggers from database servers.
        /// </summary>
        [ODataType(SqlDbType.NVarChar, MySqlDbType.VarString, OracleDbType.Varchar2, 1024)]
        public string AuthenticationKey { get; set; } = string.Empty;

        /// <summary>
        /// This will either by the setup <see cref="IDataTrigger"/> or a Json string form from an Agent install
        /// </summary>
        [ODataType(SqlDbType.VarBinary, MySqlDbType.Blob, OracleDbType.Blob, -1)]
        public byte[] Configuration { get; set; } = Array.Empty<byte>();

        /// <summary>
        /// Creates the instance of this object
        /// </summary>
        public ODataTrigger()
            : base()
        {
        }


    }

}
