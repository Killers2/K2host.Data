/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Data;
using System.Linq;
using System.Net;

using K2host.Data.Enums;
using K2host.Data.Interfaces;
using K2host.Data.Extentions.ODataConnection;

namespace K2host.Data.Classes
{
    /// <summary>
    /// This class helps create a database connections string. 
    /// </summary>
    public class ODataConnection : IDataConnection
    {
        /// <summary>
        /// The IPAddress of the database host.
        /// </summary>
        public IPAddress ServerIp { get; set; }

        /// <summary>
        /// The database name.
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// The username for this connection string
        /// </summary>
        public string Username { get; set; }

        /// <summary>
        /// The password for this connection string
        /// </summary>
        public string Password { get; set; }
        
        /// <summary>
        /// The server / host for this connection string
        /// </summary>
        public string Server { get; set; }
       
        /// <summary>
        /// The domain i.e \\Computer\User if required.
        /// </summary>
        public string Domain { get; set; }
        
        /// <summary>
        /// The port number for this connection string, if required.
        /// </summary>
        public string Port { get; set; }

        /// <summary>
        /// The connection type, if required.
        /// </summary>
        public OConnectionType ConnectionType { get; set; }

        /// <summary>
        /// Creates the instance for this class
        /// </summary>
        public ODataConnection() 
        {
            ServerIp    = IPAddress.Any;
            Database    = string.Empty;
            Username    = string.Empty;
            Password    = string.Empty;
            Server      = string.Empty;
            Domain      = string.Empty;
            Port        = string.Empty;
            ConnectionType = OConnectionType.SQLStandardSecurity;
        }

        /// <summary>
        /// Creates the instance for this class using a connection string.
        /// ODBC or SQL type
        /// </summary>
        public ODataConnection(string connectionString, OConnectionType connectionType)
        {

            ConnectionType = connectionType;

            string[] keyPairs = connectionString.Split(new string[] { ";" }, StringSplitOptions.None);

            try
            {

                Server = keyPairs.Where(n =>
                    n.ToLower().StartsWith("server") ||
                    n.ToLower().StartsWith("data source")
                ).First().Split(new string[] { "=" }, StringSplitOptions.None)[1];

                try { ServerIp = Dns.GetHostEntry(Server).AddressList[0]; } catch { }

                Database = keyPairs.Where(n =>
                    n.ToLower().StartsWith("database") ||
                    n.ToLower().StartsWith("initial catalog")
                ).First().Split(new string[] { "=" }, StringSplitOptions.None)[1];

                Username = keyPairs.Where(n =>
                    n.ToLower().StartsWith("uid") ||
                    n.ToLower().StartsWith("user id")
                ).First().Split(new string[] { "=" }, StringSplitOptions.None)[1];

                Password = keyPairs.Where(n =>
                    n.ToLower().StartsWith("pwd") ||
                    n.ToLower().StartsWith("password")
                ).First().Split(new string[] { "=" }, StringSplitOptions.None)[1];

            }
            catch (Exception ex)
            {
                throw new Exception("OConnection: init() connection string issue. " + ex.Message);
            }
        }

        /// <summary>
        /// Creates the instance for this class
        /// </summary>
        public ODataConnection(string server, string database, string username, string password, OConnectionType connectionType)
        {
            ConnectionType = connectionType;

            Server = server;
           
            try { ServerIp = Dns.GetHostEntry(server).AddressList[0]; } catch { }

            Database = database;
            Username = username;
            Password = password;

        }
        
        /// <summary>
        /// Creates the instance for this class
        /// </summary>
        public ODataConnection(string server, string database, string domain, string username, string password, OConnectionType connectionType)
        {
            ConnectionType = connectionType;

            Server = server;
            
            try { ServerIp = Dns.GetHostEntry(server).AddressList[0]; } catch { }

            Database = database;
            Username = username;
            Password = password;
            Domain = domain;

        }
        
        /// <summary>
        /// Creates the instance for this class
        /// </summary>
        public ODataConnection(string server, string database, int port, string username, string password, OConnectionType connectionType)
        {
            ConnectionType = connectionType;

            Server = server;
            
            try { ServerIp = Dns.GetHostEntry(server).AddressList[0]; } catch { }

            Database = database;
            Username = username;
            Password = password;

            if (port != 1433)
                Port = port.ToString();

        }

        /// <summary>
        /// Returns the connection string based on the values in this instance.
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public override string ToString()
        {
            return this.GetConnectionString();
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
