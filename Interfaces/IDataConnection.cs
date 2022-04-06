/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System;
using System.Net;
using K2host.Data.Enums;

namespace K2host.Data.Interfaces
{
    /// <summary>
    /// This interface helps create a database connections string. 
    /// </summary>
    public interface IDataConnection : IDisposable
    {

        /// <summary>
        /// The IPAddress of the database host.
        /// </summary>
        IPAddress ServerIp { get; set; }

        /// <summary>
        /// The database name.
        /// </summary>
        string Database { get; set; }

        /// <summary>
        /// The username for this connection string
        /// </summary>
        string Username { get; set; }

        /// <summary>
        /// The password for this connection string
        /// </summary>
        string Password { get; set; }
        
        /// <summary>
        /// The server / host for this connection string
        /// </summary>
        string Server { get; set; }
       
        /// <summary>
        /// The domain i.e \\Computer\User if required.
        /// </summary>
        string Domain { get; set; }
        
        /// <summary>
        /// The port number for this connection string, if required.
        /// </summary>
        string Port { get; set; }

        /// <summary>
        /// The connection type, if required.
        /// </summary>
        OConnectionType ConnectionType { get; set; }

    }

}
