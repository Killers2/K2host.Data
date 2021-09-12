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
using System.Linq;
using System.Net;
using System.Text;

using K2host.Data.Enums;

using gd = K2host.Data.OHelpers;

namespace K2host.Data.Classes
{
    /// <summary>
    /// This class helps create a database connections string. 
    /// </summary>
    public class OConnection
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
        /// Creates the instance for this class
        /// </summary>
        public OConnection() 
        {
            ServerIp    = IPAddress.Any;
            Database    = string.Empty;
            Username    = string.Empty;
            Password    = string.Empty;
            Server      = string.Empty;
            Domain      = string.Empty;
            Port        = string.Empty;
        }

        /// <summary>
        /// Creates the instance for this class using a connection string.
        /// ODBC or SQL type
        /// </summary>
        public OConnection(string connectionString)
        {

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
        public OConnection(string server, string database, string username, string password)
        {

            Server = server;
           
            try { ServerIp = Dns.GetHostEntry(server).AddressList[0]; } catch { }

            Database = database;
            Username = username;
            Password = password;

        }
        
        /// <summary>
        /// Creates the instance for this class
        /// </summary>
        public OConnection(string server, string database, string domain, string username, string password)
        {

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
        public OConnection(string server, string database, int port, string username, string password)
        {

            Server = server;
            
            try { ServerIp = Dns.GetHostEntry(server).AddressList[0]; } catch { }

            Database = database;
            Username = username;
            Password = password;

            if (port != 1433)
                Port = port.ToString();

        }

        /// <summary>
        /// Clones and returns a new instance of this connection.
        /// </summary>
        /// <returns></returns>
        public OConnection Clone() 
        {
            return new OConnection() {
                Database    = this.Database,
                Domain      = this.Domain,
                Password    = this.Password,
                Port        = this.Port,
                Server      = this.Server,
                ServerIp    = this.ServerIp,
                Username    = this.Username
            };
        }

        /// <summary>
        /// Returns the connection string based on the values in this instance.
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public string ToString(OConnectionType e)
        {

            string ret = string.Empty;

            switch (e)
            {
                // For use of all .Net Providers
                case OConnectionType.SQLStandardSecurity:
                    if (Port == "1433")
                        Port = string.Empty;
                    ret = "Server=" + Server + (string.IsNullOrEmpty(Port) ? "" : "," + Port) + ";Database=" + Database + ";Uid=" + Username + ";Pwd=" + Password;
                    break;
                case OConnectionType.SQLTrustedConnection:
                    if (Port == "1433")
                        Port = string.Empty;
                    ret = "Server=" + Server + (string.IsNullOrEmpty(Port) ? "" : "," + Port) + ";Database=" + Database + ";Trusted_Connection=True;";
                    break;
                case OConnectionType.SQLTrustedConnectionCE:
                    if (Port == "1433")
                        Port = string.Empty;
                    ret = "Data Source=" + Server + (string.IsNullOrEmpty(Port) ? "" : "," + Port) + ";Initial Catalog=" + Database + ";Integrated Security=SSPI;User ID=" + Domain + "\\" + Username + ";Password=" + Password + ";";
                    break;
                case OConnectionType.OracleStandardSecurity:
                    ret = "Data Source=" + Database + ";Integrated Security=yes;";
                    break;
                case OConnectionType.OracleCredentialsSecurity:
                    ret = "Data Source=" + Username + "/" + Password + "@" + Server + (string.IsNullOrEmpty(Port) ? "" : ":" + Port) + "/" + Database;
                    break;
                case OConnectionType.MySqlStandardSecurity:
                    ret = "Server=" + Server + ";" + (string.IsNullOrEmpty(Port) ? "" : "Port=" + Port + ";") + " Database=" + Database + ";Uid=" + Username + ";Pwd=" + Password + ";AllowUserVariables=True;";
                    break;

            }

            return ret;

        }

        /// <summary>
        /// This creates the database of this instance. 
        /// The username and password must be one that can create databases and connect to the master database of the server.
        /// </summary>
        /// <param name="filePath">Where the mdf files are going to be created.</param>
        /// <returns></returns>
        public bool CreateMsSqlDatabase(string filePath) 
        {
            
            StringBuilder   output  = new();
            OConnection     clone   = this.Clone();

            clone.Database = "master";

            output.Append("CREATE DATABASE [" + this.Database + "]" + Environment.NewLine);
            output.Append(" CONTAINMENT = NONE" + Environment.NewLine);
            output.Append(" ON  PRIMARY" + Environment.NewLine);
            output.Append("( NAME = N'" + this.Database + "', FILENAME = N'" + filePath + "\\" + this.Database + ".mdf' , SIZE = 8192KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )" + Environment.NewLine);
            output.Append(" LOG ON " + Environment.NewLine);
            output.Append("( NAME = N'" + this.Database + "_log', FILENAME = N'" + filePath + "\\" + this.Database + "_log.ldf' , SIZE = 8192KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )" + Environment.NewLine);
            output.Append(";" + Environment.NewLine);
            output.Append("IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))" + Environment.NewLine);
            output.Append("begin" + Environment.NewLine);
            output.Append("EXEC [" + this.Database + "].[dbo].[sp_fulltext_database] @action = 'enable'" + Environment.NewLine);
            output.Append("end;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + this.Database + "] SET ANSI_NULL_DEFAULT OFF;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + this.Database + "] SET ANSI_NULLS OFF;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + this.Database + "] SET ANSI_PADDING OFF;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + this.Database + "] SET ANSI_WARNINGS OFF;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + this.Database + "] SET TRUSTWORTHY OFF;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + this.Database + "] SET AUTO_SHRINK OFF;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + this.Database + "] SET AUTO_UPDATE_STATISTICS ON;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + this.Database + "] SET QUOTED_IDENTIFIER OFF;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + this.Database + "] SET RECOVERY FULL;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + this.Database + "] SET  MULTI_USER;" + Environment.NewLine);

            try 
            { 
                gd.Query(output.ToString(), clone, OConnectionType.SQLStandardSecurity); 
            } 
            catch (Exception) 
            { 
                return false;
            }

            return true;
        
        }

        /// <summary>
        /// Checks to see if the database exists as a record, if true it will passback the table with the record held by sql server.
        /// </summary>
        /// <param name="record"></param>
        /// <returns></returns>
        public bool TestDatabase(out DataTable record)
        {

            record = null;

            StringBuilder   output  = new();
            OConnection     clone   = this.Clone();

            clone.Database = "master";
            
            output.Append("SELECT * FROM master.dbo.sysdatabases WHERE ('[' + name + ']' = '" + this.Database + "' OR name = '" + this.Database + "');");

            try
            {
               
                DataSet dt = gd.Get(output.ToString(), clone, OConnectionType.SQLStandardSecurity);

                if (dt.Tables[0].Rows.Count <= 0)
                    return false;
                else
                    record = dt.Tables[0];

            }
            catch (Exception)
            {
                return false;
            }

            return true;
        }
                
        /// <summary>
        /// Returns a list of the tables in the database
        /// </summary>
        /// <returns></returns>
        public string[] GetTables()
        {

            List<string>    output;
            StringBuilder   input   = new();
            OConnection     clone   = this.Clone();

            input.Append("SELECT [TABLE_NAME] FROM [" + this.Database + "].INFORMATION_SCHEMA.TABLES WHERE [TABLE_TYPE] = 'BASE TABLE' AND [TABLE_NAME] <> 'tbl_MigrationVersion';");

            try
            {

                DataSet dt = gd.Get(input.ToString(), clone, OConnectionType.SQLStandardSecurity);

                if (dt.Tables[0].Rows.Count <= 0)
                    return Array.Empty<string>();
                else
                {
                    output = new();
                    dt.Tables[0].Rows.Each(r => { output.Add(r[0].ToString()); });
                }
            }
            catch (Exception)
            {
                return Array.Empty<string>();
            }

            return output.ToArray();
        }

        /// <summary>
        /// This will drop and delete the database of this instance.
        /// The username and password must be one that can create databases and connect to the master database of the server.
        /// </summary>
        /// <returns></returns>
        public bool DeleteMsSqlDatabase()
        {
            
            StringBuilder   output = new();
            OConnection     clone = this.Clone();

            clone.Database = "master";

            output.Append("ALTER DATABASE [" + this.Database + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;" + Environment.NewLine);
            output.Append("DROP DATABASE [" + this.Database + "];" + Environment.NewLine);

            try 
            { 
                gd.Query(output.ToString(), clone, OConnectionType.SQLStandardSecurity); 
            } 
            catch (Exception) 
            { 
                return false; 
            }

            return true;

        }

    }

}
