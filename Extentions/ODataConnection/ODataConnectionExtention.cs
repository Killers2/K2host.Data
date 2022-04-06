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

using K2host.Data.Classes;
using K2host.Data.Enums;
using K2host.Data.Interfaces;

namespace K2host.Data.Extentions.ODataConnection
{

    public static class ODataConnectionExtention
    {

        /// <summary>
        /// Returns the connection string based on the values in this instance.
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public static string GetConnectionString<I>(this I e) where I : IDataConnection
        {

            string output = string.Empty;

            switch (e.ConnectionType)
            {
                // For use of all .Net Providers
                case OConnectionType.SQLStandardSecurity:
                    if (e.Port == "1433")
                        e.Port = string.Empty;
                    output = "Server=" + e.Server + (string.IsNullOrEmpty(e.Port) ? "" : "," + e.Port) + ";Database=" + e.Database + ";Uid=" + e.Username + ";Pwd=" + e.Password;
                    break;
                case OConnectionType.SQLTrustedConnection:
                    if (e.Port == "1433")
                        e.Port = string.Empty;
                    output = "Server=" + e.Server + (string.IsNullOrEmpty(e.Port) ? "" : "," + e.Port) + ";Database=" + e.Database + ";Trusted_Connection=True;";
                    break;
                case OConnectionType.SQLTrustedConnectionCE:
                    if (e.Port == "1433")
                        e.Port = string.Empty;
                    output = "Data Source=" + e.Server + (string.IsNullOrEmpty(e.Port) ? "" : "," + e.Port) + ";Initial Catalog=" + e.Database + ";Integrated Security=SSPI;User ID=" + e.Domain + "\\" + e.Username + ";Password=" + e.Password + ";";
                    break;
                case OConnectionType.OracleStandardSecurity:
                    output = "Data Source=" + e.Database + ";Integrated Security=yes;";
                    break;
                case OConnectionType.OracleCredentialsSecurity:
                    output = "Data Source=" + e.Username + "/" + e.Password + "@" + e.Server + (string.IsNullOrEmpty(e.Port) ? "" : ":" + e.Port) + "/" + e.Database;
                    break;
                case OConnectionType.MySqlStandardSecurity:
                    output = "Server=" + e.Server + ";" + (string.IsNullOrEmpty(e.Port) ? "" : "Port=" + e.Port + ";") + " Database=" + e.Database + ";Uid=" + e.Username + ";Pwd=" + e.Password + ";AllowUserVariables=True;";
                    break;
                case OConnectionType.MySqlStandardNoDatabase:
                    output = "Server=" + e.Server + ";" + (string.IsNullOrEmpty(e.Port) ? "" : "Port=" + e.Port + ";") + " Uid=" + e.Username + ";Pwd=" + e.Password + ";AllowUserVariables=True;";
                    break;
            }

            return output;

        }

        /// <summary>
        /// Clones and returns a new instance of this connection.
        /// </summary>
        /// <returns></returns>
        public static I Clone<I>(this I e) where I : IDataConnection
        {

            I output = (I)Activator.CreateInstance(e.GetType());

            output.Database         = e.Database;
            output.Domain           = e.Domain;
            output.Password         = e.Password;
            output.Port             = e.Port;
            output.Server           = e.Server;
            output.ServerIp         = e.ServerIp;
            output.Username         = e.Username;
            output.ConnectionType   = e.ConnectionType;

            return output;
        }

        /// <summary>
        /// Gets the DbEnumType from the conection based on the enum data types XXXDbType enums
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public static OConnectionDbType GetDbEnumType<I>(this I e) where I : IDataConnection
        {
            return e.ConnectionType switch
            {
                OConnectionType.SQLStandardSecurity or OConnectionType.SQLTrustedConnection or OConnectionType.SQLTrustedConnectionCE   => OConnectionDbType.SqlDbType,
                OConnectionType.OracleStandardSecurity or OConnectionType.OracleCredentialsSecurity                                     => OConnectionDbType.OracleDbType,
                OConnectionType.MySqlStandardSecurity or OConnectionType.MySqlStandardNoDatabase                                        => OConnectionDbType.MySqlDbType,
                _                                                                                                                       => OConnectionDbType.SqlDbType,
            };
        }

        /// <summary>
        /// This creates the database of this instance. 
        /// The username and password must be one that can create databases and connect to the master database of the server.
        /// </summary>
        /// <param name="filePath">Where the mdf files are going to be created.</param>
        /// <returns></returns>
        public static bool CreateDatabase<I>(this I e, string filePath) where I : IDataConnection
        {
            return e
                .GetFactory()
                .CreateDatabase(e, filePath);
        }

        /// <summary>
        /// Checks to see if the database exists as a record, if true it will passback the table with the record held by sql server.
        /// </summary>
        /// <param name="record"></param>
        /// <returns></returns>
        public static bool TestDatabase<I>(this I e, out DataTable record) where I : IDataConnection
        {

            bool output = e
                .GetFactory()
                .TestDatabase(e, out DataTable tbl);

            record = tbl;

            return output;

        }

        /// <summary>
        /// Returns a list of the tables in the database
        /// </summary>
        /// <returns></returns>
        public static string[] GetTables<I>(this I e) where I : IDataConnection
        {
            return e
                .GetFactory()
                .GetTables(e);
        }

        /// <summary>
        /// This will drop and delete the database of this instance.
        /// The username and password must be one that can create databases and connect to the master database of the server.
        /// </summary>
        /// <returns></returns>
        public static bool DeleteDatabase<I>(this I e) where I : IDataConnection
        {
            return e
                .GetFactory()
                .DeleteDatabase(e);
        }

        /// <summary>
        /// Gets the int32 value of the db enum type based on the database <see cref="IDataConnection"/>
        /// </summary>
        /// <param name="e"></param>
        /// <param name="t"></param>
        /// <returns></returns>
        public static int GetDbDataType<I>(this I e, Type t) where I : IDataConnection
        {
            return e
                .GetFactory()
                .GetDbDataType(e, t);
        }

        /// <summary>
        /// Gets the sql representation of the build and produces the list of <see cref="DbParameter"/> based on the <see cref="IDataConnection"/>
        /// </summary>
        /// <param name="e"></param>
        /// <param name="XXDbType"></param>
        /// <param name="value"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public static string GetSqlRepresentation<I>(this I e, int XXDbType, object value, out IEnumerable<DbParameter> parameters) where I : IDataConnection
        {
            parameters = Array.Empty<DbParameter>();

            string output = e
                .GetFactory()
                .GetSqlRepresentation(e, XXDbType, value, out IEnumerable<DbParameter> pta);

            parameters = parameters.Concat(pta);

            return output;

        }

        /// <summary>
        /// Gets the sql representation of the build and produces the list of <see cref="DbParameter"/> based on the <see cref="IDataConnection"/>
        /// </summary>
        /// <param name="e"></param>
        /// <param name="p"></param>
        /// <param name="value"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public static string GetSqlRepresentation<I>(this I e, PropertyInfo p, object value, out IEnumerable<DbParameter> parameters) where I : IDataConnection
        {

            parameters = Array.Empty<DbParameter>();

            string output = e
                .GetFactory()
                .GetSqlRepresentation(e, p, value, out IEnumerable<DbParameter> pta);

            parameters = parameters.Concat(pta);

            return output;
        }

        /// <summary>
        /// Gets the default string values based on table creating in sql
        /// </summary>
        /// <param name="e"></param>
        /// <param name="DataType"></param>
        /// <param name="Encapsulate"></param>
        /// <returns></returns>
        public static string GetSqlDefaultValueRepresentation<I>(this I e, int DataType, bool Encapsulate = false) where I : IDataConnection
        {
            return e
                .GetFactory()
                .GetSqlDefaultValueRepresentation(DataType, Encapsulate);
        }
        
        /// <summary>
        /// Returns the <see cref="ISqlFactory"/> based on the connection
        /// </summary>
        /// <typeparam name="I"></typeparam>
        /// <param name="e"></param>
        /// <returns></returns>
        public static ISqlFactory GetFactory<I>(this I e) where I : IDataConnection
        {
            return ODataContext
                .GetFactory(e.GetDbEnumType());
        }

        /// <summary>
        /// Creates a db parameter based on the connection and type
        /// </summary>
        /// <param name="e"></param>
        /// <param name="datatype"></param>
        /// <param name="value"></param>
        /// <param name="paramname"></param>
        /// <returns></returns>
        public static DbParameter CreateParam<I>(this I e, int datatype, object value, string paramname) where I : IDataConnection
        {
            return e
                .GetFactory()
                .CreateParam(datatype, value, paramname);
        }

        /// <summary>
        /// Creates a db parameter based on the connection and type with direction
        /// </summary>
        /// <param name="e"></param>
        /// <param name="datatype"></param>
        /// <param name="direction"></param>
        /// <param name="value"></param>
        /// <param name="paramname"></param>
        /// <returns></returns>
        public static DbParameter CreateParam<I>(this I e, int datatype, ParameterDirection direction, object value, string paramname) where I : IDataConnection
        {
            return e
                .GetFactory()
                .CreateParam(datatype, direction, value, paramname);
        }

        /// <summary>
        /// Creates a db connection based on the connection and type
        /// </summary>
        /// <typeparam name="I"></typeparam>
        /// <param name="e"></param>
        /// <returns></returns>
        public static DbConnection GetNewConnection<I>(this I e) where I : IDataConnection
        {
            return e
                .GetFactory()
                .GetNewConnection(e);
        }

        /// <summary>
        /// Creates a db command based on the connection and type
        /// </summary
        /// <typeparam name="I"></typeparam>
        /// <param name="e"></param>
        /// <param name="cmdText"></param>
        /// <returns></returns>
        public static DbCommand GetNewCommand<I>(this I e, string cmdText) where I : IDataConnection
        {
            return e
                .GetFactory()
                .GetNewCommand(cmdText);
        }

        /// <summary>
        /// Creates a db data adaptor based on the connection and type
        /// </summary>
        /// <typeparam name="I"></typeparam>
        /// <param name="e"></param>
        /// <returns></returns>
        public static DbDataAdapter GetNewDataAdapter<I>(this I e) where I : IDataConnection
        {
            return e
                .GetFactory()
                .GetNewDataAdapter();
        }

        /// <summary>
        /// Run a query on the connection with or with out params
        /// </summary>
        /// <typeparam name="I">Where I is of type <see cref="IDataConnection"/></typeparam>
        /// <param name="e"></param>
        /// <param name="type"></param>
        /// <param name="Sql"></param>
        /// <param name="Parameters"></param>
        /// <returns>The data set from the query reader.</returns>
        public static DataSet Get<I>(this I e, CommandType type, string Sql, DbParameter[] Parameters) where I : IDataConnection
        {
            try
            {

                using var dbconnection   = e.GetNewConnection();
                using var dbcommand      = e.GetNewCommand(Sql);
                using var dbadaptor      = e.GetNewDataAdapter();
                dbcommand.Connection     = dbconnection;
                dbcommand.CommandTimeout = 10000;
                dbcommand.CommandType    = type;

                if (Parameters != null && Parameters.Length > 0)
                    dbcommand.Parameters.AddRange(Parameters);

                dbadaptor.SelectCommand = dbcommand;

                var tempdataset = new DataSet();

                dbadaptor.Fill(tempdataset);

                return tempdataset;

            }
            catch(Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// Run a query on the connection with or with out params
        /// </summary>
        /// <typeparam name="I">Where I is of type <see cref="IDataConnection"/></typeparam>
        /// <param name="e"></param>
        /// <param name="type"></param>
        /// <param name="Sql"></param>
        /// <param name="Parameters"></param>
        /// <param name="ex">Any <see cref="Exception"/> if there is an error</param>
        /// <returns>The data set from the query reader.</returns>
        public static DataSet Get<I>(this I e, CommandType type, string Sql, DbParameter[] Parameters, out Exception ex) where I : IDataConnection
        {
            try
            {
                ex = null;

                using var dbconnection   = e.GetNewConnection();
                using var dbcommand      = e.GetNewCommand(Sql);
                using var dbadaptor      = e.GetNewDataAdapter();
                dbcommand.Connection     = dbconnection;
                dbcommand.CommandTimeout = 10000;
                dbcommand.CommandType    = type;

                if (Parameters != null && Parameters.Length > 0)
                    dbcommand.Parameters.AddRange(Parameters);

                dbadaptor.SelectCommand = dbcommand;

                var tempdataset = new DataSet();

                dbadaptor.Fill(tempdataset);

                return tempdataset;

            }
            catch (Exception error)
            {
                ex = error;
                return null;
            }

        }

        /// <summary>
        /// Run a query on the connection with or with out params
        /// </summary>
        /// <typeparam name="I">Where I is of type <see cref="IDataConnection"/></typeparam>
        /// <param name="e"></param>
        /// <param name="type"></param>
        /// <param name="Sql"></param>
        /// <param name="Parameters"></param>
        /// <param name="sqltimout"></param>
        /// <returns>The data set from the query reader.</returns>
        public static DataSet Get<I>(this I e, CommandType type, string Sql, DbParameter[] Parameters, int sqltimout = 500) where I : IDataConnection
        {
            try
            {

                using var dbconnection   = e.GetNewConnection();
                using var dbcommand      = e.GetNewCommand(Sql);
                using var dbadaptor      = e.GetNewDataAdapter();
                dbcommand.Connection     = dbconnection;
                dbcommand.CommandTimeout = sqltimout;
                dbcommand.CommandType    = type;

                if (Parameters != null && Parameters.Length > 0)
                    dbcommand.Parameters.AddRange(Parameters);

                dbadaptor.SelectCommand = dbcommand;

                var tempdataset = new DataSet();

                dbadaptor.Fill(tempdataset);

                return tempdataset;

            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Run a query on the connection with or with out params
        /// </summary>
        /// <typeparam name="I">Where I is of type <see cref="IDataConnection"/></typeparam>
        /// <param name="e"></param>
        /// <param name="type"></param>
        /// <param name="Sql"></param>
        /// <param name="Parameters"></param>
        /// <param name="ex">Any <see cref="Exception"/> if there is an error</param>
        /// <param name="sqltimout"></param>
        /// <returns>The data set from the query reader.</returns>
        public static DataSet Get<I>(this I e, CommandType type, string Sql, DbParameter[] Parameters, out Exception ex, int sqltimout = 500) where I : IDataConnection
        {
            try
            {
                ex = null;

                using var dbconnection   = e.GetNewConnection();
                using var dbcommand      = e.GetNewCommand(Sql);
                using var dbadaptor      = e.GetNewDataAdapter();
                dbcommand.Connection     = dbconnection;
                dbcommand.CommandTimeout = sqltimout;
                dbcommand.CommandType    = type;

                if (Parameters != null && Parameters.Length > 0)
                    dbcommand.Parameters.AddRange(Parameters);

                dbadaptor.SelectCommand = dbcommand;

                var tempdataset = new DataSet();

                dbadaptor.Fill(tempdataset);

                return tempdataset;

            }
            catch (Exception error)
            {
                ex = error;
                return null;
            }

        }

        /// <summary>
        /// Run a "ExecuteNonQuery" on the connection with or with out params
        /// </summary>
        /// <typeparam name="I">Where I is of type <see cref="IDataConnection"/></typeparam>
        /// <param name="e"></param>
        /// <param name="type"></param>
        /// <param name="Sql"></param>
        /// <param name="Parameters"></param>
        /// <returns>The state of the success from the query</returns>
        public static bool Query<I>(this I e, CommandType type, string Sql, DbParameter[] Parameters) where I : IDataConnection
        {
            try
            {

                using var dbconnection   = e.GetNewConnection();
                using var dbcommand      = e.GetNewCommand(Sql);
                dbcommand.Connection     = dbconnection;
                dbcommand.CommandType    = type;
                
                dbconnection.Open();
              
                if (Parameters != null && Parameters.Length > 0)
                    dbcommand.Parameters.AddRange(Parameters);
               
                dbcommand.ExecuteNonQuery();

                return true;

            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Run a "ExecuteNonQuery" on the connection with or with out params
        /// </summary>
        /// <typeparam name="I">Where I is of type <see cref="IDataConnection"/></typeparam>
        /// <param name="e"></param>
        /// <param name="type"></param>
        /// <param name="Sql"></param>
        /// <param name="Parameters"></param>
        /// <param name="ex">Any <see cref="Exception"/> if there is an error</param>
        /// <returns>The state of the success from the query</returns>
        public static bool Query<I>(this I e, CommandType type, string Sql, DbParameter[] Parameters, out Exception ex) where I : IDataConnection
        {
            ex = null;

            try
            {

                using var dbconnection   = e.GetNewConnection();
                using var dbcommand      = e.GetNewCommand(Sql);
                dbcommand.Connection     = dbconnection;
                dbcommand.CommandType    = type;
                
                dbconnection.Open();
              
                if (Parameters != null && Parameters.Length > 0)
                    dbcommand.Parameters.AddRange(Parameters);
               
                dbcommand.ExecuteNonQuery();

                return true;

            }
            catch (Exception error)
            {
                ex = error;
                return false;
            }
        }

        /// <summary>
        /// Run a "ExecuteNonQuery" on the connection with or with out params
        /// </summary>
        /// <typeparam name="I">Where I is of type <see cref="IDataConnection"/></typeparam>
        /// <param name="e"></param>
        /// <param name="type"></param>
        /// <param name="Sql"></param>
        /// <param name="Parameters"></param>
        /// <param name="sqltimout"></param>
        /// <returns>The state of the success from the query</returns>
        public static bool Query<I>(this I e, CommandType type, string Sql, DbParameter[] Parameters, int sqltimout = 500) where I : IDataConnection
        {
            try
            {

                using var dbconnection      = e.GetNewConnection();
                using var dbcommand         = e.GetNewCommand(Sql);
                dbcommand.Connection        = dbconnection;
                dbcommand.CommandType       = type;
                dbcommand.CommandTimeout    = sqltimout;

                dbconnection.Open();

                if (Parameters != null && Parameters.Length > 0)
                    dbcommand.Parameters.AddRange(Parameters);

                dbcommand.ExecuteNonQuery();

                return true;

            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Run a "ExecuteNonQuery" on the connection with or with out params
        /// </summary>
        /// <typeparam name="I">Where I is of type <see cref="IDataConnection"/></typeparam>
        /// <param name="e"></param>
        /// <param name="type"></param>
        /// <param name="Sql"></param>
        /// <param name="Parameters"></param>
        /// <param name="ex">Any <see cref="Exception"/> if there is an error</param>
        /// <param name="sqltimout"></param>
        /// <returns>The state of the success from the query</returns>
        public static bool Query<I>(this I e, CommandType type, string Sql, DbParameter[] Parameters, out Exception ex, int sqltimout = 500) where I : IDataConnection
        {
            ex = null;

            try
            {
                using var dbconnection      = e.GetNewConnection();
                using var dbcommand         = e.GetNewCommand(Sql);
                dbcommand.Connection        = dbconnection;
                dbcommand.CommandType       = type;
                dbcommand.CommandTimeout    = sqltimout;

                dbconnection.Open();

                if (Parameters != null && Parameters.Length > 0)
                    dbcommand.Parameters.AddRange(Parameters);

                dbcommand.ExecuteNonQuery();

                return true;

            }
            catch (Exception error)
            {
                ex = error;
                return false;
            }
        }


    }

}
