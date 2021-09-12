/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.SqlClient;
using System.IO;
using System.Data.OleDb;
using System.Reflection;
using System.Text;
using System.Linq;
using System.Collections.Generic;

using Newtonsoft.Json.Linq;

using Microsoft.Reporting.NETCore;
using Microsoft.VisualBasic.FileIO;
using Microsoft.VisualBasic;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using ExcelDataReader;

using K2host.Core;
using K2host.Data.Enums;
using K2host.Data.Classes;

namespace K2host.Data
{
    
    public static class OHelpers
    {

        #region MSSQL Parameter Builders

        public static SqlParameter ParamMsSql(DbType datatype, object value, string paramname)
        {
            try
            {
                SqlParameter result = new()
                {
                    DbType = datatype,
                    Value = value,
                    ParameterName = paramname
                };

                return result;

            }
            catch
            {
                return null;
            }
        }

        public static SqlParameter ParamMsSql(DbType datatype, ParameterDirection direction, object value, string paramname)
        {
            try
            {
                SqlParameter result = new()
                {
                    DbType = datatype,
                    Value = value,
                    ParameterName = paramname,
                    Direction = direction
                };

                return result;
            }
            catch
            {
                return null;
            }
        }

        public static SqlParameter ParamMsSql(SqlDbType datatype, object value, string paramname)
        {
            try
            {
                SqlParameter temp = new()
                {
                    SqlDbType = datatype,
                    Value = value,
                    ParameterName = paramname
                };
                return temp;
            }
            catch
            {
                return null;
            }
        }

        public static SqlParameter ParamMsSql(SqlDbType datatype, ParameterDirection direction, object value, string paramname)
        {
            try
            {
                SqlParameter temp = new()
                {
                    SqlDbType = datatype,
                    Value = value,
                    ParameterName = paramname,
                    Direction = direction
                };
                return temp;
            }
            catch
            {
                return null;
            }
        }

        #endregion

        #region Odbc Parameter Builders

        public static OdbcParameter ParamOdbc(DbType datatype, object value, string paramname)
        {
            try
            {

                OdbcParameter temp = new()
                {
                    DbType = datatype,
                    Value = value,
                    ParameterName = paramname
                };

                return temp;
            }
            catch
            {
                return null;
            }
        }

        public static OdbcParameter ParamOdbc(DbType datatype, ParameterDirection direction, object value, string paramname)
        {
            try
            {
                OdbcParameter temp = new()
                {
                    DbType = datatype,
                    Value = value,
                    ParameterName = paramname,
                    Direction = direction
                };
                return temp;
            }
            catch
            {
                return null;
            }
        }

        #endregion

        #region MySql Parameter Builders

        public static MySqlParameter ParamMySql(MySqlDbType datatype, object value, string paramname)
        {
            try
            {

                MySqlParameter temp = new()
                {
                    MySqlDbType = datatype,
                    Value = value,
                    ParameterName = paramname
                };

                return temp;
            }
            catch
            {
                return null;
            }
        }

        public static MySqlParameter ParamMySql(MySqlDbType datatype, ParameterDirection direction, object value, string paramname)
        {
            try
            {
                MySqlParameter temp = new()
                {
                    MySqlDbType = datatype,
                    Value = value,
                    ParameterName = paramname,
                    Direction = direction
                };
                return temp;
            }
            catch
            {
                return null;
            }
        }

        #endregion

        #region Oracle Parameter Builders

        public static OracleParameter ParamOracle(OracleDbType datatype, object value, string paramname)
        {
            try
            {

                OracleParameter temp = new()
                {
                    OracleDbType = datatype,
                    Value = value,
                    ParameterName = paramname
                };

                return temp;
            }
            catch
            {
                return null;
            }
        }

        public static OracleParameter ParamOracle(OracleDbType datatype, ParameterDirection direction, object value, string paramname)
        {
            try
            {
                OracleParameter temp = new()
                {
                    OracleDbType = datatype,
                    Value = value,
                    ParameterName = paramname,
                    Direction = direction
                };
                return temp;
            }
            catch
            {
                return null;
            }
        }

        #endregion

        #region DataSet Builders

        public static DataSet Get(string StoredProc, DbParameter[] Parameters, string ConnectionString)
        {
            try
            {

                using var tempconnection = new SqlConnection(ConnectionString);
                using var tempcommand = new SqlCommand(StoredProc);
                using var tempadaptor = new SqlDataAdapter();
                tempcommand.Connection = tempconnection;
                tempcommand.CommandTimeout = 10000;
                tempcommand.CommandType = CommandType.StoredProcedure;

                if (Parameters != null)
                    tempcommand.Parameters.AddRange(Parameters);

                tempadaptor.SelectCommand = tempcommand;

                var tempdataset = new DataSet();

                tempadaptor.Fill(tempdataset);

                return tempdataset;

            }
            catch (Exception)
            {
                return null;
            }


        }

        public static DataSet Get(string StoredProc, DbParameter[] Parameters, OConnection Connection, OConnectionType ConnectionType)
        {
            try
            {

                string ConnectionString = Connection.ToString(ConnectionType);

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(StoredProc))
                        using (var tempadaptor = new SqlDataAdapter())
                        {
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = 10000;
                            tempcommand.CommandType = CommandType.StoredProcedure;

                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);

                            tempadaptor.SelectCommand = tempcommand;

                            var tempdataset = new DataSet();

                            tempadaptor.Fill(tempdataset);

                            return tempdataset;
                        }

                    case OConnectionType.OracleStandardSecurity:
                    case OConnectionType.OracleCredentialsSecurity:

                        using (var tempconnection = new OracleConnection(ConnectionString))
                        using (var tempcommand = new OracleCommand(StoredProc))
                        using (var tempadaptor = new OracleDataAdapter())
                        {
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = 10000;
                            tempcommand.CommandType = CommandType.StoredProcedure;

                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);

                            tempadaptor.SelectCommand = tempcommand;

                            var tempdataset = new DataSet();

                            tempadaptor.Fill(tempdataset);

                            return tempdataset;
                        }

                    case OConnectionType.MySqlStandardSecurity:

                        using (var tempconnection = new MySqlConnection(ConnectionString))
                        using (var tempcommand = new MySqlCommand(StoredProc))
                        using (var tempadaptor = new MySqlDataAdapter())
                        {
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = 10000;
                            tempcommand.CommandType = CommandType.StoredProcedure;

                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);

                            tempadaptor.SelectCommand = tempcommand;

                            var tempdataset = new DataSet();

                            tempadaptor.Fill(tempdataset);

                            return tempdataset;
                        }

                }

                return null;

            }
            catch
            {
                return null;
            }


        }

        public static DataSet Get(string StoredProc, DbParameter[] Parameters, string ConnectionString, out Exception ex)
        {
            try
            {
                ex = null;

                using var tempconnection = new SqlConnection(ConnectionString);
                using var tempcommand = new SqlCommand(StoredProc);
                using var tempadaptor = new SqlDataAdapter();
                tempcommand.Connection = tempconnection;
                tempcommand.CommandTimeout = 10000;
                tempcommand.CommandType = CommandType.StoredProcedure;

                if (Parameters != null)
                    tempcommand.Parameters.AddRange(Parameters);

                tempadaptor.SelectCommand = tempcommand;

                var tempdataset = new DataSet();

                tempadaptor.Fill(tempdataset);

                return tempdataset;

            }
            catch (Exception error)
            {
                ex = error;
                return null;
            }


        }

        public static DataSet Get(string StoredProc, DbParameter[] Parameters, OConnection Connection, OConnectionType ConnectionType, out Exception ex)
        {
            try
            {
                ex = null;

                string ConnectionString = Connection.ToString(ConnectionType);

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(StoredProc))
                        using (var tempadaptor = new SqlDataAdapter())
                        {
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = 10000;
                            tempcommand.CommandType = CommandType.StoredProcedure;

                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);

                            tempadaptor.SelectCommand = tempcommand;

                            var tempdataset = new DataSet();

                            tempadaptor.Fill(tempdataset);

                            return tempdataset;
                        }

                    case OConnectionType.OracleStandardSecurity:
                    case OConnectionType.OracleCredentialsSecurity:

                        using (var tempconnection = new OracleConnection(ConnectionString))
                        using (var tempcommand = new OracleCommand(StoredProc))
                        using (var tempadaptor = new OracleDataAdapter())
                        {
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = 10000;
                            tempcommand.CommandType = CommandType.StoredProcedure;

                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);

                            tempadaptor.SelectCommand = tempcommand;

                            var tempdataset = new DataSet();

                            tempadaptor.Fill(tempdataset);

                            return tempdataset;
                        }

                    case OConnectionType.MySqlStandardSecurity:
                        using (var tempconnection = new MySqlConnection(ConnectionString))
                        using (var tempcommand = new MySqlCommand(StoredProc))
                        using (var tempadaptor = new MySqlDataAdapter())
                        {
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = 10000;
                            tempcommand.CommandType = CommandType.StoredProcedure;

                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);

                            tempadaptor.SelectCommand = tempcommand;

                            var tempdataset = new DataSet();

                            tempadaptor.Fill(tempdataset);

                            return tempdataset;
                        }

                }

                return null;

            }
            catch (Exception error)
            {
                ex = error;
                return null;
            }


        }

        public static DataSet Get(string Sql, string ConnectionString)
        {
            try
            {
                using var tempconnection    = new SqlConnection(ConnectionString);
                using var tempcommand       = new SqlCommand(Sql);
                using var tempadaptor       = new SqlDataAdapter();
                tempcommand.CommandTimeout  = 500;
                tempcommand.Connection      = tempconnection;
                tempadaptor.SelectCommand   = tempcommand;

                var tempdataset = new DataSet();

                tempadaptor.Fill(tempdataset);

                return tempdataset;
            }
            catch
            {
                return null;
            }
        }

        public static DataSet Get(string Sql, OConnection Connection, OConnectionType ConnectionType)
        {
            try
            {

                string ConnectionString = Connection.ToString(ConnectionType);

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(Sql))
                        using (var tempadaptor = new SqlDataAdapter())
                        {
                            tempcommand.CommandTimeout = 500;
                            tempcommand.Connection = tempconnection;
                            tempadaptor.SelectCommand = tempcommand;

                            var tempdataset = new DataSet();

                            tempadaptor.Fill(tempdataset);

                            return tempdataset;
                        }

                    case OConnectionType.OracleStandardSecurity:
                    case OConnectionType.OracleCredentialsSecurity:

                        using (var tempconnection = new OracleConnection(ConnectionString))
                        using (var tempcommand = new OracleCommand(Sql))
                        using (var tempadaptor = new OracleDataAdapter())
                        {
                            tempcommand.CommandTimeout = 500;
                            tempcommand.Connection = tempconnection;
                            tempadaptor.SelectCommand = tempcommand;

                            var tempdataset = new DataSet();

                            tempadaptor.Fill(tempdataset);

                            return tempdataset;
                        }

                    case OConnectionType.MySqlStandardSecurity:

                        using (var tempconnection = new MySqlConnection(ConnectionString))
                        using (var tempcommand = new MySqlCommand(Sql))
                        using (var tempadaptor = new MySqlDataAdapter())
                        {
                            tempcommand.CommandTimeout = 500;
                            tempcommand.Connection = tempconnection;
                            tempadaptor.SelectCommand = tempcommand;

                            var tempdataset = new DataSet();

                            tempadaptor.Fill(tempdataset);

                            return tempdataset;
                        }

                }

                return null;

            }
            catch
            {
                return null;
            }

        }

        public static DataSet Get(string Sql, string ConnectionString, out Exception ex)
        {
            try
            {
                ex = null;

                using var tempconnection = new SqlConnection(ConnectionString);
                using var tempcommand = new SqlCommand(Sql);
                using var tempadaptor = new SqlDataAdapter();
                tempcommand.CommandTimeout = 500;
                tempcommand.Connection = tempconnection;
                tempadaptor.SelectCommand = tempcommand;

                var tempdataset = new DataSet();

                tempadaptor.Fill(tempdataset);

                return tempdataset;
            }
            catch (Exception error)
            {
                ex = error;
                return null;
            }
        }

        public static DataSet Get(string Sql, OConnection Connection, OConnectionType ConnectionType, out Exception ex)
        {
            try
            {
                ex = null;

                string ConnectionString = Connection.ToString(ConnectionType);

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(Sql))
                        using (var tempadaptor = new SqlDataAdapter())
                        {
                            tempcommand.CommandTimeout = 500;
                            tempcommand.Connection = tempconnection;
                            tempadaptor.SelectCommand = tempcommand;

                            var tempdataset = new DataSet();

                            tempadaptor.Fill(tempdataset);

                            return tempdataset;
                        }

                    case OConnectionType.OracleStandardSecurity:
                    case OConnectionType.OracleCredentialsSecurity:

                        using (var tempconnection = new OracleConnection(ConnectionString))
                        using (var tempcommand = new OracleCommand(Sql))
                        using (var tempadaptor = new OracleDataAdapter())
                        {
                            tempcommand.CommandTimeout = 500;
                            tempcommand.Connection = tempconnection;
                            tempadaptor.SelectCommand = tempcommand;

                            var tempdataset = new DataSet();

                            tempadaptor.Fill(tempdataset);

                            return tempdataset;
                        }

                    case OConnectionType.MySqlStandardSecurity:

                        using (var tempconnection = new MySqlConnection(ConnectionString))
                        using (var tempcommand = new MySqlCommand(Sql))
                        using (var tempadaptor = new MySqlDataAdapter())
                        {
                            tempcommand.CommandTimeout = 500;
                            tempcommand.Connection = tempconnection;
                            tempadaptor.SelectCommand = tempcommand;

                            var tempdataset = new DataSet();

                            tempadaptor.Fill(tempdataset);

                            return tempdataset;
                        }


                }

                return null;

            }
            catch (Exception error)
            {
                ex = error;
                return null;
            }
        }
        
        public static DataSet Get(string Sql, string ConnectionString, int sqltimout = 500)
        {
            try
            {

                using var tempconnection = new System.Data.Odbc.OdbcConnection(ConnectionString);
                using var tempcommand = new System.Data.Odbc.OdbcCommand();
                using var tempadaptor = new System.Data.Odbc.OdbcDataAdapter();
                tempcommand.CommandTimeout = sqltimout;
                tempcommand.Connection = tempconnection;
                tempcommand.CommandText = Sql;

                tempadaptor.SelectCommand = tempcommand;

                DataSet tempdataset = new();

                tempconnection.Open();

                tempadaptor.Fill(tempdataset);

                return tempdataset;

            }
            catch
            {
                return null;
            }
        }

        public static DataSet Get(string Sql, OConnection Connection, OConnectionType ConnectionType, int sqltimout = 500)
        {
            try
            {

                string ConnectionString = Connection.ToString(ConnectionType);

                using var tempconnection = new System.Data.Odbc.OdbcConnection(ConnectionString);
                using var tempcommand = new System.Data.Odbc.OdbcCommand();
                using var tempadaptor = new System.Data.Odbc.OdbcDataAdapter();
                tempcommand.CommandTimeout = sqltimout;
                tempcommand.Connection = tempconnection;
                tempcommand.CommandText = Sql;

                tempadaptor.SelectCommand = tempcommand;

                DataSet tempdataset = new();

                tempconnection.Open();

                tempadaptor.Fill(tempdataset);

                return tempdataset;

            }
            catch
            {
                return null;
            }
        }

        public static DataSet Get(string StoredProc, OdbcParameter[] Parameters, string ConnectionString, int sqltimout = 500)
        {
            try
            {

                using var tempconnection = new System.Data.Odbc.OdbcConnection(ConnectionString);
                using var tempcommand = new System.Data.Odbc.OdbcCommand(StoredProc);
                using var tempadaptor = new System.Data.Odbc.OdbcDataAdapter();
                tempcommand.CommandTimeout = sqltimout;
                tempcommand.Connection = tempconnection;
                tempcommand.CommandType = CommandType.StoredProcedure;

                if (Parameters != null)
                    tempcommand.Parameters.AddRange(Parameters);

                tempadaptor.SelectCommand = tempcommand;

                DataSet tempdataset = new();

                tempconnection.Open();

                tempadaptor.Fill(tempdataset);

                return tempdataset;

            }
            catch
            {
                return null;
            }
        }

        public static DataSet Get(string StoredProc, OdbcParameter[] Parameters, OConnection Connection, OConnectionType ConnectionType, int sqltimout = 500)
        {
            try
            {

                string ConnectionString = Connection.ToString(ConnectionType);

                using var tempconnection = new System.Data.Odbc.OdbcConnection(ConnectionString);
                using var tempcommand = new System.Data.Odbc.OdbcCommand(StoredProc);
                using var tempadaptor = new System.Data.Odbc.OdbcDataAdapter();
                tempcommand.CommandTimeout = sqltimout;
                tempcommand.Connection = tempconnection;
                tempcommand.CommandType = CommandType.StoredProcedure;

                if (Parameters != null)
                    tempcommand.Parameters.AddRange(Parameters);

                tempadaptor.SelectCommand = tempcommand;

                DataSet tempdataset = new();

                tempconnection.Open();

                tempadaptor.Fill(tempdataset);

                return tempdataset;

            }
            catch
            {
                return null;
            }
        }

        public static bool Clear(DataSet e)
        {
            try
            {
                e.Clear();
                e.Tables.Clear();
                e.Dispose();
                e = null;
                return true;
            }
            catch
            {
                return false;
            }
        }
      
        public static ODataFieldSet[] GetFieldSets(this Type obj, ODataFieldSetType e)
        {
            List<ODataFieldSet> output = new();

            ODataExceptionType NonInteract = e switch
            {
                ODataFieldSetType.SELECT => ODataExceptionType.NON_SELECT,
                ODataFieldSetType.INSERT => ODataExceptionType.NON_INSERT,
                ODataFieldSetType.UPDATE => ODataExceptionType.NON_UPDATE,
                _ => ODataExceptionType.NON_SELECT,
            };

            foreach (PropertyInfo p in obj.GetProperties())
                if (p.GetCustomAttributes(typeof(TSQLDataException), true).Length > 0)
                {
                    if (!((TSQLDataException)p.GetCustomAttributes(typeof(TSQLDataException), true)[0]).ODataExceptionType.HasFlag(NonInteract))
                        output.Add(
                            new ODataFieldSet()
                            {
                                Column = p,
                                DataType = GetSqlDbType(p.PropertyType)
                            });

                }
                else
                    output.Add(
                        new ODataFieldSet()
                        {
                            Column = p,
                            DataType = GetSqlDbType(p.PropertyType)
                        });

            return output.ToArray();
        }

        #endregion

        #region Query Runners

        public static bool Query(string StoredProc, SqlParameter[] Parameters, string ConnectionString)
        {
            try
            {
                using var tempconnection = new SqlConnection(ConnectionString);
                using var tempcommand = new SqlCommand(StoredProc);
                tempcommand.CommandType = CommandType.StoredProcedure;
                tempcommand.Connection = tempconnection;
                tempconnection.Open();
                if (Parameters != null)
                    tempcommand.Parameters.AddRange(Parameters);
                tempcommand.ExecuteNonQuery();
                return true;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static bool Query(string StoredProc, DbParameter[] Parameters, OConnection Connection, OConnectionType ConnectionType)
        {
            try
            {

                string ConnectionString = Connection.ToString(ConnectionType);

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(StoredProc))
                        {
                            tempcommand.CommandType = CommandType.StoredProcedure;
                            tempcommand.Connection = tempconnection;
                            tempconnection.Open();
                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);
                            tempcommand.ExecuteNonQuery();
                            return true;
                        }

                    case OConnectionType.OracleStandardSecurity:
                    case OConnectionType.OracleCredentialsSecurity:

                        using (var tempconnection = new OracleConnection(ConnectionString))
                        using (var tempcommand = new OracleCommand(StoredProc))
                        {
                            tempcommand.CommandType = CommandType.StoredProcedure;
                            tempcommand.Connection = tempconnection;
                            tempconnection.Open();
                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);
                            tempcommand.ExecuteNonQuery();
                            return true;
                        }

                    case OConnectionType.MySqlStandardSecurity:

                        using (var tempconnection = new MySqlConnection(ConnectionString))
                        using (var tempcommand = new MySqlCommand(StoredProc))
                        {
                            tempcommand.CommandType = CommandType.StoredProcedure;
                            tempcommand.Connection = tempconnection;
                            tempconnection.Open();
                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);
                            tempcommand.ExecuteNonQuery();
                            return true;
                        }

                }

                return false;

            }
            catch (Exception)
            {
                throw;
            }
        }

        public static bool Query(string StoredProc, SqlParameter[] Parameters, string ConnectionString, int sqltimout = 500)
        {
            try
            {
                using var tempconnection = new SqlConnection(ConnectionString);
                using var tempcommand = new SqlCommand(StoredProc);
                tempcommand.CommandType = CommandType.StoredProcedure;
                tempcommand.CommandTimeout = sqltimout;
                tempcommand.Connection = tempconnection;
                tempconnection.Open();
                if (Parameters != null)
                    tempcommand.Parameters.AddRange(Parameters);
                tempcommand.ExecuteNonQuery();
                return true;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static bool Query(string StoredProc, DbParameter[] Parameters, OConnection Connection, OConnectionType ConnectionType, int sqltimout = 500)
        {
            try
            {

                string ConnectionString = Connection.ToString(ConnectionType);

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(StoredProc))
                        {
                            tempcommand.CommandType = CommandType.StoredProcedure;
                            tempcommand.CommandTimeout = sqltimout;
                            tempcommand.Connection = tempconnection;
                            tempconnection.Open();
                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);
                            tempcommand.ExecuteNonQuery();
                            return true;
                        }

                    case OConnectionType.OracleStandardSecurity:
                    case OConnectionType.OracleCredentialsSecurity:

                        using (var tempconnection = new OracleConnection(ConnectionString))
                        using (var tempcommand = new OracleCommand(StoredProc))
                        {
                            tempcommand.CommandType = CommandType.StoredProcedure;
                            tempcommand.CommandTimeout = sqltimout;
                            tempcommand.Connection = tempconnection;
                            tempconnection.Open();
                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);
                            tempcommand.ExecuteNonQuery();
                            return true;
                        }

                    case OConnectionType.MySqlStandardSecurity:

                        using (var tempconnection = new MySqlConnection(ConnectionString))
                        using (var tempcommand = new MySqlCommand(StoredProc))
                        {
                            tempcommand.CommandType = CommandType.StoredProcedure;
                            tempcommand.CommandTimeout = sqltimout;
                            tempcommand.Connection = tempconnection;
                            tempconnection.Open();
                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);
                            tempcommand.ExecuteNonQuery();
                            return true;
                        }

                }

                return false;

            }
            catch (Exception)
            {
                throw;
            }
        }

        public static bool Query(string Sql, string ConnectionString, int sqltimout = 500)
        {
            try
            {
                using var tempconnection = new SqlConnection(ConnectionString);
                using var tempcommand = new SqlCommand(Sql);
                tempcommand.CommandTimeout = sqltimout;
                tempcommand.Connection = tempconnection;
                tempconnection.Open();
                tempcommand.ExecuteNonQuery();
                return true;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public static bool Query(string Sql, OConnection Connection, OConnectionType ConnectionType, int sqltimout = 500)
        {
            try
            {

                string ConnectionString = Connection.ToString(ConnectionType);

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(Sql))
                        {
                            tempcommand.CommandTimeout = sqltimout;
                            tempcommand.Connection = tempconnection;
                            tempconnection.Open();
                            tempcommand.ExecuteNonQuery();
                            return true;
                        }

                    case OConnectionType.OracleStandardSecurity:
                    case OConnectionType.OracleCredentialsSecurity:

                        using (var tempconnection = new OracleConnection(ConnectionString))
                        using (var tempcommand = new OracleCommand(Sql))
                        {
                            tempcommand.CommandTimeout = sqltimout;
                            tempcommand.Connection = tempconnection;
                            tempconnection.Open();
                            tempcommand.ExecuteNonQuery();
                            return true;
                        }

                    case OConnectionType.MySqlStandardSecurity:

                        using (var tempconnection = new MySqlConnection(ConnectionString))
                        using (var tempcommand = new MySqlCommand(Sql))
                        {
                            tempcommand.CommandTimeout = sqltimout;
                            tempcommand.Connection = tempconnection;
                            tempconnection.Open();
                            tempcommand.ExecuteNonQuery();
                            return true;
                        }

                }

                return false;

            }
            catch (Exception)
            {
                throw;
            }
        }
      
        public static bool Query(string Sql, string ConnectionString, out Exception ex)
        {
            ex = null;
            try
            {
                using var tempconnection = new SqlConnection(ConnectionString);
                using var tempcommand = new SqlCommand(Sql);
                tempcommand.CommandTimeout = 500;
                tempcommand.Connection = tempconnection;
                tempconnection.Open();
                tempcommand.ExecuteNonQuery();
                return true;
            }
            catch (Exception error)
            {
                ex = error;
                return false;
            }
        }

        public static bool Query(string Sql, OConnection Connection, OConnectionType ConnectionType, out Exception ex)
        {
            ex = null;

            try
            {

                string ConnectionString = Connection.ToString(ConnectionType);

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(Sql))
                        {
                            tempcommand.CommandTimeout = 500;
                            tempcommand.Connection = tempconnection;
                            tempconnection.Open();
                            tempcommand.ExecuteNonQuery();
                            return true;
                        }

                    case OConnectionType.OracleStandardSecurity:
                    case OConnectionType.OracleCredentialsSecurity:

                        using (var tempconnection = new OracleConnection(ConnectionString))
                        using (var tempcommand = new OracleCommand(Sql))
                        {

                            tempcommand.CommandTimeout = 500;
                            tempcommand.Connection = tempconnection;
                            tempconnection.Open(); ;
                            tempcommand.ExecuteNonQuery();
                            return true;
                        }

                    case OConnectionType.MySqlStandardSecurity:

                        using (var tempconnection = new MySqlConnection(ConnectionString))
                        using (var tempcommand = new MySqlCommand(Sql))
                        {
                            tempcommand.CommandTimeout = 500;
                            tempcommand.Connection = tempconnection;
                            tempconnection.Open();
                            tempcommand.ExecuteNonQuery();
                            return true;
                        }

                }

                return false;

            }
            catch (Exception error)
            {
                ex = error;
                return false;
            }
        }

        public static int QueryODBC(string Sql, string ConnectionString, int sqltimout = 500)
        {
            try
            {

                using var MyConnection = new System.Data.Odbc.OdbcConnection(ConnectionString);
                using var MyCommand = new System.Data.Odbc.OdbcCommand
                {
                    CommandTimeout = sqltimout,
                    Connection = MyConnection,
                    CommandText = Sql
                };
                MyConnection.Open();
                return MyCommand.ExecuteNonQuery();
            }
            catch
            {
                return -1;
            }
        }

        public static int QueryODBC(string Sql, OConnection Connection, OConnectionType ConnectionType, int sqltimout = 500)
        {
            try
            {
                string ConnectionString     = Connection.ToString(ConnectionType);
                using var MyConnection      = new System.Data.Odbc.OdbcConnection(ConnectionString);
                using var MyCommand         = new System.Data.Odbc.OdbcCommand
                {
                    CommandTimeout = sqltimout,
                    Connection = MyConnection,
                    CommandText = Sql
                };
                MyConnection.Open();
                return MyCommand.ExecuteNonQuery();

            }
            catch
            {
                return -1;
            }
        }

        public static int QueryODBC(string StoredProc, OdbcParameter[] Parameters, string ConnectionString, int sqltimout = 500)
        {
            try
            {
                using var MyConnection = new System.Data.Odbc.OdbcConnection(ConnectionString);
                using var MyCommand = new System.Data.Odbc.OdbcCommand(StoredProc)
                {
                    CommandTimeout = sqltimout,
                    Connection = MyConnection,
                    CommandType = CommandType.StoredProcedure
                };
                if (Parameters != null)
                    MyCommand.Parameters.AddRange(Parameters);
                MyConnection.Open();
                return MyCommand.ExecuteNonQuery();

            }
            catch
            {
                return -1;
            }
        }

        public static int QueryODBC(string StoredProc, OdbcParameter[] Parameters, OConnection Connection, OConnectionType ConnectionType, int sqltimout = 500)
        {
            try
            {
                string ConnectionString     = Connection.ToString(ConnectionType);
                using var MyConnection      = new System.Data.Odbc.OdbcConnection(ConnectionString);
                using var MyCommand         = new System.Data.Odbc.OdbcCommand(StoredProc)
                {
                    CommandTimeout  = sqltimout,
                    Connection      = MyConnection,
                    CommandType     = CommandType.StoredProcedure
                };
                if (Parameters != null)
                    MyCommand.Parameters.AddRange(Parameters);
                MyConnection.Open();
                return MyCommand.ExecuteNonQuery();

            }
            catch
            {
                return -1;
            }
        }

        #endregion

        #region String Functions

        public static string SafeString(string strText)
        {

            if (string.IsNullOrEmpty(strText))
                return string.Empty;

            return strText.Replace("'", "''");
        }

        public static string SafeStringReverse(string strText)
        {
            return strText.Replace("''", "'");
        }

        public static string ConvertTime(TimeSpan ts)
        {
            string DBHours  = ts.Hours.ToString();
            string DBMins   = ts.Minutes.ToString();
            string DBSecs   = ts.Seconds.ToString();

            if (DBHours.ToString().Length == 1)
                DBHours = "0" + DBHours;

            if (DBMins.ToString().Length == 1)
                DBMins = "0" + DBMins;        

            if (DBSecs.ToString().Length == 1)
                DBSecs = "0" + DBSecs;

            return DBHours + ":" + DBMins + ":" + DBSecs;
        }

        public static string ConvertDate(DateTime dt)
        {
            int DBYear = dt.Year;
            string DBMonth;
            string DBDay;
            if (dt.Month.ToString().Length == 1)
            {
                DBMonth = "0" + dt.Month;
            }
            else
            {
                DBMonth = dt.Month.ToString();
            }
            if (dt.Day.ToString().Length == 1)
            {
                DBDay = "0" + dt.Day;
            }
            else
            {
                DBDay = dt.Day.ToString();
            }
            return DBYear + "-" + DBMonth + "-" + DBDay;
        }

        public static string ConvertDateTime(DateTime dt)
        {
            int DBYear = dt.Year;
            string DBMonth;
            string DBDay;
            if (dt.Month.ToString().Length == 1)
            {
                DBMonth = "0" + dt.Month;
            }
            else
            {
                DBMonth = dt.Month.ToString();
            }
            if (dt.Day.ToString().Length == 1)
            {
                DBDay = "0" + dt.Day;
            }
            else
            {
                DBDay = dt.Day.ToString();
            }
            return DBYear + "-" + DBMonth + "-" + DBDay + " " + Microsoft.VisualBasic.Strings.FormatDateTime(dt, Constants.vbLongTime);
        }

        public static string ConvertDateTime_MMDDYYYY_HHMMSS(DateTime dt)
        {
            int DBYear = dt.Year;
            string DBMonth;
            string DBDay;
            if (dt.Month.ToString().Length == 1)
            {
                DBMonth = "0" + dt.Month;
            }
            else
            {
                DBMonth = dt.Month.ToString();
            }
            if (dt.Day.ToString().Length == 1)
            {
                DBDay = "0" + dt.Day;
            }
            else
            {
                DBDay = dt.Day.ToString();
            }
            return DBMonth + "/" + DBDay + "/" + DBYear + " " + Microsoft.VisualBasic.Strings.FormatDateTime(dt, Constants.vbLongTime);
        }

        public static string DateTimeNow()
        {
            int DBYear = DateAndTime.Now.Year;
            string DBMonth;
            string DBDay;
            if (DateAndTime.Now.Month.ToString().Length == 1)
            {
                DBMonth = "0" + DateAndTime.Now.Month.ToString();
            }
            else
            {
                DBMonth = DateAndTime.Now.Month.ToString();
            }
            if (DateAndTime.Now.Day.ToString().Length == 1)
            {
                DBDay = "0" + DateAndTime.Now.Day;
            }
            else
            {
                DBDay = DateAndTime.Now.Day.ToString();
            }
            return DBYear + "-" + DBMonth + "-" + DBDay + " " + Microsoft.VisualBasic.Strings.FormatDateTime(DateAndTime.Now, Constants.vbLongTime);
        }

        public static string RowToString(DataRow e)
        {
            string result = string.Empty;
            try
            {

                foreach (object r in e.ItemArray)
                    result += "'" + r.ToString() + "',";

                result = result.Remove(result.Length - 1, 1);
            }
            catch { }
            return result;
        }

        public static string RowToString(DataRowCollection e)
        {
            string result = string.Empty;
            try
            {

                foreach (DataRow r in e)
                    result += "'" + (string)r[0] + "',";

                result = result.Remove(result.Length - 1, 1);
            }
            catch { }
            return result;
        }

        public static string RowToNumbers(DataRowCollection e)
        {
            string result = string.Empty;
            try
            {

                foreach (DataRow r in e)
                    result += "" + Convert.ToString(r[0]) + ",";

                result = result.Remove(result.Length - 1, 1);
            }
            catch { }
            return result;
        }

        public static string GetSqlRepresentation(PropertyInfo p, object value)
        {

            StringBuilder output = new();

            if (value == null)
                output.Append("NULL");
            else
            {
                if (p == null)
                    output.Append(value.ToString());
                else
                    switch (p.PropertyType.Name)
                    {
                        case "String":
                            output.Append("'" + SafeString(value.ToString()) + "'");
                            break;
                        case "DateTime":
                            output.Append("CAST('" + ConvertDateTime((DateTime)value) + "' AS DATETIME)");
                            break;
                        case "Date":
                            output.Append("CAST('" + ConvertDateTime((DateTime)value) + "' AS DATE)");
                            break;
                        case "TimeSpan":
                            output.Append("CAST('" + ConvertTime((TimeSpan)value) + "' AS TIME(0))");
                            break;
                        case "Boolean":
                            output.Append(Convert.ToBoolean(value) ? "1" : "0");
                            break;
                        default:
                            if (p.PropertyType.BaseType.Name == "Enum")
                                output.Append(Convert.ToInt32(value)); //.ToString()
                            else
                                output.Append(value.ToString());
                            break;
                    }

            }

            return output.ToString();

        }
      
        public static string GetSqlRepresentation(SqlDbType DataType, object value)
        {

            StringBuilder output = new();

            switch (DataType)
            {
                case SqlDbType.VarChar:
                case SqlDbType.NVarChar:
                case SqlDbType.NText:
                case SqlDbType.Text:
                    output.Append("'" + SafeString(value.ToString()) + "'");
                    break;
                case SqlDbType.DateTime:
                    output.Append("CAST('" + ConvertDateTime((DateTime)value) + "' AS DATETIME)");
                    break;
                case SqlDbType.Date:
                    output.Append("CAST('" + ConvertDateTime((DateTime)value) + "' AS DATE)");
                    break;
                case SqlDbType.Time:
                    output.Append("CAST('" + ConvertTime((TimeSpan)value) + "' AS TIME(0))");
                    break;
                case SqlDbType.Bit:
                    output.Append(Convert.ToBoolean(value) ? "1" : "0");
                    break;
                default:
                    output.Append(value.ToString());
                    break;
            }

            return output.ToString();

        }
      
        public static SqlDbType GetSqlDbType(Type t)
        {
            SqlDbType _result;

            if (t.BaseType.Name == "Enum")
                _result = SqlDbType.Int;
            else
            {
                _result = (t.Name.ToString()) switch
                {
                    "String" => SqlDbType.NVarChar,
                    "Int16" => SqlDbType.SmallInt,
                    "Int32" => SqlDbType.Int,
                    "Int64" => SqlDbType.BigInt,
                    "UInt16" => SqlDbType.SmallInt,
                    "UInt32" => SqlDbType.Int,
                    "UInt64" => SqlDbType.BigInt,
                    "Decimal" => SqlDbType.Decimal,
                    "Double" => SqlDbType.Float,
                    "Single" => SqlDbType.Float,
                    "DateTime" => SqlDbType.DateTime,
                    "Date" => SqlDbType.Date,
                    "Time" => SqlDbType.Time,
                    "TimeSpan" => SqlDbType.Time,
                    "Boolean" => SqlDbType.Bit,
                    "Byte[]" => SqlDbType.Binary,
                    _ => SqlDbType.NVarChar,
                };
            }

            return _result;
        }

        public static string GetSqlDefaultValueRepresentation(SqlDbType DataType, bool Encapsulate = false) 
        {

            return (DataType) switch
            {
                SqlDbType.Char          => Encapsulate ? "''''" : "''",
                SqlDbType.VarChar       => Encapsulate ? "''''" : "''",
                SqlDbType.NVarChar      => Encapsulate ? "''''" : "''",
                SqlDbType.NChar         => Encapsulate ? "''''" : "''",
                SqlDbType.NText         => Encapsulate ? "''''" : "''",

                SqlDbType.Bit           => "0",
                SqlDbType.TinyInt       => "0",
                SqlDbType.SmallInt      => "0",
                SqlDbType.Int           => "0",
                SqlDbType.BigInt        => "0",

                SqlDbType.SmallMoney    => "0.00",
                SqlDbType.Real          => "0.00",
                SqlDbType.Decimal       => "0.00",
                SqlDbType.Float         => "0.00",
                SqlDbType.Money         => "0.00",

                SqlDbType.Date          => "getdate()",
                SqlDbType.SmallDateTime => "getdate()",
                SqlDbType.DateTime      => "getdate()",
                SqlDbType.DateTime2     => "getdate()",
                SqlDbType.Time          => Encapsulate ? "''00:00:00''" : "'00:00:00'",

                SqlDbType.Binary        => "0x00",
                SqlDbType.Image         => "0x00",  
                SqlDbType.VarBinary     => "0x00",

                _                       => Encapsulate ? "''''" : "''",
            };

        }

        public static object NullSafeString(object arg, object returnIfnull = null)
        {
            object returnValue;
            if ((object.ReferenceEquals(arg, System.DBNull.Value)) || (arg == null) || (object.ReferenceEquals(arg, string.Empty)))
            {
                returnValue = returnIfnull;
            }
            else
            {
                try
                {
                    returnValue = arg;
                }
                catch
                {
                    returnValue = returnIfnull;
                }
            }
            return returnValue;
        }

        public static DateTime NullSafeDateTime(object arg)
        {
            DateTime returnValue;
            if ((object.ReferenceEquals(arg, System.DBNull.Value)) || (arg == null) || (object.ReferenceEquals(arg, string.Empty)))
            {
                returnValue = DateTime.MinValue;
            }
            else
            {
                try
                {
                    returnValue = (DateTime)arg;
                }
                catch
                {
                    returnValue = DateTime.MinValue;
                }
            }
            return returnValue;
        }

        public static int NullSafeInt(object arg)
        {
            int returnValue;
            if ((object.ReferenceEquals(arg, System.DBNull.Value)) || (arg == null) || (object.ReferenceEquals(arg, string.Empty)))
            {
                returnValue = 0;
            }
            else
            {
                try
                {
                    returnValue = (int)arg;
                }
                catch
                {
                    returnValue = 0;
                }
            }
            return returnValue;
        }

        public static long NullSafeLong(object arg)
        {
            long returnValue;
            if ((object.ReferenceEquals(arg, System.DBNull.Value)) || (arg == null) || (object.ReferenceEquals(arg, string.Empty)))
            {
                returnValue = 0;
            }
            else
            {
                try
                {
                    returnValue = (long)arg;
                }
                catch
                {
                    returnValue = 0;
                }
            }
            return returnValue;
        }

        public static decimal NullSafeDecimal(object arg)
        {
            decimal returnValue;
            if ((object.ReferenceEquals(arg, System.DBNull.Value)) || (arg == null) || (object.ReferenceEquals(arg, string.Empty)))
            {
                returnValue = (decimal)0.00;
            }
            else
            {
                try
                {
                    returnValue = (decimal)arg;
                }
                catch
                {
                    returnValue = (decimal)0.00;
                }
            }
            return returnValue;
        }

        public static double NullSafeDouble(object arg)
        {
            double returnValue;
            if ((object.ReferenceEquals(arg, System.DBNull.Value)) || (arg == null) || (object.ReferenceEquals(arg, string.Empty)))
            {
                returnValue = 0.00D;
            }
            else
            {
                try
                {
                    returnValue = (double)arg;
                }
                catch
                {
                    returnValue = 0.00D;
                }
            }
            return returnValue;
        }

        public static short NullSafeShort(object arg)
        {
            short returnValue;
            if ((object.ReferenceEquals(arg, System.DBNull.Value)) || (arg == null) || (object.ReferenceEquals(arg, string.Empty)))
            {
                returnValue = 0;
            }
            else
            {
                try
                {
                    returnValue = (short)arg;
                }
                catch
                {
                    returnValue = 0;
                }
            }
            return returnValue;
        }

        public static string NullSafeString(object arg)
        {
            string returnValue;
            if ((object.ReferenceEquals(arg, System.DBNull.Value)) || (arg == null) || (object.ReferenceEquals(arg, string.Empty)))
            {
                returnValue = string.Empty;
            }
            else
            {
                try
                {
                    returnValue = (string)arg;
                }
                catch
                {
                    returnValue = string.Empty;
                }
            }
            return returnValue;
        }

        #endregion

        #region Converters

        private static readonly Dictionary<System.Type, CellValues> columnTypeToCellDataTypeMap = new()
        {
            { typeof(Boolean), CellValues.Boolean },
            { typeof(Byte), CellValues.Number },
            { typeof(Decimal), CellValues.Number },
            { typeof(Char), CellValues.String },
            { typeof(DateTime), CellValues.Date },
            { typeof(Double), CellValues.Number },
            { typeof(Int16), CellValues.Number },
            { typeof(Int32), CellValues.Number },
            { typeof(Int64), CellValues.Number },
            { typeof(SByte), CellValues.Number },
            { typeof(Single), CellValues.Number },
            { typeof(String), CellValues.String },
            { typeof(UInt16), CellValues.Number },
            { typeof(UInt32), CellValues.Number },
            { typeof(UInt64), CellValues.Number },
        };

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Interoperability", "CA1416:Validate platform compatibility", Justification = "<Pending>")]
        public static DataTable ParseCsv(string path, bool header, string[] fields = null, bool imex = false, OExcelParserType ptype = OExcelParserType.Delimited)
        {

            if (!File.Exists(path))
                return null;

            string full = Path.GetFullPath(path);
            string file = Path.GetFileName(full);
            string dir = Path.GetDirectoryName(full);
            string ext = file.Remove(0, file.LastIndexOf(".") + 1);
            string connString = string.Empty;
            string tablename = string.Empty;

            if ((ext == "xlsx") || (ext == "xls"))
            {
                if (ext == "xlsx")
                {
                    connString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + path + ";Extended Properties=\"Excel 12.0;HDR=" + (header == true ? "YES" : "NO") + ";IMEX=" + (imex == true ? "1" : "0") + "\"";
                }
                else
                {

                    connString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + path + ";Extended Properties=\"Excel 8.0;HDR=" + (header == true ? "YES" : "NO") + ";IMEX=" + (imex == true ? "1" : "0") + "\"";

                    //connString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + path + ";Extended Properties=\"Excel 8.0;HDR=" + (header == true ? "YES" : "NO") + ";IMEX=" + (imex == true ? "1" : "0") + "\"";

                }

                OleDbConnection conn = new(connString);
                conn.Open();

                DataTable schemaTable = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                tablename = schemaTable.Rows[0][2].ToString();


                if (tablename.EndsWith("'_"))
                {
                    tablename = tablename.Remove(0, 1);
                    tablename = tablename.Remove(tablename.Length - 2);
                }

                if (tablename.EndsWith("_"))
                {
                    tablename = tablename.Remove(tablename.Length - 1);
                }

                conn.Close();
                conn.Dispose();

            }

            if (ext == "csv")
            {

                string t = "Delimited";

                if (ptype == OExcelParserType.Delimited) { t = "Delimited"; }
                if (ptype == OExcelParserType.TabDelimited) { t = "TabDelimited"; }
                if (ptype == OExcelParserType.CsvDelimited) { t = "CSVDelimited"; }

                connString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=\"" + dir + "\\\";Extended Properties=\"text;HDR=" + (header == true ? "YES" : "NO") + ";FMT=" + t + "\"";

                //connString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source=\"" + dir + "\\\";Extended Properties=\"text;HDR=" + (header == true ? "YES" : "NO") + ";FMT=" + t + "\"";

                tablename = file;
            }

            string query;

            if (fields == null)
            {
                query = "SELECT * FROM [" + tablename + "]";
            }
            else
            {

                string sfields = string.Empty;

                for (int i = 0; i < fields.Length; i++)
                    sfields += "[" + fields[i] + "], ";

                sfields = sfields.Remove(sfields.Length - 2, 2);

                query = "SELECT " + sfields + " FROM [" + tablename + "]";

            }

            DataTable dTable = new();
            OleDbDataAdapter dAdapter = new(query, connString);

            try
            {
                dAdapter.Fill(dTable);
            }
            catch (Exception)
            {

                if ((ext == "xlsx") || (ext == "xls"))
                {
                    if (imex)
                    {
                        throw;
                    }
                    else
                    {
                        return OHelpers.ParseCsv(path, header, fields, true);
                    }
                }

            }

            dAdapter.Dispose();

            return dTable;

        }

        public static DataTable ParseCsv(string path, bool hasFieldsEnclosedInQuotes, string delimiter)
        {

            DataTable csvData = new();

            try
            {

                using TextFieldParser csvReader = new(path);
                csvReader.SetDelimiters(new string[] { delimiter });

                csvReader.HasFieldsEnclosedInQuotes = hasFieldsEnclosedInQuotes;
                string[] colFields = csvReader.ReadFields();

                foreach (string column in colFields)
                {

                    DataColumn datecolumn = new(column)
                    {
                        AllowDBNull = true
                    };

                    csvData.Columns.Add(datecolumn);

                }

                while (!csvReader.EndOfData)
                {

                    string[] fieldData = csvReader.ReadFields();

                    for (int i = 0; i < fieldData.Length; i++)
                    {

                        if (fieldData[i] == "")
                            fieldData[i] = null;

                    }

                    csvData.Rows.Add(fieldData);

                }
            }

            catch { }

            return csvData;

        }

        public static DataTable ParseCsv(Stream content, bool hasFieldsEnclosedInQuotes, string delimiter)
        {

            DataTable csvData = new();

            try
            {

                using TextFieldParser csvReader = new(content);
                csvReader.SetDelimiters(new string[] { delimiter });

                csvReader.HasFieldsEnclosedInQuotes = hasFieldsEnclosedInQuotes;
                string[] colFields = csvReader.ReadFields();

                colFields.ForEach(column =>
                {
                    csvData.Columns.Add(
                        new DataColumn(column)
                        {
                            AllowDBNull = true
                        }
                    );
                });

                while (!csvReader.EndOfData)
                {
                    string[] fieldData = csvReader.ReadFields();

                    for (int i = 0; i < fieldData.Length; i++)
                        if (fieldData[i] == "")
                            fieldData[i] = null;

                    csvData.Rows.Add(fieldData);
                }
            }

            catch { }

            return csvData;

        }

        public static string ToCsv(this DataSet dataset, bool incHeaders = false, bool quoteWrap = false, string colSep = "", string rowSep = "\r\n")
        {
            return dataset.Tables[0].ToCsv(incHeaders, quoteWrap, colSep, rowSep);
        }

        public static string ToCsv(this DataTable table, bool incHeaders = false, bool quoteWrap = false, string colSep = "", string rowSep = "\r\n")
        {

            string format = string.Empty;
            string output = string.Empty;

            if (quoteWrap)
                format = string.Join(colSep, Enumerable.Range(0, table.Columns.Count).Select(i => string.Format("\"{{{0}}}\"", i)));
            else
                format = string.Join(colSep, Enumerable.Range(0, table.Columns.Count).Select(i => string.Format("{{{0}}}", i)));

            if (incHeaders)
                output += string.Format(format, table.Columns.OfType<DataColumn>().Select(i => i.ColumnName).ToArray()) + rowSep;

            output += string.Join(rowSep, table.Rows.OfType<DataRow>().Select(i => string.Format(format, i.ItemArray)));

            return output;

        }

        public static DataSet FromCSV(Stream e, string password)
        {

            ExcelReaderConfiguration c = new()
            {
                FallbackEncoding = Encoding.GetEncoding(1252),
                AutodetectSeparators = new char[] { ',', '\t', '|' },
                LeaveOpen = false,
                AnalyzeInitialCsvRows = 0,
            };

            if (!string.IsNullOrEmpty(password))
                c.Password = password;

            IExcelDataReader reader = ExcelReaderFactory.CreateCsvReader(e, c);

            return reader.AsDataSet(new ExcelDataSetConfiguration()
            {
                // Gets or sets a value indicating whether to set the DataColumn.DataType 
                // property in a second pass.
                UseColumnDataType = true,

                // Gets or sets a callback to determine whether to include the current sheet
                // in the DataSet. Called once per sheet before ConfigureDataTable.
                FilterSheet = (tableReader, sheetIndex) => true,

                // Gets or sets a callback to obtain configuration options for a DataTable. 
                ConfigureDataTable = (tableReader) => new ExcelDataTableConfiguration()
                {
                    // Gets or sets a value indicating the prefix of generated column names.
                    EmptyColumnNamePrefix = "Column",

                    // Gets or sets a value indicating whether to use a row from the 
                    // data as column names.
                    UseHeaderRow = true,

                    // Gets or sets a callback to determine which row is the header row. 
                    // Only called when UseHeaderRow = true.
                    //ReadHeaderRow = (rowReader) => {
                    // F.ex skip the first row and use the 2nd row as column headers:
                    //    rowReader.Read();
                    //},

                    // Gets or sets a callback to determine whether to include the 
                    // current row in the DataTable.
                    //FilterRow = (rowReader) => { return true; },

                    // Gets or sets a callback to determine whether to include the specific
                    // column in the DataTable. Called once per column after reading the 
                    // headers.
                    //FilterColumn = (rowReader, columnIndex) => { return true; }

                }
            });

        }

        public static DataSet FromExcel(Stream e, string password)
        {

            ExcelReaderConfiguration c = new() { LeaveOpen = false };

            if (!string.IsNullOrEmpty(password))
                c.Password = password;

            // Auto-detect format, supports:
            //  - Binary Excel files (2.0-2003 format; *.xls)
            //  - OpenXml Excel files (2007 format; *.xlsx, *.xlsb)

            IExcelDataReader reader = ExcelReaderFactory.CreateReader(e, c);

            return reader.AsDataSet(new ExcelDataSetConfiguration()
            {
                // Gets or sets a value indicating whether to set the DataColumn.DataType 
                // property in a second pass.
                UseColumnDataType = true,

                // Gets or sets a callback to determine whether to include the current sheet
                // in the DataSet. Called once per sheet before ConfigureDataTable.
                FilterSheet = (tableReader, sheetIndex) => true,

                // Gets or sets a callback to obtain configuration options for a DataTable. 
                ConfigureDataTable = (tableReader) => new ExcelDataTableConfiguration()
                {
                    // Gets or sets a value indicating the prefix of generated column names.
                    EmptyColumnNamePrefix = "Column",

                    // Gets or sets a value indicating whether to use a row from the 
                    // data as column names.
                    UseHeaderRow = true,

                    // Gets or sets a callback to determine which row is the header row. 
                    // Only called when UseHeaderRow = true.
                    //ReadHeaderRow = (rowReader) => {
                    // F.ex skip the first row and use the 2nd row as column headers:
                    //    rowReader.Read();
                    //},

                    // Gets or sets a callback to determine whether to include the 
                    // current row in the DataTable.
                    //FilterRow = (rowReader) => { return true; },

                    // Gets or sets a callback to determine whether to include the specific
                    // column in the DataTable. Called once per column after reading the 
                    // headers.
                    //FilterColumn = (rowReader, columnIndex) => { return true; }

                }
            });

        }

        public static byte[] ToExcel(this DataSet dataset)
        {

            MemoryStream ms = new();

            using (SpreadsheetDocument workbook = SpreadsheetDocument.Create(ms, SpreadsheetDocumentType.Workbook))
            {
                workbook.AddWorkbookPart();
                workbook.WorkbookPart.Workbook = new Workbook { Sheets = new Sheets() };

                uint sheetId = 1;

                WorksheetPart sheetPart = workbook.WorkbookPart.AddNewPart<WorksheetPart>();

                SheetData sheetData = new();

                sheetPart.Worksheet = new Worksheet(sheetData);

                Sheets sheets = workbook.WorkbookPart.Workbook.GetFirstChild<Sheets>();

                string relationshipId = workbook.WorkbookPart.GetIdOfPart(sheetPart);

                if (sheets.Elements<Sheet>().Any())
                    sheetId = sheets.Elements<Sheet>().Select(s => s.SheetId.Value).Max() + 1;

                foreach (DataTable dataTable in dataset.Tables)
                {

                    //Lets add a new sheet to the document.
                    Sheet sheet = new()
                    {
                        Id = relationshipId,
                        SheetId = sheetId,
                        Name = dataTable.TableName
                    };
                    sheets.Append(new List<OpenXmlElement> { sheet });

                    //Lets add the header columns
                    Row headerRow = new();
                    foreach (DataColumn column in dataTable.Columns)
                        headerRow.AppendChild(new Cell
                        {
                            DataType = CellValues.String,
                            CellValue = new CellValue(column.ColumnName)
                        });

                    sheetData.AppendChild(headerRow);

                    foreach (DataRow row in dataTable.Rows)
                    {
                        Row newRow = new();
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            // map the column type to an OpenXML SDK CellValues type
                            if (!columnTypeToCellDataTypeMap.TryGetValue(column.DataType, out CellValues cellDataType))
                                cellDataType = CellValues.String;

                            // construct the cell
                            newRow.AppendChild(new Cell
                            {
                                DataType = cellDataType,
                                CellValue = new CellValue((row[column] != null ? row[column].ToString() : string.Empty))
                            });

                        }
                        sheetData.AppendChild(newRow);
                    }
                }
            }

            byte[] output = ms.ToArray();

            ms.Dispose();

            return output;

        }
      
        public static DataTable JoinDataTables(this DataTable table, DataTable joinTo, params Func<DataRow, DataRow, bool>[] joinOn)
        {

            DataTable result = new();

            foreach (DataColumn col in table.Columns)
                if (result.Columns[col.ColumnName] == null)
                    result.Columns.Add(col.ColumnName, col.DataType);

            foreach (DataColumn col in joinTo.Columns)
                if (result.Columns[col.ColumnName] == null)
                    result.Columns.Add(col.ColumnName, col.DataType);

            foreach (DataRow row1 in table.Rows)
            {

                var joinRows = joinTo.AsEnumerable().Where(row2 => {
                    foreach (var parameter in joinOn)
                        if (!parameter(row1, row2))
                            return false;
                    return true;
                });

                foreach (DataRow fromRow in joinRows)
                {
                    DataRow insertRow = result.NewRow();
                    foreach (DataColumn col1 in table.Columns)
                        insertRow[col1.ColumnName] = row1[col1.ColumnName];
                    foreach (DataColumn col2 in joinTo.Columns)
                        insertRow[col2.ColumnName] = fromRow[col2.ColumnName];
                    result.Rows.Add(insertRow);
                }

            }

            return result;
        }

        #endregion

        #region Callback Loops

        public static void Each(this DataRowCollection rows, Action<DataRow> predicate)
        {

            foreach (DataRow r in rows)
                predicate(r);

        }
        
        public static void Each(this DataColumnCollection columns, Action<DataColumn> predicate)
        {

            foreach (DataColumn c in columns)
                predicate(c);

        }

        public static void Each(this DataSet dataset, Action<DataTable> predicate)
        {

            foreach (DataTable t in dataset.Tables)
                predicate(t);

        }

        #endregion

        #region Tools

        public static ReportParameterInfoCollection GetSsrsReportParameters(string serverUrl, string reportPath)
        {
            return new ServerReport
            {
                ReportServerUrl = new Uri(serverUrl),
                ReportPath      = reportPath
            }.GetParameters();
        }

        public static JObject GetParmsForSSRS(IDictionary<string, string> query)
        {

            //This comes from the formatted plugin template config file. (SQLReportingService)
            string ServiceUrl = query["Service Url"];
            string ReportPath = query["Report Path"];

            JObject output = new();

            try
            {

                ReportParameterInfoCollection Parameters = GetSsrsReportParameters(ServiceUrl, ReportPath);
               
                Parameters
                    .Cast<ReportParameterInfo>()
                    .ToArray()
                    .ForEach(Parameter => {
                        output.Add(new JProperty("listitem", new JObject(
                            new JProperty("name", Parameter.Name),
                            new JProperty("prompt", Parameter.Prompt),
                            new JProperty("datatype", Parameter.DataType.ToString())
                        )));
                    });

            }
            catch (Exception ex)
            {
                output.Add(new JProperty("response", new JObject(new JProperty("message", "Error: " + ex.Message))));
            }

            return output;

        }

        public static JObject GetSSSPList(IDictionary<string, string> query)
        {

            JObject output = new(new JProperty("listitem", new JArray()));

            try
            {

                int port = 1433;

                if (!string.IsNullOrEmpty(query["Port"]))
                    port = Convert.ToInt32(query["Port"]);

                OConnection c = new(
                    query["Server"],
                    query["Database"],
                    port,
                    query["Username"],
                    query["Password"]
                );

                string sql = "SELECT [NAME] FROM sysobjects WHERE type = 'P' AND category = 0;";

                DataSet ds = Get(sql, c.ToString((OConnectionType)Convert.ToInt32(query["Connection"])));

                JArray list = (JArray)output.Properties().Where(p => p.Name == "listitem").FirstOrDefault().Value;

                if (ds != null && ds.Tables[0].Rows.Count > 0)
                {
                    ds.Tables[0].Rows.Each(r => {
                        list.Add(new JObject(new JProperty("name", r["NAME"].ToString())));
                    });
                    Clear(ds);
                }

            }
            catch (Exception ex)
            {
                output.Add(new JProperty("response", new JObject(new JProperty("message", "Error: " + ex.Message))));
            }

            return output;

        }

        public static JObject GetParmsForSSSP(IDictionary<string, string> query)
        {

            JObject output = new(new JProperty("listitem", new JArray()));

            try
            {

                int port = 1433;

                if (!string.IsNullOrEmpty(query["Port"]))
                    port = Convert.ToInt32(query["Port"]);

                OConnection c = new(
                    query["Server"],
                    query["Database"],
                    port,
                    query["Username"],
                    query["Password"]
                );

                string sql = "SELECT pa.parameter_id AS [Id], pa.name AS [Name], UPPER(t.name) AS [DataType], t.max_length AS [Length] ";
                sql += "FROM sys.parameters pa ";
                sql += "INNER JOIN sys.procedures p ON pa.object_id = p.object_id ";
                sql += "INNER JOIN sys.types t ON pa.system_type_id = t.system_type_id AND pa.user_type_id = t.user_type_id ";
                sql += "WHERE p.name = '" + query["Procedures"] + "';";

                DataSet ds = Get(sql, c.ToString((OConnectionType)Convert.ToInt32(query["Connection"])));
                
                JArray list = (JArray)output.Properties().Where(p => p.Name == "listitem").FirstOrDefault().Value;

                if (ds != null && ds.Tables[0].Rows.Count > 0) {
                    ds.Tables[0].Rows.Each(r => {
                        list.Add(new JObject(
                            new JProperty("name", r["Name"].ToString()),
                            new JProperty("datatype", r["DataType"].ToString()),
                            new JProperty("datalength", r["Length"].ToString())
                        ));
                    });
                    Clear(ds);
                }

            }
            catch (Exception ex)
            {
                output.Add(new JProperty("response", new JObject(new JProperty("message", "Error: " + ex.Message))));
            }

            return output;

        }
        
        public static string CreateTableSql(DataTable table)
        {
            StringBuilder sql       = new();
            StringBuilder alterSql  = new();

            sql.AppendFormat("CREATE TABLE [{0}] (", table.TableName);

            for (int i = 0; i < table.Columns.Count; i++)
            {
                bool isNumeric = false;
                bool usesColumnDefault = true;

                sql.AppendFormat("\n\t[{0}]", table.Columns[i].ColumnName);

                switch (table.Columns[i].DataType.ToString().ToUpper())
                {
                    case "SYSTEM.INT16":
                        sql.Append(" smallint");
                        isNumeric = true;
                        break;
                    case "SYSTEM.INT32":
                        sql.Append(" int");
                        isNumeric = true;
                        break;
                    case "SYSTEM.INT64":
                        sql.Append(" bigint");
                        isNumeric = true;
                        break;
                    case "SYSTEM.DATETIME":
                        sql.Append(" datetime");
                        usesColumnDefault = false;
                        break;
                    case "SYSTEM.STRING":
                        sql.AppendFormat(" nvarchar({0})", table.Columns[i].MaxLength == -1 ? "max" : table.Columns[i].MaxLength.ToString());
                        break;
                    case "SYSTEM.SINGLE":
                        sql.Append(" float");
                        isNumeric = true;
                        break;
                    case "SYSTEM.DOUBLE":
                        sql.Append(" float");
                        isNumeric = true;
                        break;
                    case "SYSTEM.DECIMAL":
                        sql.Append(" decimal(18, 6)");
                        isNumeric = true;
                        break;
                    default:
                        sql.AppendFormat(" nvarchar({0})", table.Columns[i].MaxLength == -1 ? "max" : table.Columns[i].MaxLength.ToString());
                        break;
                }

                if (table.Columns[i].AutoIncrement)
                {
                    sql.AppendFormat(" IDENTITY({0},{1})",
                        table.Columns[i].AutoIncrementSeed,
                        table.Columns[i].AutoIncrementStep);
                }
                else
                {
                    // DataColumns will add a blank DefaultValue for any AutoIncrement column. 
                    // We only want to create an ALTER statement for those columns that are not set to AutoIncrement. 
                    if (table.Columns[i].DefaultValue != null)
                    {
                        if (usesColumnDefault)
                        {
                            if (isNumeric)
                            {
                                alterSql.AppendFormat("\nALTER TABLE {0} ADD CONSTRAINT [DF_{0}_{1}]  DEFAULT ({2}) FOR [{1}];",
                                    table.TableName,
                                    table.Columns[i].ColumnName,
                                    string.IsNullOrEmpty(table.Columns[i].DefaultValue.ToString()) ? 0 : table.Columns[i].DefaultValue);
                            }
                            else
                            {
                                alterSql.AppendFormat("\nALTER TABLE {0} ADD CONSTRAINT [DF_{0}_{1}]  DEFAULT ('{2}') FOR [{1}];",
                                    table.TableName,
                                    table.Columns[i].ColumnName,
                                    string.IsNullOrEmpty(table.Columns[i].DefaultValue.ToString()) ? string.Empty : table.Columns[i].DefaultValue);
                            }
                        }
                        else
                        {
                            // Default values on Date columns, e.g., "DateTime.Now" will not translate to SQL.
                            // This inspects the caption for a simple XML string to see if there is a SQL compliant default value, e.g., "GETDATE()".
                            try
                            {
                                System.Xml.XmlDocument xml = new();

                                xml.LoadXml(table.Columns[i].Caption);

                                alterSql.AppendFormat("\nALTER TABLE {0} ADD CONSTRAINT [DF_{0}_{1}]  DEFAULT ({2}) FOR [{1}];",
                                    table.TableName,
                                    table.Columns[i].ColumnName,
                                    xml.GetElementsByTagName("defaultValue")[0].InnerText);
                            }
                            catch
                            {
                                // Handle
                            }
                        }
                    }
                }

                if (!table.Columns[i].AllowDBNull)
                {
                    sql.Append(" NOT NULL");
                }

                sql.Append(',');
            }

            if (table.PrimaryKey.Length > 0)
            {
                StringBuilder primaryKeySql = new();

                primaryKeySql.AppendFormat("\n\tCONSTRAINT PK_{0} PRIMARY KEY (", table.TableName);

                for (int i = 0; i < table.PrimaryKey.Length; i++)
                {
                    primaryKeySql.AppendFormat("{0},", table.PrimaryKey[i].ColumnName);
                }

                primaryKeySql.Remove(primaryKeySql.Length - 1, 1);
                primaryKeySql.Append(')');

                sql.Append(primaryKeySql);
            }
            else
            {
                sql.Remove(sql.Length - 1, 1);
            }

            sql.AppendFormat("\n);\n{0}", alterSql.ToString());

            return sql.ToString();
        }

        #endregion

    }

}
