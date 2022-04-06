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
using EFCore.BulkExtensions;

using K2host.Core;
using K2host.Data.Enums;
using K2host.Data.Classes;
using K2host.Data.Attributes;
using K2host.Data.Extentions.ODataConnection;
using K2host.Data.Interfaces;

namespace K2host.Data
{
    
    public static class OHelpers
    {

        #region Parameter Builders

        public static DbParameter CreateParam(SqlDbType datatype, object value, string paramname)
        {
            try
            {
                return new SqlParameter()
                {
                    SqlDbType       = datatype,
                    Value           = value,
                    ParameterName   = paramname
                };
            }
            catch
            {
                return null;
            }
        }

        public static DbParameter CreateParam(SqlDbType datatype, ParameterDirection direction, object value, string paramname)
        {
            try
            {
                return new SqlParameter()
                {
                    SqlDbType       = datatype,
                    Value           = value,
                    ParameterName   = paramname,
                    Direction       = direction
                };
            }
            catch
            {
                return null;
            }
        }

        public static DbParameter CreateParam(DbType datatype, object value, string paramname)
        {
            try
            {

                return new OdbcParameter()
                {
                    DbType          = datatype,
                    Value           = value,
                    ParameterName   = paramname
                };

            }
            catch
            {
                return null;
            }
        }

        public static DbParameter CreateParam(DbType datatype, ParameterDirection direction, object value, string paramname)
        {
            try
            {
                return new OdbcParameter()
                {
                    DbType          = datatype,
                    Value           = value,
                    ParameterName   = paramname,
                    Direction       = direction
                };
            }
            catch
            {
                return null;
            }
        }

        public static DbParameter CreateParam(MySqlDbType datatype, object value, string paramname)
        {
            try
            {

                return new MySqlParameter()
                {
                    MySqlDbType     = datatype,
                    Value           = value,
                    ParameterName   = paramname
                };
            }
            catch
            {
                return null;
            }
        }

        public static DbParameter CreateParam(MySqlDbType datatype, ParameterDirection direction, object value, string paramname)
        {
            try
            {
                return new MySqlParameter ()
                {
                    MySqlDbType     = datatype,
                    Value           = value,
                    ParameterName   = paramname,
                    Direction       = direction
                };
            }
            catch
            {
                return null;
            }
        }

        public static DbParameter CreateParam(OracleDbType datatype, object value, string paramname)
        {
            try
            {
                return new OracleParameter()
                {
                    OracleDbType    = datatype,
                    Value           = value,
                    ParameterName   = paramname
                };
            }
            catch
            {
                return null;
            }
        }

        public static DbParameter CreateParam(OracleDbType datatype, ParameterDirection direction, object value, string paramname)
        {
            try
            {
                return new OracleParameter()
                {
                    OracleDbType    = datatype,
                    Value           = value,
                    ParameterName   = paramname,
                    Direction       = direction
                };
            }
            catch
            {
                return null;
            }
        }

        #endregion

        #region DataSet Builders

        public static DataSet Get(CommandType type, string Sql, DbParameter[] Parameters, string ConnectionString, OConnectionType ConnectionType)
        {
            try
            {

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(Sql))
                        using (var tempadaptor = new SqlDataAdapter())
                        {
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = 10000;
                            tempcommand.CommandType = type;

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
                        using (var tempcommand = new OracleCommand(Sql))
                        using (var tempadaptor = new OracleDataAdapter())
                        {
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = 10000;
                            tempcommand.CommandType = type;

                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);

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
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = 10000;
                            tempcommand.CommandType = type;

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

        public static DataSet Get(CommandType type, string Sql, DbParameter[] Parameters, string ConnectionString, OConnectionType ConnectionType, out Exception ex)
        {
            ex = null;
            try
            {

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(Sql))
                        using (var tempadaptor = new SqlDataAdapter())
                        {
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = 10000;
                            tempcommand.CommandType = type;

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
                        using (var tempcommand = new OracleCommand(Sql))
                        using (var tempadaptor = new OracleDataAdapter())
                        {
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = 10000;
                            tempcommand.CommandType = type;

                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);

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
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = 10000;
                            tempcommand.CommandType = type;

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
            catch(Exception error)
            {
                ex = error;
                return null;
            }
        }

        public static DataSet Get(CommandType type, string Sql, DbParameter[] Parameters, string ConnectionString, OConnectionType ConnectionType, int sqltimout = 500)
        {
            try
            {

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(Sql))
                        using (var tempadaptor = new SqlDataAdapter())
                        {
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = sqltimout;
                            tempcommand.CommandType = type;

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
                        using (var tempcommand = new OracleCommand(Sql))
                        using (var tempadaptor = new OracleDataAdapter())
                        {
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = sqltimout;
                            tempcommand.CommandType = type;

                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);

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
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = sqltimout;
                            tempcommand.CommandType = type;

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

        public static DataSet Get(CommandType type, string Sql, DbParameter[] Parameters, string ConnectionString, OConnectionType ConnectionType, out Exception ex, int sqltimout = 500)
        {
            ex = null;
            try
            {

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(Sql))
                        using (var tempadaptor = new SqlDataAdapter())
                        {
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = sqltimout;
                            tempcommand.CommandType = type;

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
                        using (var tempcommand = new OracleCommand(Sql))
                        using (var tempadaptor = new OracleDataAdapter())
                        {
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = sqltimout;
                            tempcommand.CommandType = type;

                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);

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
                            tempcommand.Connection = tempconnection;
                            tempcommand.CommandTimeout = sqltimout;
                            tempcommand.CommandType = type;

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
        
        #endregion

        #region Query Runners

        public static bool Query(CommandType type, string Sql, DbParameter[] Parameters, string ConnectionString, OConnectionType ConnectionType)
        {
            try
            {

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(Sql))
                        {
                            tempcommand.CommandType = type;
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
                        using (var tempcommand = new OracleCommand(Sql))
                        {
                            tempcommand.CommandType = type;
                            tempcommand.Connection = tempconnection;
                            tempconnection.Open();
                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);
                            tempcommand.ExecuteNonQuery();
                            return true;
                        }

                    case OConnectionType.MySqlStandardSecurity:

                        using (var tempconnection = new MySqlConnection(ConnectionString))
                        using (var tempcommand = new MySqlCommand(Sql))
                        {
                            tempcommand.CommandType = type;
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

        public static bool Query(CommandType type, string Sql, DbParameter[] Parameters, string ConnectionString, OConnectionType ConnectionType, out Exception ex)
        {
            ex = null;

            try
            {

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(Sql))
                        {
                            tempcommand.CommandType = type;
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
                        using (var tempcommand = new OracleCommand(Sql))
                        {
                            tempcommand.CommandType = type;
                            tempcommand.Connection = tempconnection;
                            tempconnection.Open();
                            if (Parameters != null)
                                tempcommand.Parameters.AddRange(Parameters);
                            tempcommand.ExecuteNonQuery();
                            return true;
                        }

                    case OConnectionType.MySqlStandardSecurity:

                        using (var tempconnection = new MySqlConnection(ConnectionString))
                        using (var tempcommand = new MySqlCommand(Sql))
                        {
                            tempcommand.CommandType = type;
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
            catch (Exception error)
            {
                ex = error;
                return false;
            }
        }

        public static bool Query(CommandType type, string Sql, DbParameter[] Parameters, string ConnectionString, OConnectionType ConnectionType, int sqltimout = 500)
        {
            try
            {

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(Sql))
                        {
                            tempcommand.CommandType = type; 
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
                        using (var tempcommand = new OracleCommand(Sql))
                        {
                            tempcommand.CommandType = type; 
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
                        using (var tempcommand = new MySqlCommand(Sql))
                        {
                            tempcommand.CommandType = type;
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

        public static bool Query(CommandType type, string Sql, DbParameter[] Parameters, string ConnectionString, OConnectionType ConnectionType, out Exception ex, int sqltimout = 500)
        {
            ex = null;

            try
            {

                switch (ConnectionType)
                {
                    case OConnectionType.SQLStandardSecurity:
                    case OConnectionType.SQLTrustedConnection:
                    case OConnectionType.SQLTrustedConnectionCE:

                        using (var tempconnection = new SqlConnection(ConnectionString))
                        using (var tempcommand = new SqlCommand(Sql))
                        {
                            tempcommand.CommandType = type;
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
                        using (var tempcommand = new OracleCommand(Sql))
                        {
                            tempcommand.CommandType = type;
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
                        using (var tempcommand = new MySqlCommand(Sql))
                        {
                            tempcommand.CommandType = type;
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
            catch (Exception error)
            {
                ex = error;
                return false;
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
                e.ItemArray.ForEach(r => {
                    result += "'" + r.ToString() + "',";
                });
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
                e.ForEach(r => {
                    result += "'" + (string)r[0] + "',";
                });
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
                e.ForEach(r => {
                    result += "" + Convert.ToString(r[0]) + ",";
                });
                result = result.Remove(result.Length - 1, 1);
            }
            catch { }
            return result;
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
       
        public static Type NullSafeType(this Type e)
        {
            string type = e.FullName;

            if (!type.StartsWith("System.Nullable`1"))
                return e;

            try
            {
                type = type.Remove(0, type.IndexOf("[[") + 2);
                type = type.Remove(type.IndexOf(","));
            }
            catch (Exception)
            {
                return e;
            }

            return Type.GetType(type);

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

                string sql = "SELECT [NAME] FROM sysobjects WHERE type = 'P' AND category = 0;";

                DataSet ds = new ODataConnection(
                    query["Server"],
                    query["Database"],
                    port,
                    query["Username"],
                    query["Password"],
                    (OConnectionType)Convert.ToInt32(query["Connection"])
                ).Get(CommandType.Text, sql, null);

                JArray list = (JArray)output.Properties().Where(p => p.Name == "listitem").FirstOrDefault().Value;

                if (ds != null && ds.Tables[0].Rows.Count > 0)
                {
                    ds.Tables[0].Rows.ForEach(r => {
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

                string sql = "SELECT pa.parameter_id AS [Id], pa.name AS [Name], UPPER(t.name) AS [DataType], t.max_length AS [Length] ";
                sql += "FROM sys.parameters pa ";
                sql += "INNER JOIN sys.procedures p ON pa.object_id = p.object_id ";
                sql += "INNER JOIN sys.types t ON pa.system_type_id = t.system_type_id AND pa.user_type_id = t.user_type_id ";
                sql += "WHERE p.name = '" + query["Procedures"] + "';";

                DataSet ds = new ODataConnection(
                    query["Server"],
                    query["Database"],
                    port,
                    query["Username"],
                    query["Password"],
                    (OConnectionType)Convert.ToInt32(query["Connection"])
                ).Get(CommandType.Text, sql, null);

                JArray list = (JArray)output.Properties().Where(p => p.Name == "listitem").FirstOrDefault().Value;

                if (ds != null && ds.Tables[0].Rows.Count > 0) {
                    ds.Tables[0].Rows.ForEach(r => {
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

        #endregion

        #region Extentions

        public static void ForEach(this DataRowCollection rows, Action<DataRow> action)
        {

            foreach (DataRow r in rows)
                action(r);

        }
        
        public static void ForEach(this DataColumnCollection columns, Action<DataColumn> action)
        {

            foreach (DataColumn c in columns)
                action(c);

        }

        public static void ForEach(this DataSet dataset, Action<DataTable> action)
        {

            foreach (DataTable t in dataset.Tables)
                action(t);

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

        public static string CreateTableSql(this DataTable table)
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
                if (p.GetCustomAttributes<ODataExceptionAttribute>().Any())
                {
                    if (!p.GetCustomAttributes<ODataExceptionAttribute>().First().ODataExceptionType.HasFlag(NonInteract))
                        output.Add(
                            new ODataFieldSet()
                            {
                                Column      = p,
                                DataType    = ODataContext.Connection().GetDbDataType(p.PropertyType)
                            });

                }
                else
                    output.Add(
                        new ODataFieldSet()
                        {
                            Column      = p,
                            DataType    = ODataContext.Connection().GetDbDataType(p.PropertyType)
                        });

            return output.ToArray();
        }

        public static string GetMappedName(this Type e) 
        {
            var tnm = e.Name;
            var map = e.GetCustomAttribute<ODataReMapAttribute>();
            if (map != null)
                tnm = map.ModelName;
            return tnm;
        }
       
        public static string GetMappedName(this TypeInfo e)
        {
            var tnm = e.Name;
            var map = e.GetCustomAttribute<ODataReMapAttribute>();
            if (map != null)
                tnm = map.ModelName;
            return tnm;
        }

        #endregion

        #region IQueryable Extentions For Linq

        /// <summary>
        /// This is like the FirstOrDefault() but builds with reflection.
        /// </summary>
        /// <typeparam name="I"></typeparam>
        /// <param name="e"></param>
        /// <returns></returns>
        public static I ToFirstOrDefault<I>(this IQueryable<I> e, out ODataException ex)
            where I : class, IDataObject
        {
            ex = null;
         
            try
            {
                var Query = e.ToParametrizedSql();

                DataSet dts = ODataContext
                    .Connection()
                    .Get(
                        CommandType.Text,
                        Query.Item1,
                        Query.Item2.ToArray()
                    );

                if (dts == null)
                {
                    ex = new ODataException("K2host.Data." + typeof(I).Name + ".ToFirstOrDefault(): No Dataset(s) Returned.");
                    return default;
                }

                if (dts.Tables.Count <= 0)
                {
                    ex = new ODataException("K2host.Data." + typeof(I).Name + ".ToFirstOrDefault(): No Dataset Table(s) Returned.");
                    return default;
                }

                if (dts.Tables[0].Rows.Count <= 0)
                {
                    ex = new ODataException("K2host.Data." + typeof(I).Name + ".ToFirstOrDefault(): No Dataset Table Row(s) Returned.");
                    return default;
                }

                I output = ODataObject<I>
                    .Retrieve(dts);

                Clear(dts);

                return output;

            }
            catch (Exception exc)
            {
                ex = new ODataException(exc.Message, exc);
                return default;
            }

        }

        /// <summary>
        /// This is pulls an array of, but builds with reflection.
        /// </summary>
        /// <typeparam name="I"></typeparam>
        /// <param name="e"></param>
        /// <returns></returns>
        public static I[] ToArrayOrDefault<I>(this IQueryable<I> e, out ODataException ex)
            where I : class, IDataObject
        {

            ex = null;

            try
            {
                var Query = e.ToParametrizedSql();

                DataSet dts = ODataContext
                    .Connection()
                    .Get(
                        CommandType.Text,
                        Query.Item1,
                        Query.Item2.ToArray()
                    );

                if (dts == null)
                {
                    ex = new ODataException("K2host.Data." + typeof(I).Name + ".ToArrayOrDefault(): No Dataset(s) Returned.");
                    return Array.Empty<I>();
                }

                if (dts.Tables.Count <= 0)
                {
                    ex = new ODataException("K2host.Data." + typeof(I).Name + ".ToArrayOrDefault(): No Dataset Table(s) Returned.");
                    return Array.Empty<I>();
                }

                if (dts.Tables[0].Rows.Count <= 0)
                {
                    ex = new ODataException("K2host.Data." + typeof(I).Name + ".ToArrayOrDefault(): No Dataset Table Row(s) Returned.");
                    return Array.Empty<I>();
                }

                I[] output = ODataObject<I>
                    .List(dts)
                    .ToArray();

                Clear(dts);

                return output;

            }
            catch (Exception exc)
            {
                ex = new ODataException(exc.Message, exc);
                return Array.Empty<I>();
            }


        }

        /// <summary>
        /// This will convert the query and return the json representation directly from the database.
        /// </summary>
        /// <typeparam name="I"></typeparam>
        /// <param name="e"></param>
        /// <returns></returns>
        public static string ToJsonOrDefault<I>(this IQueryable<I> e, out ODataException ex)
            where I : class, IDataObject
        {
            ex = null;

            try
            {

                var Query = e.ToParametrizedSql();

                var sqlString = Query.Item1 + $" FOR JSON PATH, ROOT('{typeof(I).GetMappedName()}')";

                DataSet dts = ODataContext
                    .Connection()
                    .Get(
                        CommandType.Text,
                        sqlString,
                        Query.Item2.ToArray()
                    );

                if (dts == null)
                {
                    ex = new ODataException("K2host.Data." + typeof(I).Name + ".ToJsonOrDefault(): No Dataset(s) Returned.");
                    return string.Empty;
                }

                if (dts.Tables.Count <= 0)
                {
                    ex = new ODataException("K2host.Data." + typeof(I).Name + ".ToJsonOrDefault(): No Dataset Table(s) Returned.");
                    return string.Empty;
                }

                if (dts.Tables[0].Rows.Count <= 0)
                {
                    ex = new ODataException("K2host.Data." + typeof(I).Name + ".ToJsonOrDefault(): No Dataset Table Row(s) Returned.");
                    return string.Empty;
                }

                string output = string.Empty;

                foreach (DataRow r in dts.Tables[0].Rows)
                    output += r[0].ToString();

                Clear(dts);

                return output;

            }
            catch (Exception exc)
            {
                ex = new ODataException(exc.Message, exc);
                return string.Empty;
            }

        }

        /// <summary>
        /// This will convert the query and return the json representation directly from the database with a total rec count for paging etc...
        /// </summary>
        /// <typeparam name="I"></typeparam>
        /// <param name="e"></param>
        /// <param name="enableTotalCount"></param>
        /// <param name="totalCount"></param>
        /// <returns></returns>
        public static string ToJsonOrDefaultWithCount<I>(this IQueryable<I> e, bool enableTotalCount, out long totalCount, out ODataException ex)
            where I : class, IDataObject
        {
            ex = null;
            totalCount = 0;

            try
            {

                var Query = e.ToParametrizedSql();

                var sqlString = Query.Item1 + $" FOR JSON PATH, ROOT('{typeof(I).GetMappedName()}')";

                if (enableTotalCount)
                    sqlString += $"; SELECT COUNT(*) FROM {typeof(I).GetMappedName()};";

                DataSet dts = ODataContext
                    .Connection()
                    .Get(
                        CommandType.Text,
                        sqlString,
                        Query.Item2.ToArray()
                    );

                if (dts == null)
                {
                    ex = new ODataException("K2host.Data." + typeof(I).Name + ".ToJsonOrDefaultWithCount(): No Dataset(s) Returned.");
                    return string.Empty;
                }

                if (dts.Tables.Count <= 0)
                {
                    ex = new ODataException("K2host.Data." + typeof(I).Name + ".ToJsonOrDefaultWithCount(): No Dataset Table(s) Returned.");
                    return string.Empty;
                }

                if (dts.Tables[0].Rows.Count <= 0)
                {
                    ex = new ODataException("K2host.Data." + typeof(I).Name + ".ToJsonOrDefaultWithCount(): No Dataset Table Row(s) Returned.");
                    return string.Empty;
                }

                if (enableTotalCount)
                    totalCount = Convert.ToInt64(dts.Tables[1].Rows[0][0]);

                string output = string.Empty;

                foreach (DataRow r in dts.Tables[0].Rows)
                    output += r[0].ToString();

                Clear(dts);

                return output;

            }
            catch (Exception exc)
            {
                ex = new ODataException(exc.Message, exc);
                return string.Empty;
            }

        }







        #endregion
    }

}
