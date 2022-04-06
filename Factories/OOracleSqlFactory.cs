using System;
using System.Reflection;
using System.Data.Common;
using System.Data;
using System.Text;
using System.Linq;
using System.Collections.Generic;

using Oracle.ManagedDataAccess.Client;

using K2host.Core;
using K2host.Data.Enums;
using K2host.Data.Classes;
using K2host.Data.Interfaces;
using K2host.Data.Attributes;
using K2host.Data.Extentions.ODataConnection;

using gl = K2host.Core.OHelpers;
using gd = K2host.Data.OHelpers;

namespace K2host.Data.Factories
{

    public class OOracleSqlFactory : ISqlFactory
    {

        public OOracleSqlFactory() { }

        public string SetMigrationVersion(string version)
        {
            return "UPDATE MigrationVersion SET Version = '" + version + "';";
        }

        public string GetMigrationVersion()
        {
            return "SELECT Version FROM MigrationVersion";
        }

        public string TruncateDatabaseTable(Type obj)
        {
            return "TRUNCATE TABLE " + obj.GetTypeInfo().GetMappedName();
        }

        public string DropDatabaseStoredProc(string typeName)
        {
            return "DROP PROCEDURE IF EXISTS " + typeName;
        }

        public string DropDatabaseTable(string typeName)
        {
            return "DROP TABLE " + typeName;
        }

        public string DropDatabaseStoredProc(Type obj)
        {
            return "DROP PROCEDURE IF EXISTS " + obj.GetTypeInfo().GetMappedName();
        }

        public string DropDatabaseTable(Type obj)
        {
            return "DROP TABLE " + obj.GetTypeInfo().GetMappedName();
        }

        public string CreateDatabaseStoredProc(Type obj)
        {
            List<string> ignore = new() { "Uid", "ParentId", "ParentType", "Flags", "Updated", "Datestamp" };

            StringBuilder output = new();

            var tnm = obj.GetTypeInfo().GetMappedName();

            output.Append("CREATE OR REPLACE PROCEDURE " + tnm + "(" + Environment.NewLine);
            output.Append(" pUid IN NUMBER(19)" + Environment.NewLine);
            output.Append(",pParentId IN NUMBER(19)" + Environment.NewLine);
            output.Append(",pParentType IN NVARCHAR(255)" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .Each(p => {
                    output.Append(",p" + p.Name + " IN " + ((ODataTypeAttribute)p.GetCustomAttributes(true)[0]).ToString() + Environment.NewLine);
                    return true;
                });

            output.Append(",pFlags IN NUMBER(19)" + Environment.NewLine);
            output.Append(",pUpdated IN TIMESTAMP" + Environment.NewLine);
            output.Append(",pDatestamp IN TIMESTAMP" + Environment.NewLine);
            output.Append(") " + Environment.NewLine);
            output.Append("AS " + Environment.NewLine);
            output.Append("BEGIN" + Environment.NewLine);
            output.Append("    IF (pUid > 0) THEN" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        IF (pFlags = -2) THEN" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("            DELETE FROM " + tnm + " WHERE Uid = pUid;" + Environment.NewLine);
            output.Append("            SELECT pUid;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        ELSEIF (pFlags = -1) THEN " + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("            SELECT " + Environment.NewLine);
            output.Append("            Uid" + Environment.NewLine);
            output.Append("            ,ParentId" + Environment.NewLine);
            output.Append("            ,ParentType" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .Each(p => {
                    output.Append("            ," + p.Name + Environment.NewLine);
                    return true;
                });

            output.Append("            ,Flags" + Environment.NewLine);
            output.Append("            ,Updated" + Environment.NewLine);
            output.Append("            ,Datestamp" + Environment.NewLine);
            output.Append("            FROM " + tnm + " WHERE Uid = pUid;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        ELSE " + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("            UPDATE " + tnm + " SET " + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => (p.GetCustomAttribute<ODataTypeAttribute>(true) != null && !ignore.Contains(p.Name)) && (p.GetCustomAttribute<ODataExceptionAttribute>(true) == null || p.GetCustomAttribute<ODataExceptionAttribute>(true) != null && !p.GetCustomAttribute<ODataExceptionAttribute>(true).ODataExceptionType.HasFlag(ODataExceptionType.NON_UPDATE)))
                .ToArray()
                .ForEach(p => {
                    output.Append("            " + p.Name + " = p" + p.Name + "," + Environment.NewLine);
                });

            output.Append("            ParentId = pParentId," + Environment.NewLine);
            output.Append("            ParentType = pParentType," + Environment.NewLine);
            output.Append("            Flags = pFlags," + Environment.NewLine);
            output.Append("            Updated = pUpdated, " + Environment.NewLine);
            output.Append("            Datestamp = pDatestamp " + Environment.NewLine);
            output.Append("            WHERE Uid = pUid;" + Environment.NewLine);
            output.Append("            COMMIT;" + Environment.NewLine);
            output.Append("            SELECT pUid;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        END IF;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("    ELSE " + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        INSERT INTO " + tnm + " (" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => (p.GetCustomAttribute<ODataTypeAttribute>(true) != null && !ignore.Contains(p.Name)) && (p.GetCustomAttribute<ODataExceptionAttribute>(true) == null || p.GetCustomAttribute<ODataExceptionAttribute>(true) != null && !p.GetCustomAttribute<ODataExceptionAttribute>(true).ODataExceptionType.HasFlag(ODataExceptionType.NON_INSERT)))
                .ToArray()
                .ForEach(p => {
                    output.Append("            " + p.Name + "," + Environment.NewLine);
                });

            output.Append("            ParentId," + Environment.NewLine);
            output.Append("            ParentType," + Environment.NewLine);
            output.Append("            Flags," + Environment.NewLine);
            output.Append("            Updated," + Environment.NewLine);
            output.Append("            Datestamp" + Environment.NewLine);
            output.Append("        ) VALUES (" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => (p.GetCustomAttribute<ODataTypeAttribute>(true) != null && !ignore.Contains(p.Name)) && (p.GetCustomAttribute<ODataExceptionAttribute>(true) == null || p.GetCustomAttribute<ODataExceptionAttribute>(true) != null && !p.GetCustomAttribute<ODataExceptionAttribute>(true).ODataExceptionType.HasFlag(ODataExceptionType.NON_INSERT)))
                .ToArray()
                .ForEach(p => {
                    output.Append("            p" + p.Name + "," + Environment.NewLine);
                });

            output.Append("            pParentId," + Environment.NewLine);
            output.Append("            pParentType," + Environment.NewLine);
            output.Append("            pFlags," + Environment.NewLine);
            output.Append("            pUpdated," + Environment.NewLine);
            output.Append("            pDatestamp" + Environment.NewLine);
            output.Append("        );" + Environment.NewLine);
            output.Append("        COMMIT;" + Environment.NewLine);

            output.Append("        SELECT MAX(Uid) FROM " + tnm + ";" + Environment.NewLine);

            //may need to insert the uid for your_seq.NEXTVAL so we can get the id of the record back ?
            //https://docs.oracle.com/en/database/other-databases/nosql-database/19.1/java-driver-table/creating-tables-identity-column.html
            //https://www.titanwolf.org/Network/q/04fc076d-9bad-41f3-b1f4-36e872fc7324/y
            //https://mkyong.com/oracle/oracle-stored-procedure-insert-example/
            
            //SELECT your_seq.CURRVAL FROM DUAL;

            output.Append(Environment.NewLine);
            output.Append("    END IF;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("END " + tnm + ";" + Environment.NewLine);

            return output.ToString();

        }

        public string CreateDatabaseTable(Type obj, IDataConnection Connection)
        {

            List<string> ignore = new() { "Uid", "ParentId", "ParentType", "Flags", "Updated", "Datestamp" };

            StringBuilder output = new();

            var tnm = obj.GetTypeInfo().GetMappedName();

            output.Append("CREATE TABLE " + tnm + " ( " + Environment.NewLine);
            output.Append("    Uid NUMBER(19) GENERATED ALWAYS AS IDENTITY INCREMENT BY 1 START WITH 1 NOT NULL," + Environment.NewLine);
            output.Append("    ParentId NUMBER(19) NULL," + Environment.NewLine);
            output.Append("    ParentType VARCHAR2(255) NULL," + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .ForEach(p => {
                    output.Append("    " + p.Name + " " + ((ODataTypeAttribute)p.GetCustomAttributes(true)[0]).ToString() + " NULL," + Environment.NewLine);
                });

            output.Append("    Flags NUMBER(19) NULL," + Environment.NewLine);
            output.Append("    Updated TIMESTAMP NULL," + Environment.NewLine);
            output.Append("    Datestamp TIMESTAMP NULL," + Environment.NewLine);
            output.Append(" CONSTRAINT " + tnm + "_PK PRIMARY KEY ( Uid ) ENABLE " + Environment.NewLine);
            output.Append(");" + Environment.NewLine);

            return output.ToString();

        }

        public string CreateMigrationVersionTable(string DatabaseName)
        {

            StringBuilder output = new();

            output.Append("CREATE TABLE MigrationVersion (Version VARCHAR2(255) NULL);");
            output.Append("INSERT INTO MigrationVersion (Version) VALUES ('0');");

            return output.ToString();


        }

        public bool MergeMigrationBackupAndRemove(Type obj, IDataConnection Connection)
        {

            return false;

        }

        public bool MergeMigrationRestoreAndRemove(Type obj, IDataConnection Connection)
        {

            return false;
        }

        public string GetTypeAttributeRepresentation(ODataTypeAttribute e)
        {
            return e.OracleSQLDataType switch
            {
                (int)OracleDbType.NChar =>          e.DataTypeSize <= 0 ? "NCHAR"       : "NCHAR(" + e.DataTypeSize.ToString() + ")",
                (int)OracleDbType.Varchar2 =>       e.DataTypeSize <= 0 ? "VARCHAR2"    : "VARCHAR2(" + e.DataTypeSize.ToString() + ")",
                (int)OracleDbType.NVarchar2 =>      e.DataTypeSize <= 0 ? "NVARCHAR2"   : "NVARCHAR2(" + e.DataTypeSize.ToString() + ")",
                (int)OracleDbType.Long =>           e.DataTypeSize <= 0 ? "LONG"        : "LONG(" + e.DataTypeSize.ToString() + ")",
                (int)OracleDbType.LongRaw =>        e.DataTypeSize <= 0 ? "LONG RAW"     : "LONG RAW(" + e.DataTypeSize.ToString() + ")",
                (int)OracleDbType.Clob =>           "CLOB",
                (int)OracleDbType.NClob =>          "NCLOB",
                (int)OracleDbType.Blob =>           "BLOB",
                (int)OracleDbType.BFile =>          "BFILE",
                (int)OracleDbType.BinaryDouble =>   "BINARY_DOUBLE",
                (int)OracleDbType.BinaryFloat =>    "BINARY_FLOAT",
                (int)OracleDbType.Raw =>            e.DataTypeSize <= 0 ? "RAW(2000)"   : "RAW(" + e.DataTypeSize.ToString() + ")",
                (int)OracleDbType.Int16 =>          e.DataTypeSize <= 0 ? "NUMBER(5)"   : "NUMBER(" + e.DataTypeSize.ToString() + ")",
                (int)OracleDbType.Int32 =>          e.DataTypeSize <= 0 ? "NUMBER(10)"  : "NUMBER(" + e.DataTypeSize.ToString() + ")",
                (int)OracleDbType.Int64 =>          e.DataTypeSize <= 0 ? "NUMBER(19)"  : "NUMBER(" + e.DataTypeSize.ToString() + ")",
                (int)OracleDbType.Double =>         "NUMBER(" + e.DataTypeSize.ToString() + ", " + e.DataTypePlaces.ToString() + ")",
                (int)OracleDbType.Decimal =>        "NUMBER(" + e.DataTypeSize.ToString() + ", " + e.DataTypePlaces.ToString() + ")",
                (int)OracleDbType.Boolean =>        "NUMBER(5)",
                (int)OracleDbType.Byte =>           "NUMBER(1)",
                (int)OracleDbType.Date =>           "DATE",
                (int)OracleDbType.TimeStamp =>      e.DataTypeSize <= 0 ? "TIMESTAMP" : "TIMESTAMP(" + e.DataTypeSize.ToString() + ")",
                (int)OracleDbType.TimeStampTZ =>    "TIMESTAMP WITH TIME ZONE",
                (int)OracleDbType.TimeStampLTZ =>   "TIMESTAMP WITH LOCAL TIME ZONE",
                (int)OracleDbType.IntervalDS =>     "INTERVAL YEAR TO MONTH",
                (int)OracleDbType.IntervalYM =>     "INTERVAL DAY TO SECOND",
                _ =>                                "NVARCHAR2(4000)",
            };
        }
       
        public DbDataAdapter GetNewDataAdapter()
        {
            return new OracleDataAdapter();
        }

        public DbCommand GetNewCommand(string cmdText)
        {
            return new OracleCommand(cmdText);
        }

        public DbConnection GetNewConnection(IDataConnection Connection)
        {
            return new OracleConnection(Connection.ToString());
        }

        public DbParameter CreateParam(int datatype, ParameterDirection direction, object value, string paramname)
        {
            try
            {
                return gd.CreateParam((OracleDbType)datatype, direction, value, paramname);
            }
            catch
            {
                return null;
            }
        }

        public DbParameter CreateParam(int datatype, object value, string paramname)
        {
            try
            {
                return gd.CreateParam((OracleDbType)datatype, value, paramname);
            }
            catch
            {
                return null;
            }
        }

        public string GetSqlDefaultValueRepresentation(int datatype, bool encapsulate = false)
        {

            throw new ODataException("Oracle Unsupported for GetSqlDefaultValueRepresentation()");

            //return (datatype) switch
            //{
            //    (int)MySqlDbType.Char             => encapsulate ? "''''" : "''",
            //    (int)MySqlDbType.VarChar          => encapsulate ? "''''" : "''",
            //    (int)MySqlDbType.NVarChar         => encapsulate ? "''''" : "''",
            //    (int)MySqlDbType.NChar            => encapsulate ? "''''" : "''",
            //    (int)MySqlDbType.NText            => encapsulate ? "''''" : "''",
            //    (int)MySqlDbType.Bit              => "0",
            //    (int)MySqlDbType.TinyInt          => "0",
            //    (int)MySqlDbType.SmallInt         => "0",
            //    (int)MySqlDbType.Int              => "0",
            //    (int)MySqlDbType.BigInt           => "0",
            //    (int)MySqlDbType.SmallMoney       => "0.00",
            //    (int)MySqlDbType.Real             => "0.00",
            //    (int)MySqlDbType.Decimal          => "0.00",
            //    (int)MySqlDbType.Float            => "0.00",
            //    (int)MySqlDbType.Money            => "0.00",
            //    (int)MySqlDbType.Date             => "getdate()",
            //    (int)MySqlDbType.SmallDateTime    => "getdate()",
            //    (int)MySqlDbType.DateTime         => "getdate()",
            //    (int)MySqlDbType.DateTime2        => "getdate()",
            //    (int)MySqlDbType.Time             => encapsulate ? "''00:00:00''" : "'00:00:00'",
            //    (int)MySqlDbType.Binary           => "0x00",
            //    (int)MySqlDbType.Image            => "0x00",
            //    (int)MySqlDbType.VarBinary        => "0x00",
            //    _                               => encapsulate ? "''''" : "''",
            //};

        }

        public string GetSqlRepresentation(IDataConnection Connection, PropertyInfo p, object value, out IEnumerable<DbParameter> parameters)
        {

            parameters = Array.Empty<DbParameter>();

            StringBuilder output = new();

            if (value == null)
                output.Append("NULL");
            else
            {
                if (p == null)
                    output.Append(value.ToString());
                else
                {

                    p.GetCustomAttributes<ODataPropertyAttribute>()?
                        .OrderBy(a => a.Order)
                        .ForEach(a => {
                            value = a.OnWriteValue(value);
                        });

                    ODataContext.PropertyConverters()
                        .Where(c => c.CanConvert(p))
                        .ForEach(c => {
                            value = c.OnConvertTo(p, value, null);
                        });

                    string uparam = gl.UniqueIdent();

                    output.Append("@Value" + uparam);


                    if (p.PropertyType.BaseType.Name == "Enum")
                        parameters = parameters.Append(Connection.CreateParam((int)OracleDbType.Int32, value, "@Value" + uparam));
                    else
                        parameters = parameters.Append(Connection.CreateParam(Connection.GetDbDataType(p.PropertyType), value, "@Value" + uparam));

                }
            }

            return output.ToString();

        }

        public string GetSqlRepresentation(IDataConnection Connection, int xxDbType, object value, out IEnumerable<DbParameter> parameters)
        {
            parameters = Array.Empty<DbParameter>();

            StringBuilder output = new();

            if (xxDbType == -1)
                output.Append(value.ToString());
            else
            {
                string uparam = gl.UniqueIdent();

                output.Append("@Value" + uparam);

                parameters = parameters.Append(gd.CreateParam((OracleDbType)xxDbType, value, "@Value" + uparam));

            }

            return output.ToString();

        }

        public int GetDbDataType(IDataConnection Connection, Type t)
        {

            int _result;

            //Grab the embbed type if nullable.
            t = t.NullSafeType();

            if (t.BaseType.Name == "Enum")
                _result = (int)OracleDbType.Int32;
            else 
                _result = (t.Name.ToString()) switch
                {
                    "String"    => (int)OracleDbType.NVarchar2,
                    "Int16"     => (int)OracleDbType.Int16,
                    "Int32"     => (int)OracleDbType.Int32,
                    "Int64"     => (int)OracleDbType.Int64,
                    "UInt16"    => (int)OracleDbType.Int16,
                    "UInt32"    => (int)OracleDbType.Int32,
                    "UInt64"    => (int)OracleDbType.Int64,
                    "Decimal"   => (int)OracleDbType.Decimal,
                    "Double"    => (int)OracleDbType.Double,
                    "Single"    => (int)OracleDbType.Double,
                    "DateTime"  => (int)OracleDbType.TimeStamp,
                    "Date"      => (int)OracleDbType.Date,
                    "Time"      => (int)OracleDbType.IntervalDS,
                    "TimeSpan"  => (int)OracleDbType.IntervalDS,
                    "Boolean"   => (int)OracleDbType.Boolean,
                    "Byte[]"    => (int)OracleDbType.Blob,
                    _           => (int)OracleDbType.Long,
                };

            return _result;

        }

        public bool DeleteDatabase(IDataConnection Connection)
        {

            throw new ODataException("Unsupported method for : DeleteDatabase()");
            
            //return false;

        }

        public string[] GetTables(IDataConnection Connection)
        {

            throw new ODataException("Unsupported method for : GetTables()");

        }

        public bool TestDatabase(IDataConnection e, out DataTable record)
        {

            throw new ODataException("Unsupported method for : TestDatabase()");

        }

        public bool CreateDatabase(IDataConnection Connection, string filePath)
        {

            throw new ODataException("Unsupported method for : CreateDatabase()");

        }

        public string TakeSkipBuildString(ODataTakeSkip e)
        {

            throw new ODataException("Oracle Unsupported for TakeSkipBuildString()");

        }

        public string StuffFunctionBuildString(ODataStuffFunction e)
        {

            throw new ODataException("Oracle Unsupported for StuffFunctionBuildString()");


        }

        public string OrderBuildString(ODataOrder e, string prefix = "")
        {
            throw new ODataException("Oracle Unsupported for StuffFunctionBuildString()");

        }

        public string HavingSetBuildString(ODataHavingSet e, out IEnumerable<DbParameter> parameters, bool UseFieldPrefixing = false)
        {

            parameters = Array.Empty<DbParameter>();

            throw new ODataException("Oracle Unsupported for HavingSetBuildMsSql()");

        }

        public string GroupSetBuildString(ODataGroupSet e, bool UseFieldPrefixing = false)
        {

            throw new ODataException("Oracle Unsupported for GroupSetBuildBuildString()");


        }

        public string ForPathBuildString(ODataForPath e)
        {

            throw new ODataException("Oracle Unsupported for ForPathBuildString()");

        }

        public string ApplySetBuildString(ODataApplySet e)
        {

            throw new ODataException("Oracle Unsupported for ApplySetBuildString()");

        }

        public string CaseBuildString(ODataCase e, out IEnumerable<DbParameter> parameters, bool UseFieldPrefixing = false)
        {

            parameters = Array.Empty<DbParameter>();

            throw new ODataException("Oracle Unsupported for CaseBuildString()");

        }

        public string ConditionBuildString(ODataCondition e, out IEnumerable<DbParameter> parameters, bool UsePrefix = false)
        {
            parameters = Array.Empty<DbParameter>();

            throw new ODataException("Oracle Unsupported for ConditionBuildString()");

        }

        public string FieldSetUpdateBuildString(ODataFieldSet e, out IEnumerable<DbParameter> parameters)
        {
            parameters = Array.Empty<DbParameter>();

            throw new ODataException("Oracle Unsupported for FieldSetUpdateBuildString()");


        }

        public string FieldSetInsertBuildString(ODataFieldSet e, out IEnumerable<DbParameter> parameters)
        {
            parameters = Array.Empty<DbParameter>();

            throw new ODataException("Oracle Unsupported for FieldSetUpdateBuildString()");


        }

        public string FieldSetSelectBuildString(ODataFieldSet e, out IEnumerable<DbParameter> parameters, bool UseFieldPrefixing = false, bool UseFieldDefaultAlias = false)
        {
            parameters = Array.Empty<DbParameter>();

            throw new ODataException("Oracle Unsupported for FieldSetUpdateBuildString()");


        }

        public string InsertQueryBuildString(ODataInsertQuery e)
        {
            throw new ODataException("Oracle Unsupported for InsertQueryBuildString()");

        }

        public string DeleteQueryBuildString(ODataDeleteQuery e)
        {
            throw new ODataException("Oracle Unsupported for DeleteQueryBuildString()");

        }

        public string UpdateQueryBuildString(ODataUpdateQuery e)
        {
            throw new ODataException("Oracle Unsupported for UpdateQueryBuildString()");

        }

        public string SelectQueryBuildString(ODataSelectQuery e)
        {
            throw new ODataException("Oracle Unsupported for SelectQueryBuildString()");

        }

    }

}
