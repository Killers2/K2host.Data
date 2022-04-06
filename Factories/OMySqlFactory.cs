using System;
using System.Reflection;
using System.Data.Common;
using System.Data;
using System.Text;
using System.Linq;
using System.Collections.Generic;

using MySql.Data.MySqlClient;

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
    
    public class OMySqlFactory : ISqlFactory
    {

        public OMySqlFactory() { }

        public string SetMigrationVersion(string version)
        {
            return "UPDATE MigrationVersion SET Version = '" + version + "';";
        }
       
        public string GetMigrationVersion()
        {
            return "SELECT Version FROM MigrationVersion;";
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
            return "DROP TABLE IF EXISTS " + typeName;
        }

        public string DropDatabaseStoredProc(Type obj)
        {
            return "DROP PROCEDURE IF EXISTS " + obj.GetTypeInfo().GetMappedName();
        }

        public string DropDatabaseTable(Type obj)
        {
            return "DROP TABLE IF EXISTS " + obj.GetTypeInfo().GetMappedName();
        }

        public string CreateDatabaseStoredProc(Type obj)
        {
            List<string> ignore = new() { "Uid", "ParentId", "ParentType", "Flags", "Updated", "Datestamp" };

            StringBuilder output = new();

            var tnm = obj.GetTypeInfo().GetMappedName();

            output.Append("CREATE PROCEDURE `" + tnm + "`(" + Environment.NewLine);
            output.Append(" IN pUid BIGINT" + Environment.NewLine);
            output.Append(",IN pParentId BIGINT" + Environment.NewLine);
            output.Append(",IN pParentType NVARCHAR(255)" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .Each(p => {
                    output.Append(",IN p" + p.Name + " " + ((ODataTypeAttribute)p.GetCustomAttributes(true)[0]).ToString() + Environment.NewLine);
                    return true;
                });

            output.Append(",IN pFlags BIGINT" + Environment.NewLine);
            output.Append(",IN pUpdated DATETIME" + Environment.NewLine);
            output.Append(",IN pDatestamp DATETIME" + Environment.NewLine);
            output.Append(")" + Environment.NewLine);
            output.Append("BEGIN" + Environment.NewLine);
            output.Append("    IF (pUid > 0) THEN" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        IF (pFlags = -2) THEN" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("            DELETE FROM " + tnm + " WHERE Uid = pUid;" + Environment.NewLine);
            output.Append("            SELECT pUid;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        ELSEIF (pFlags = -1) THEN /*select*/" + Environment.NewLine);
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
            output.Append("        ELSE /*update*/" + Environment.NewLine);
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
            output.Append("            SELECT pUid;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        END IF;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("    ELSE /*insert*/" + Environment.NewLine);
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
            output.Append("        SELECT Uid FROM " + tnm + " WHERE Uid = LAST_INSERT_ID();" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("    END IF;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("END;" + Environment.NewLine);

            return output.ToString();

        }

        public string CreateDatabaseTable(Type obj, IDataConnection Connection)
        {
            List<string> ignore = new() { "Uid", "ParentId", "ParentType", "Flags", "Updated", "Datestamp" };

            StringBuilder output = new();

            var tnm = obj.GetTypeInfo().GetMappedName();

            output.Append("CREATE TABLE `" + tnm + "` ( " + Environment.NewLine);
            output.Append("    `Uid` BIGINT NOT NULL AUTO_INCREMENT," + Environment.NewLine);
            output.Append("    `ParentId` BIGINT NULL," + Environment.NewLine);
            output.Append("    `ParentType` VARCHAR(255) NULL," + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .ForEach(p => {
                    output.Append("    `" + p.Name + "` " + ((ODataTypeAttribute)p.GetCustomAttributes(true)[0]).ToString() + " NULL," + Environment.NewLine);
                });

            output.Append("    `Flags` BIGINT NULL," + Environment.NewLine);
            output.Append("    `Updated` DATETIME NULL," + Environment.NewLine);
            output.Append("    `Datestamp` DATETIME NULL," + Environment.NewLine);
            output.Append(" PRIMARY KEY (`Uid`) " + Environment.NewLine);
            output.Append(") " + Environment.NewLine);
            output.Append("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ;" + Environment.NewLine);

            output.Append(Environment.NewLine);
            output.Append(";ALTER TABLE `" + tnm + "` ALTER `ParentId` SET DEFAULT (0) " + Environment.NewLine);
            output.Append(";ALTER TABLE `" + tnm + "` ALTER `ParentType` SET DEFAULT ('') " + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .ForEach(p => {
                    output.Append(";ALTER TABLE `" + tnm + "` ALTER `" + p.Name + "` SET DEFAULT (" + Connection.GetSqlDefaultValueRepresentation(Connection.GetDbDataType(p.PropertyType)) + ") " + Environment.NewLine);
                });

            output.Append(";ALTER TABLE `" + tnm + "` ALTER `Flags` SET DEFAULT (0) " + Environment.NewLine);
            output.Append(";ALTER TABLE `" + tnm + "` ALTER `Updated` SET DEFAULT (CURRENT_TIMESTAMP) " + Environment.NewLine);
            output.Append(";ALTER TABLE `" + tnm + "` ALTER `Datestamp` SET DEFAULT (CURRENT_TIMESTAMP) " + Environment.NewLine);

            return output.ToString();

        }
       
        public string CreateMigrationVersionTable(string DatabaseName)
        {

            StringBuilder output = new();

            output.Append("CREATE TABLE `MigrationVersion` (Version VARCHAR(255) NULL);");
            output.Append("INSERT INTO `MigrationVersion` (Version) VALUES ('0');");

            return output.ToString();

        }
        
        public bool MergeMigrationBackupAndRemove(Type obj, IDataConnection Connection)
        {
            
            var tnm = obj.GetTypeInfo().GetMappedName();

            var ds1 = Connection.Get(CommandType.Text, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '" + Connection.Database + "' AND table_name = '" + tnm + "';", null);
            
            if (Convert.ToInt32(ds1.Tables[0].Rows[0][0]) > 0)
            {
                var ds2 = Connection.Get(CommandType.Text, "SELECT COUNT(`Uid`) FROM `" + tnm + "`;", null);

                if (Convert.ToInt32(ds2.Tables[0].Rows[0][0]) > 0)
                    Connection.Query(CommandType.Text, "RENAME TABLE `" + tnm + "` TO `old_" + tnm + "`;", null);
                else 
                    Connection.Query(CommandType.Text, "DROP TABLE IF EXISTS `" + tnm + "`;", null);

                Connection.Query(CommandType.Text, "DROP PROCEDURE IF EXISTS `" + tnm + "`;", null);

                gd.Clear(ds2);

            }

            gd.Clear(ds1);

            return true;
        }

        public bool MergeMigrationRestoreAndRemove(Type obj, IDataConnection Connection)
        {

            var tnm = obj.GetTypeInfo().GetMappedName();

            List<string> ignore = new() { "Uid", "ParentId", "ParentType", "Flags", "Updated", "Datestamp" };

            var ds1 = Connection.Get(CommandType.Text, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '" + Connection.Database + "' AND table_name = '" + tnm + "';", null);

            if (Convert.ToInt32(ds1.Tables[0].Rows[0][0]) > 0)
            { 
            
                StringBuilder output = new();
                output.Append("SET @QueryString = CONCAT('");
                output.Append("SELECT `Uid`, `ParentId`, `ParentType`, '");

                obj.GetTypeInfo()
                    .GetProperties()
                    .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                    .ToArray()
                    .ForEach(p => {
                        int dbt = Connection.GetDbDataType(p.PropertyType);
                        output.Append(", (CASE WHEN ((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '" + Connection.Database + "' AND table_name = 'old_" + tnm + "' AND column_name = '" + p.Name + "') > 0) THEN '`" + p.Name + "`,' ELSE '" + Connection.GetSqlDefaultValueRepresentation(dbt, true) + " AS `" + p.Name + "`,' END)");
                    });

                output.Append(", '`Flags`, `Updated`, `Datestamp` FROM `old_" + tnm + "`;'); SELECT @QueryString AS 'QueryString';");

                var ds2 = Connection.Get(CommandType.Text, output.ToString(), null);

                string selectString = ds2.Tables[0].Rows[0][0].ToString();
               
                gd.Clear(ds2);

                output = new();
                output.Append("INSERT INTO `" + tnm + "` (" + Environment.NewLine);
                output.Append("Uid," + Environment.NewLine);
                output.Append("ParentId," + Environment.NewLine);
                output.Append("ParentType," + Environment.NewLine);

                obj.GetTypeInfo()
                    .GetProperties()
                    .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                    .ToArray()
                    .ForEach(p => {
                        output.Append(p.Name + "," + Environment.NewLine);
                    });

                output.Append("Flags," + Environment.NewLine);
                output.Append("Updated," + Environment.NewLine);
                output.Append("Datestamp" + Environment.NewLine);
                output.Append(") ");
                output.Append(selectString);

                Connection.Query(CommandType.Text, output.ToString(), null);

            }

            Connection.Query(CommandType.Text, "DROP TABLE IF EXISTS `old_" + tnm + "`;", null);

            gd.Clear(ds1);

            return true;

        }

        public string GetTypeAttributeRepresentation(ODataTypeAttribute e) 
        {
            return e.MySQLDataType switch
            {
                (int)MySqlDbType.VarChar =>         e.DataTypeSize <= 0 ? "CHAR"        : "CHAR(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.VarString =>       e.DataTypeSize <= 0 ? "VARCHAR"     : "VARCHAR(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.TinyText =>        "TINYTEXT",
                (int)MySqlDbType.Text =>            e.DataTypeSize <= 0 ? "TEXT"        : "TEXT(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.MediumText =>      "MEDIUMTEXT",
                (int)MySqlDbType.LongText =>        "LONGTEXT",
                (int)MySqlDbType.TinyBlob =>        "TINYBLOB",
                (int)MySqlDbType.Blob =>            e.DataTypeSize <= 0 ? "BLOB"        : "BLOB(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.MediumBlob =>      "MEDIUMBLOB",
                (int)MySqlDbType.LongBlob =>        "LONGBLOB",
                (int)MySqlDbType.Binary =>          e.DataTypeSize <= 0 ? "BINARY"      : "BINARY(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.VarBinary =>       e.DataTypeSize <= 0 ? "VARBINARY"   : "VARBINARY(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.Bit =>             e.DataTypeSize <= 0 ? "BIT"         : "BIT(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.Byte =>            e.DataTypeSize <= 0 ? "TINYINT"     : "TINYINT(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbTypeExt.Boolean =>      "BOOLEAN",  //TINYINT(1)
                (int)MySqlDbType.Int16 =>           e.DataTypeSize <= 0 ? "SMALLINT"    : "SMALLINT(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.Int24 =>           e.DataTypeSize <= 0 ? "MEDIUMINT"   : "MEDIUMINT(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.Int32 =>           e.DataTypeSize <= 0 ? "INT"         : "INT(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.Int64 =>           e.DataTypeSize <= 0 ? "BIGINT"      : "BIGINT(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.UInt16 =>          e.DataTypeSize <= 0 ? "SMALLINT"    : "SMALLINT(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.UInt24 =>          e.DataTypeSize <= 0 ? "MEDIUMINT"   : "MEDIUMINT(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.UInt32 =>          e.DataTypeSize <= 0 ? "INT"         : "INT(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.UInt64 =>          e.DataTypeSize <= 0 ? "BIGINT"      : "BIGINT(" + e.DataTypeSize.ToString() + ")",
                (int)MySqlDbType.Float =>           "FLOAT(" + e.DataTypeSize.ToString() + ", " + e.DataTypePlaces.ToString() + ")",
                (int)MySqlDbType.Double =>          "DOUBLE(" + e.DataTypeSize.ToString() + ", " + e.DataTypePlaces.ToString() + ")",
                (int)MySqlDbType.Decimal =>         "DECIMAL(" + e.DataTypeSize.ToString() + ", " + e.DataTypePlaces.ToString() + ")",
                (int)MySqlDbType.Date =>            "DATE",
                (int)MySqlDbType.DateTime =>        string.IsNullOrEmpty(e.DataTypeFormat) ? "DATETIME"     : "DATETIME(" + e.DataTypeFormat.ToString() + ")",
                (int)MySqlDbType.Timestamp =>       string.IsNullOrEmpty(e.DataTypeFormat) ? "TIMESTAMP"    : "TIMESTAMP(" + e.DataTypeFormat.ToString() + ")",
                (int)MySqlDbType.Time =>            string.IsNullOrEmpty(e.DataTypeFormat) ? "TIME"         : "TIME(" + e.DataTypeFormat.ToString() + ")",
                (int)MySqlDbType.Year =>            "YEAR",
                (int)MySqlDbType.Guid               => "BINARY(16)",
                _ =>                                "VARCHAR",
            };
        }
      
        public DbDataAdapter GetNewDataAdapter()
        {
            return new MySqlDataAdapter();
        }

        public DbCommand GetNewCommand(string cmdText)
        {
            return new MySqlCommand(cmdText);
        }

        public DbConnection GetNewConnection(IDataConnection Connection)
        {
            return new MySqlConnection(Connection.ToString());
        }

        public DbParameter CreateParam(int datatype, ParameterDirection direction, object value, string paramname)
        {
            try
            {
                return gd.CreateParam((MySqlDbType)datatype, direction, value, paramname);
            }
            catch
            {
                return default;
            }
        }

        public DbParameter CreateParam(int datatype, object value, string paramname)
        {
            try
            {
                return gd.CreateParam((MySqlDbType)datatype, value, paramname);
            }
            catch
            {
                return null;
            }
        }

        public string GetSqlDefaultValueRepresentation(int datatype, bool encapsulate = false)
        {

            return (datatype) switch
            {
                (int)MySqlDbType.VarChar        => encapsulate ? "''''" : "''",
                (int)MySqlDbType.VarString      => encapsulate ? "''''" : "''",
                (int)MySqlDbType.TinyText       => encapsulate ? "''''" : "''",
                (int)MySqlDbType.MediumText     => encapsulate ? "''''" : "''",
                (int)MySqlDbType.LongText       => encapsulate ? "''''" : "''",
                (int)MySqlDbType.Text           => encapsulate ? "''''" : "''",
                (int)MySqlDbType.String         => encapsulate ? "''''" : "''",
                (int)MySqlDbType.Bit            => "0",
                (int)MySqlDbType.Int16          => "0",
                (int)MySqlDbType.Int24          => "0",
                (int)MySqlDbType.Int32          => "0",
                (int)MySqlDbType.Int64          => "0",
                (int)MySqlDbType.NewDecimal     => "0.00",
                (int)MySqlDbType.Decimal        => "0.00",
                (int)MySqlDbType.Float          => "0.00",
                (int)MySqlDbType.Date           => "CURRENT_DATE",
                (int)MySqlDbType.DateTime       => "CURRENT_TIMESTAMP",
                (int)MySqlDbType.Time           => encapsulate ? "''00:00:00''" : "'00:00:00'",
                (int)MySqlDbType.Timestamp      => "CURRENT_TIMESTAMP",
                (int)MySqlDbType.Binary         => "0x00",
                (int)MySqlDbType.TinyBlob       => "0x00",
                (int)MySqlDbType.VarBinary      => "0x00",
                _                               => encapsulate ? "''''" : "''",
            };

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
                        parameters = parameters.Append(Connection.CreateParam((int)MySqlDbType.Int32, value, "@Value" + uparam));
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

                parameters = parameters.Append(gd.CreateParam((MySqlDbType)xxDbType, value, "@Value" + uparam));

            }

            return output.ToString();

        }

        public int GetDbDataType(IDataConnection Connection, Type t)
        {

            int _result;

            //Grab the embbed type if nullable.
            t = t.NullSafeType();

            if (t.BaseType.Name == "Enum")
                _result = (int)MySqlDbType.Int32;
            else
                _result = (t.Name.ToString()) switch
                {
                    "String"    => (int)MySqlDbType.VarString,
                    "Int16"     => (int)MySqlDbType.Int16,
                    "Int32"     => (int)MySqlDbType.Int32,
                    "Int64"     => (int)MySqlDbType.Int64,
                    "UInt16"    => (int)MySqlDbType.UInt16,
                    "UInt32"    => (int)MySqlDbType.UInt32,
                    "UInt64"    => (int)MySqlDbType.UInt64,
                    "Decimal"   => (int)MySqlDbType.Decimal,
                    "Double"    => (int)MySqlDbType.Float,
                    "Single"    => (int)MySqlDbType.Float,
                    "DateTime"  => (int)MySqlDbType.DateTime,
                    "Date"      => (int)MySqlDbType.Date,
                    "Time"      => (int)MySqlDbType.Time,
                    "TimeSpan"  => (int)MySqlDbType.Time,
                    "Boolean"   => (int)MySqlDbType.Bit,
                    "Byte[]"    => (int)MySqlDbType.Binary,
                    _           => (int)MySqlDbType.VarString,
                };

            return _result;

        }

        public bool DeleteDatabase(IDataConnection Connection)
        {

            StringBuilder   output  = new();
            IDataConnection clone   = Connection.Clone();

            output.Append("DROP DATABASE IF EXISTS " + Connection.Database + ";" + Environment.NewLine);

            try
            {
                clone.Query(CommandType.Text, output.ToString(), null);
            }
            catch (Exception)
            {
                return false;
            }

            return true;

        }

        public string[] GetTables(IDataConnection Connection)
        {

            List<string>    output;
            StringBuilder   input = new();
            IDataConnection clone = Connection.Clone();

            input.Append("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + Connection.Database + "' AND table_name != 'MigrationVersion';");

            try
            {

                DataSet dt = clone.Get(CommandType.Text, input.ToString(), null);

                if (dt.Tables[0].Rows.Count <= 0)
                    return Array.Empty<string>();
                else
                {
                    output = new();
                    dt.Tables[0].Rows.ForEach(r => {
                        if (r[0].ToString() != "MigrationVersion".ToLower())
                            output.Add(r[0].ToString());
                    });
                }
            }
            catch (Exception)
            {
                return Array.Empty<string>();
            }

            return output.ToArray();
        }

        public bool TestDatabase(IDataConnection Connection, out DataTable record)
        {

            record = null;

            StringBuilder   output  = new();
            IDataConnection clone   = Connection.Clone();

            clone.ConnectionType = OConnectionType.MySqlStandardNoDatabase;

            output.Append("SELECT * FROM information_schema.tables WHERE table_schema = '" + Connection.Database + "';");

            try
            {

                DataSet dt = clone.Get(CommandType.Text, output.ToString(), null);

                if (dt.Tables[0].Rows.Count <= 0)
                    return false;
                else
                    record = dt.Tables[0];

                return true;

            }
            catch (Exception)
            {
                return false;
            }

        }

        public bool CreateDatabase(IDataConnection Connection, string filePath)
        {

            StringBuilder   output = new();
            IDataConnection clone = Connection.Clone();

            clone.Database = "sys";

            output.Append("CREATE DATABASE IF NOT EXISTS " + Connection.Database + ";" + Environment.NewLine);
            output.Append("GRANT ALL ON " + Connection.Database + " TO '" + Connection.Username + "'@'%';" + Environment.NewLine);


            try
            {
                return clone.Query(CommandType.Text, output.ToString(), null);
            }
            catch (Exception)
            {
                return false;
            }

        }

        public string TakeSkipBuildString(ODataTakeSkip e)
        {

            StringBuilder output = new();

            output.Append("LIMIT " + e.Take.ToString() + ", " + e.Skip.ToString());

            return output.ToString();

        }

        public string StuffFunctionBuildString(ODataStuffFunction e)
        {

            StringBuilder output = new();

            output.Append(" INSERT((");
            output.Append(e.Query.ToString());
            output.Append("), " + e.StartPosistion.ToString() + ", " + e.NumberOfChars.ToString() + ", '" + e.ReplacementExpression + "')");

            return output.ToString();

        }

        public string OrderBuildString(ODataOrder e, string prefix = "")
        {
            StringBuilder output = new();

            if (e.Random)
                output.Append("RAND()");
            else
            {
                if (e.Function != ODataFunction.NONE)
                    output.Append(e.Function.ToString() + "(");

                output.Append(prefix + e.Column.Name);

                if (e.Function != ODataFunction.NONE)
                    output.Append(')');

                switch (e.Order)
                {
                    case ODataOrderType.ASC:
                        output.Append(" ASC");
                        break;
                    case ODataOrderType.DESC:
                        output.Append(" DESC");
                        break;
                    case ODataOrderType.NONE:
                        output.Append(string.Empty);
                        break;
                    default:
                        output.Append(string.Empty);
                        break;
                }
            }

            return output.ToString();
        }

        public string HavingSetBuildString(ODataHavingSet e, out IEnumerable<DbParameter> parameters, bool UseFieldPrefixing = false)
        {

            parameters = Array.Empty<DbParameter>();

            StringBuilder output = new();

            if (e.Function != ODataFunction.NONE)
                output.Append(e.Function.ToString() + "(");

            if (UseFieldPrefixing)
                output.Append(e.Column.ReflectedType.GetMappedName() + ".");

            output.Append("`" + e.Column.Name + "`");

            if (e.Function != ODataFunction.NONE)
                output.Append(')');

            switch (e.Operator)
            {
                case ODataOperator.EQUAL:
                    output.Append(" = ");
                    break;
                case ODataOperator.GREATER_THAN:
                    output.Append(" > ");
                    break;
                case ODataOperator.LESS_THAN:
                    output.Append(" < ");
                    break;
                case ODataOperator.GREATER_THAN_OR_EQUAL:
                    output.Append(" >= ");
                    break;
                case ODataOperator.LESS_THAN_OR_EQUAL:
                    output.Append(" <= ");
                    break;
                case ODataOperator.NOT_EQUAL:
                    output.Append(" <> ");
                    break;
                case ODataOperator.BETWEEN:
                    output.Append(" BETWEEN ");
                    break;
                case ODataOperator.LIKE:
                    output.Append(" LIKE ");
                    break;
                case ODataOperator.IN:
                    output.Append(" IN (");
                    break;
                case ODataOperator.NOT_BETWEEN:
                    output.Append(" NOT BETWEEN ");
                    break;
                case ODataOperator.NOT_LIKE:
                    output.Append(" NOT LIKE ");
                    break;
                case ODataOperator.NOT_IN:
                    output.Append(" NOT IN (");
                    break;
                case ODataOperator.IS:
                    output.Append(" IS ");
                    break;
                case ODataOperator.IS_NOT:
                    output.Append(" IS NOT ");
                    break;
                case ODataOperator.NONE:
                    output.Append(string.Empty);
                    break;
                default:
                    output.Append(string.Empty);
                    break;
            }

            if (e.Value.GetType().Name == "RuntimePropertyInfo")
            {
                output.Append(ODataContext.Connection().GetSqlRepresentation(-1, "`" + ((PropertyInfo)e.Value).ReflectedType.GetMappedName() + "`.`" + ((PropertyInfo)e.Value).Name + "`", out IEnumerable<DbParameter> pta));
                parameters = parameters.Concat(pta);
            }
            else
            {
                output.Append(ODataContext.Connection().GetSqlRepresentation(e.Column, e.Value, out IEnumerable<DbParameter> pta));
                parameters = parameters.Concat(pta);
            }

            if (e.Operator == ODataOperator.IN || e.Operator == ODataOperator.NOT_IN)
                output.Append(") ");

            switch (e.FollowBy)
            {
                case ODataFollower.NONE:
                    output.Append(string.Empty);
                    break;
                case ODataFollower.AND:
                    output.Append(" AND ");
                    break;
                case ODataFollower.AND_NOT:
                    output.Append(" AND NOT ");
                    break;
                case ODataFollower.NOT:
                    output.Append(" NOT ");
                    break;
                case ODataFollower.OR:
                    output.Append(" OR ");
                    break;
                case ODataFollower.OR_NOT:
                    output.Append(" OR NOT ");
                    break;
                default:
                    output.Append(string.Empty);
                    break;
            }

            return output.ToString();

        }

        public string GroupSetBuildString(ODataGroupSet e, bool UseFieldPrefixing = false)
        {

            StringBuilder output = new();

            if (UseFieldPrefixing)
                output.Append("`" + e.Column.ReflectedType.GetMappedName() + "`.");

            output.Append("`" + e.Column.Name + "`");

            return output.ToString();

        }

        public string ForPathBuildString(ODataForPath e)
        {

            throw new ODataException("MySql Unsupported for ForPathBuildString()");

        }

        public string ApplySetBuildString(ODataApplySet e)
        {

            StringBuilder output = new();

            if (e.ApplyType == ODataApplyType.CROSS)
                output.Append("JOIN LATERAL (");

            if (e.ApplyType == ODataApplyType.OUTER)
                output.Append("LEFT JOIN LATERAL (");

            output.Append(e.Query.ToString());

            output.Append(") " + e.Alias + " ON 1=1");

            return output.ToString();

        }

        public string CaseBuildString(ODataCase e, out IEnumerable<DbParameter> parameters, bool UseFieldPrefixing = false)
        {

            parameters = Array.Empty<DbParameter>();

            StringBuilder output = new();

            output.Append("WHEN (");

            foreach (var condition in e.When)
            {
                output.Append(condition.ToString(out IEnumerable<DbParameter> pta, UseFieldPrefixing));
                parameters = parameters.Concat(pta);
            }

            output.Append(") THEN ");

            if (e.Then.GetType().Name == "RuntimePropertyInfo")
            {
                output.Append(ODataContext.Connection().GetSqlRepresentation(-1, "`" + ((PropertyInfo)e.Then).Name + "`", out IEnumerable<DbParameter> pta));
                parameters = parameters.Concat(pta);
            }
            else if (e.Then.GetType().Name == "ODataPropertyInfo")
            {
                output.Append(ODataContext.Connection().GetSqlRepresentation(-1, ((PropertyInfo)e.Then).Name, out IEnumerable<DbParameter> pta));
                parameters = parameters.Concat(pta);
            }
            else
            {
                output.Append(ODataContext.Connection().GetSqlRepresentation(ODataContext.Connection().GetDbDataType(e.Then.GetType()), e.Then, out IEnumerable<DbParameter> pta));
                parameters = parameters.Concat(pta);
            }

            if (e.Else != null)
            {
                output.Append(" ELSE ");
                if (e.Else.GetType().Name == "RuntimePropertyInfo")
                {
                    output.Append(ODataContext.Connection().GetSqlRepresentation(-1, "`" + ((PropertyInfo)e.Else).Name + "`", out IEnumerable<DbParameter> pta));
                    parameters = parameters.Concat(pta);
                }
                else if (e.Else.GetType().Name == "ODataPropertyInfo")
                {
                    output.Append(ODataContext.Connection().GetSqlRepresentation(-1, ((PropertyInfo)e.Else).Name, out IEnumerable<DbParameter> pta));
                    parameters = parameters.Concat(pta);
                }
                else
                {
                    output.Append(ODataContext.Connection().GetSqlRepresentation(ODataContext.Connection().GetDbDataType(e.Else.GetType()), e.Else, out IEnumerable<DbParameter> pta));
                    parameters = parameters.Concat(pta);
                }
            }

            return output.ToString();

        }

        public string ConditionBuildString(ODataCondition e, out IEnumerable<DbParameter> parameters, bool UsePrefix = false)
        {
            parameters = Array.Empty<DbParameter>();

            StringBuilder output = new();

            switch (e.Prefix)
            {
                case ODataFollower.NONE:
                    output.Append(string.Empty);
                    break;
                case ODataFollower.AND:
                    output.Append("AND ");
                    break;
                case ODataFollower.AND_NOT:
                    output.Append("AND NOT ");
                    break;
                case ODataFollower.NOT:
                    output.Append("NOT ");
                    break;
                case ODataFollower.OR:
                    output.Append("OR ");
                    break;
                case ODataFollower.OR_NOT:
                    output.Append("OR NOT ");
                    break;
                default:
                    output.Append(string.Empty);
                    break;
            }

            if (e.Container == ODataConditionContainer.OPEN)
                output.Append("( ");

            if (e.FieldFunction != ODataFunction.NONE)
                output.Append(e.FieldFunction.ToString() + "(");

            if (e.Case != null && e.SubQuery == null)
            {
                output.Append("(CASE ");

                foreach (var c in e.Case)
                {
                    output.Append(c.ToString(out IEnumerable<DbParameter> pta, UsePrefix) + " ");
                    parameters = parameters.Concat(pta);
                }

                output.Remove(output.Length - 1, 1);

                output.Append(" END) ");
            }
            else if (e.Case == null && e.SubQuery != null)
            {
                output.Append(e.SubQuery.ToString());
            }
            else
            {
                if (e.Column == null)
                    output.Append(e.ColumnText);
                else
                {
                    if (e.Column.GetType().Name == "ODataPropertyInfo")
                        output.Append(e.Column.Name);
                    else
                    {
                        if (UsePrefix)
                            output.Append(e.Column.ReflectedType.GetMappedName() + ".`" + e.Column.Name + "`");
                        else
                            output.Append("`" + e.Column.Name + "`");
                    }
                }
            }

            if (e.FieldFunction != ODataFunction.NONE)
                output.Append(") ");

            switch (e.Operator)
            {
                case ODataOperator.EQUAL:
                    output.Append(" = ");
                    break;
                case ODataOperator.GREATER_THAN:
                    output.Append(" > ");
                    break;
                case ODataOperator.LESS_THAN:
                    output.Append(" < ");
                    break;
                case ODataOperator.GREATER_THAN_OR_EQUAL:
                    output.Append(" >= ");
                    break;
                case ODataOperator.LESS_THAN_OR_EQUAL:
                    output.Append(" <= ");
                    break;
                case ODataOperator.NOT_EQUAL:
                    output.Append(" <> ");
                    break;
                case ODataOperator.BETWEEN:
                    output.Append(" BETWEEN ");
                    break;
                case ODataOperator.LIKE:
                    output.Append(" LIKE ");
                    break;
                case ODataOperator.IN:
                    output.Append(" IN (");
                    break;
                case ODataOperator.NOT_BETWEEN:
                    output.Append(" NOT BETWEEN ");
                    break;
                case ODataOperator.NOT_LIKE:
                    output.Append(" NOT LIKE ");
                    break;
                case ODataOperator.NOT_IN:
                    output.Append(" NOT IN (");
                    break;
                case ODataOperator.IS:
                    output.Append(" IS ");
                    break;
                case ODataOperator.IS_NOT:
                    output.Append(" IS NOT ");
                    break;
                case ODataOperator.NONE:
                    output.Append(string.Empty);
                    break;
                default:
                    output.Append(string.Empty);
                    break;
            }

            if (e.ValueFunction != ODataFunction.NONE)
                output.Append(e.FieldFunction.ToString() + "(");

            foreach (object v in e.Values)
            {

                if (e.Operator == ODataOperator.LIKE || e.Operator == ODataOperator.NOT_LIKE)
                {
                    switch (e.LikeOperator)
                    {
                        case ODataLikeOperator.NONE:
                            output.Append("'" + gd.SafeString(v.ToString()) + "',");
                            break;
                        case ODataLikeOperator.STARTS_WITH:
                            output.Append("'" + gd.SafeString(v.ToString()) + "%',");
                            break;
                        case ODataLikeOperator.ENDS_WITH:
                            output.Append("'%" + gd.SafeString(v.ToString()) + "',");
                            break;
                        case ODataLikeOperator.CONTAINS:
                            output.Append("'%" + gd.SafeString(v.ToString()) + "%',");
                            break;
                        default:
                            output.Append("'" + gd.SafeString(v.ToString()) + "',");
                            break;
                    }
                }
                else
                {

                    if (v == null)
                    {
                        output.Append(ODataContext.Connection().GetSqlRepresentation(null, v, out IEnumerable<DbParameter> pta) + ",");
                        parameters = parameters.Concat(pta);
                    }
                    else
                    {
                        if (e.Case != null || e.SubQuery != null)
                        {
                            output.Append(ODataContext.Connection().GetSqlRepresentation(ODataContext.Connection().GetDbDataType(v.GetType()), v, out IEnumerable<DbParameter> pta) + ",");
                            parameters = parameters.Concat(pta);
                        }
                        else
                        {
                            if (v.GetType().Name == "RuntimePropertyInfo")
                            {
                                output.Append(ODataContext.Connection().GetSqlRepresentation(-1, ((PropertyInfo)v).ReflectedType.GetMappedName() + ".`" + ((PropertyInfo)v).Name, out IEnumerable<DbParameter> pta) + "`,");
                                parameters = parameters.Concat(pta);
                            }
                            else if (v.GetType().Name == "ODataPropertyInfo")
                            {
                                output.Append(ODataContext.Connection().GetSqlRepresentation(-1, ((ODataPropertyInfo)v).Name, out IEnumerable<DbParameter> pta) + ",");
                                parameters = parameters.Concat(pta);
                            }
                            else
                            {
                                output.Append(ODataContext.Connection().GetSqlRepresentation(e.Column, v, out IEnumerable<DbParameter> pta) + ",");
                                parameters = parameters.Concat(pta);
                            }
                        }
                    }

                }
            }

            output.Remove(output.Length - 1, 1);

            if (e.Operator == ODataOperator.IN || e.Operator == ODataOperator.NOT_IN)
                output.Append(") ");

            if (e.ValueFunction != ODataFunction.NONE)
                output.Append(") ");

            if (e.Container == ODataConditionContainer.CLOSE)
                output.Append(" ) ");

            switch (e.FollowBy)
            {
                case ODataFollower.NONE:
                    output.Append(string.Empty);
                    break;
                case ODataFollower.AND:
                    output.Append(" AND ");
                    break;
                case ODataFollower.AND_NOT:
                    output.Append(" AND NOT ");
                    break;
                case ODataFollower.NOT:
                    output.Append(" NOT ");
                    break;
                case ODataFollower.OR:
                    output.Append(" OR ");
                    break;
                case ODataFollower.OR_NOT:
                    output.Append(" OR NOT ");
                    break;
                default:
                    output.Append(string.Empty);
                    break;
            }

            return output.ToString();

        }

        public string FieldSetUpdateBuildString(ODataFieldSet e, out IEnumerable<DbParameter> parameters)
        {
            parameters = Array.Empty<DbParameter>();

            StringBuilder output = new();

            output.Append("`" + e.Column.Name + "`");

            if (e.UpdateOperator == ODataUpdateOperator.EQUAL)
                output.Append(" = ");

            if (e.UpdateOperator == ODataUpdateOperator.PLUS_EQUAL)
                output.Append(" += ");

            output.Append(ODataContext
                .Connection()
                .GetSqlRepresentation(e.Column, e.NewValue, out IEnumerable<DbParameter> pta)
            );

            parameters = parameters.Concat(pta);

            return output.ToString();

        }

        public string FieldSetInsertBuildString(ODataFieldSet e, out IEnumerable<DbParameter> parameters)
        {
            parameters = Array.Empty<DbParameter>();

            StringBuilder output = new();

            output.Append("`" + e.Column.Name + "`");

            return output.ToString();
        }

        public string FieldSetSelectBuildString(ODataFieldSet e, out IEnumerable<DbParameter> parameters, bool UseFieldPrefixing = false, bool UseFieldDefaultAlias = false)
        {
            parameters = Array.Empty<DbParameter>();

            StringBuilder output = new();

            if (e.Function != ODataFunction.NONE)
                output.Append(e.Function.ToString() + "(");

            if (e.Cast != ODataCast.NONE)
                output.Append("CAST(");

            if (e.Prefix != null)
            {
                output.Append(ODataContext.Connection().GetSqlRepresentation(ODataContext.Connection().GetDbDataType(e.Prefix.GetType()), e.Prefix, out IEnumerable<DbParameter> pta) + " + ");
                parameters = parameters.Concat(pta);
            }

            if (e.Case != null && e.Column == null && e.SubQuery == null)
            {

                output.Append("CASE ");

                foreach (var c in e.Case)
                {
                    output.Append(c.ToString(out IEnumerable<DbParameter> pta, UseFieldPrefixing) + " ");
                    parameters = parameters.Concat(pta);
                }

                output.Remove(output.Length - 1, 1);

                output.Append(" END ");

                if (e.Cast != ODataCast.NONE)
                    output.Append(" AS " + e.Cast.ToString() + ") ");

                if (e.Suffix != null)
                {
                    output.Append("+ " + ODataContext.Connection().GetSqlRepresentation(ODataContext.Connection().GetDbDataType(e.Suffix.GetType()), e.Suffix, out IEnumerable<DbParameter> pta) + " ");
                    parameters = parameters.Concat(pta);
                }

                if (e.Function != ODataFunction.NONE)
                    output.Append(") ");

                if (!string.IsNullOrEmpty(e.Alias))
                    output.Append("AS `" + e.Alias + "`");

            }

            if (e.Case == null && e.Column != null && e.SubQuery == null)
            {

                if (UseFieldPrefixing)
                    output.Append(e.Column.ReflectedType.GetMappedName() + ".");

                output.Append("`" + e.Column.Name + "`");

                if (e.Cast != ODataCast.NONE)
                    output.Append(" AS " + e.Cast.ToString() + ") ");

                if (e.Suffix != null)
                {
                    output.Append("+ " + ODataContext.Connection().GetSqlRepresentation(ODataContext.Connection().GetDbDataType(e.Suffix.GetType()), e.Suffix, out IEnumerable<DbParameter> pta) + " ");
                    parameters = parameters.Concat(pta);
                }

                if (e.Function != ODataFunction.NONE)
                    output.Append(") ");

                if (UseFieldDefaultAlias)
                    output.Append(" AS `" + e.Column.ReflectedType.GetMappedName() + "." + e.Column.Name + "`");

                if (!UseFieldDefaultAlias && !string.IsNullOrEmpty(e.Alias))
                    output.Append(" AS `" + e.Alias + "`");

            }

            if (e.Case == null && e.Column == null && e.SubQuery != null)
            {

                output.Append('(');
                output.Append(e.SubQuery.ToString());
                output.Append(") ");

                if (e.Cast != ODataCast.NONE)
                    output.Append(" AS " + e.Cast.ToString() + ") ");

                if (e.Suffix != null)
                {
                    output.Append("+ " + ODataContext.Connection().GetSqlRepresentation(ODataContext.Connection().GetDbDataType(e.Suffix.GetType()), e.Suffix, out IEnumerable<DbParameter> pta) + " ");
                    parameters = parameters.Concat(pta);
                }

                if (e.Function != ODataFunction.NONE)
                    output.Append(") ");

                if (!string.IsNullOrEmpty(e.Alias))
                    output.Append(" AS `" + e.Alias + "`");
            }

            if (e.Case == null && e.Column == null && e.SubQuery == null && e.NewValue != null)
            {

                output.Append(ODataContext.Connection().GetSqlRepresentation(e.DataType, e.NewValue, out IEnumerable<DbParameter> pta));
                parameters = parameters.Concat(pta);

                if (e.Cast != ODataCast.NONE)
                    output.Append(" AS " + e.Cast.ToString() + ") ");

                if (e.Suffix != null)
                {
                    output.Append("+ " + ODataContext.Connection().GetSqlRepresentation(ODataContext.Connection().GetDbDataType(e.Suffix.GetType()), e.Suffix, out IEnumerable<DbParameter> ptab) + " ");
                    parameters = parameters.Concat(ptab);
                }

                if (e.Function != ODataFunction.NONE)
                    output.Append(") ");

                if (!string.IsNullOrEmpty(e.Alias))
                    output.Append(" AS `" + e.Alias + "`");

            }

            return output.ToString();

        }

        public string InsertQueryBuildString(ODataInsertQuery e)
        {

            StringBuilder output = new();

            var ToTableName = e.To.GetMappedName();

            //if (e.UseIdentityInsert)
            //    output.Append("SET IDENTITY_INSERT " + ToTableName + " ON;");

            output.Append("INSERT INTO " + ToTableName + " ( ");

            e.Fields.ForEach(f => {
                output.Append(f.ToInsertString(out IEnumerable<DbParameter> pta) + ", ");
                e.Parameters = e.Parameters.Concat(pta);
            });

            output.Remove(output.Length - 2, 2);

            output.Append(')');

            if (e.ValueSets.Count <= 0 && e.Select == null)
            {
                output.Append(" VALUES (");
                e.Fields.ForEach(field => { output.Append(field.NewValue + ", "); });
                output.Remove(output.Length - 2, 2);
                output.Append(')');
            }

            if (e.ValueSets.Count > 0 && e.Select == null)
            {
                output.Append(" VALUES ");
                e.ValueSets.ForEach(values => {
                    output.Append('(');
                    values.ForEach(field => { output.Append(field.NewValue + ", "); });
                    output.Remove(output.Length - 2, 2);
                    output.Append("), ");
                });
                output.Remove(output.Length - 2, 2);
            }

            if (e.ValueSets.Count <= 0 && e.Select != null)
                output.Append(e.Select.ToString());

            //if (e.UseIdentityInsert)
            //    output.Append("SET IDENTITY_INSERT " + ToTableName + " OFF;");

            return output.ToString();

        }

        public string DeleteQueryBuildString(ODataDeleteQuery e)
        {
            StringBuilder output = new();

            output.Append("DELETE FROM " + e.From.GetMappedName());

            if (e.Where != null)
            {
                output.Append(" WHERE ");
                e.Where.ForEach(condition => {
                    output.Append(condition.ToString(out IEnumerable<DbParameter> pta, false));
                    e.Parameters = e.Parameters.Concat(pta);
                });
            }

            return output.ToString();
        }

        public string UpdateQueryBuildString(ODataUpdateQuery e)
        {
            StringBuilder output = new();

            var FromTableName = e.From.GetMappedName();

            output.Append("UPDATE " + FromTableName + " SET ");

            if (e.Fields != null)
                e.Fields.ForEach(f => {
                    output.Append(f.ToUpdateString(out IEnumerable<DbParameter> pta) + ", ");
                    e.Parameters = e.Parameters.Concat(pta);
                });

            output.Remove(output.Length - 2, 2);

            if (e.Joins != null || e.Applies != null)
            {

                output.Append(" FROM " + FromTableName + " ");

                if (e.Joins != null)
                    e.Joins.ForEach(j => {
                        var JoinTableName = j.Join.GetMappedName();
                        output.Append(" " + j.JoinType.ToString().Replace("_", " ") + " JOIN " + JoinTableName + " " + JoinTableName + " ON " + JoinTableName + ".[" + j.JoinOnField.Name + "] = " + j.JoinEqualsField.ReflectedType.GetMappedName() + ".[" + j.JoinEqualsField.Name + "]");
                    });

                if (e.Applies != null)
                    e.Applies.ForEach(a => { output.Append(" " + a.ToString()); });

            }

            if (e.Where != null)
            {
                output.Append(" WHERE ");
                e.Where.ForEach(condition => {
                    output.Append(condition.ToString(out IEnumerable<DbParameter> pta, (e.Joins != null || e.Applies != null || e.UseFieldPrefixing)));
                    e.Parameters = e.Parameters.Concat(pta);
                });
            }

            return output.ToString();

        }

        public string SelectQueryBuildString(ODataSelectQuery e)
        {

            StringBuilder output = new();

            if (e.IsSubQuery)
                output.Append('(');

            output.Append("SELECT ");

            if (e.Stuff != null)
            {
                output.Append(e.Stuff.ToString() + " ");
                e.Parameters = e.Parameters.Concat(e.Stuff.Query.Parameters);
            }
            else
            {

                if (e.Distinct)
                    output.Append("DISTINCT ");

                if (e.ForPath != null && e.ForPath.PathType == ODataForPathType.JSON) 
                {
                    output.Append("JSON_ARRAYAGG(JSON_OBJECT( ");
                }

                if (e.Fields != null)
                    e.Fields.ForEach(f => {

                        if (e.ForPath != null && e.ForPath.PathType == ODataForPathType.JSON) {
                            e.UseFieldPrefixing = false;
                            e.UseFieldDefaultAlias = false;
                            output.Append("\"" + f.Column.Name + "\", ");
                        }

                        output.Append(f.ToSelectString(out IEnumerable<DbParameter> pta, e.UseFieldPrefixing, e.UseFieldDefaultAlias) + ", ");
                        
                        e.Parameters = e.Parameters.Concat(pta);
                    });

                if (e.Joins != null && e.IncludeJoinFields)
                    e.Joins.ForEach(j =>
                    {

                        if (j.Join != null && j.JoinQuery == null)
                            j.Join.GetFieldSets(ODataFieldSetType.SELECT).ForEach(f => {
                               
                                //if (e.ForPath != null && e.ForPath.PathType == ODataForPathType.JSON)
                                //{
                                //    e.UseFieldPrefixing = false;
                                //    e.UseFieldDefaultAlias = false;
                                //    output.Append("\"" + f.Column.Name + "\", ");
                                //}

                                output.Append(f.ToSelectString(out IEnumerable<DbParameter> pta, true, true) + ", ");
                                
                                e.Parameters = e.Parameters.Concat(pta);
                            });

                        if (j.Join == null && j.JoinQuery != null)
                            j.JoinQuery.Fields.ForEach(f => {
                                
                                if (e.ForPath != null && e.ForPath.PathType == ODataForPathType.JSON)
                                {
                                    j.JoinQuery.UseFieldPrefixing = false;
                                    j.JoinQuery.UseFieldDefaultAlias = false;
                                    output.Append("\"" + f.Column.Name + "\", ");
                                }

                                output.Append(f.ToSelectString(out IEnumerable<DbParameter> pta, j.JoinQuery.UseFieldPrefixing, j.JoinQuery.UseFieldDefaultAlias) + ", ");
                               
                                e.Parameters = e.Parameters.Concat(pta);
                            });

                    });

                if (e.Applies != null && e.IncludeApplyFields)
                    e.Applies.ForEach(a => { 
                        a.Query.Fields.ForEach(f => {
                            output.Append(a.Alias + ".[" + a.Alias + "." + f.Column.Name + "] AS [" + a.Alias + "." + f.Column.Name + "], "); 
                        }); 
                    });

                output.Remove(output.Length - 2, 2);

                if (e.ForPath != null && e.ForPath.PathType == ODataForPathType.JSON)
                {
                    output.Append(")) AS JSONOUT ");
                }

                if (e.From != null)
                {

                    var FromTableName = e.From.GetMappedName();
                    output.Append(" FROM " + FromTableName + " " + FromTableName);

                    if (e.Joins != null)
                        e.Joins.ForEach(j =>
                        {

                            if (j.Join != null && j.JoinQuery == null)
                            {
                                var JoinTableName = j.Join.GetMappedName();
                                output.Append(" " + j.JoinType.ToString().Replace("_", " ") + " JOIN " + JoinTableName + " " + JoinTableName + " ON ");
                            }

                            if (j.Join == null && j.JoinQuery != null)
                                output.Append(" " + j.JoinType.ToString().Replace("_", " ") + " JOIN ( " + j.JoinQuery.ToString() + " ) " + j.JoinQuery.From.GetMappedName() + " ON ");

                            if (j.JoinConditions != null && j.JoinConditions.Length > 0)
                            {
                                j.JoinConditions.ForEach(condition => {
                                    output.Append(condition.ToString(out IEnumerable<DbParameter> pta, true));
                                    e.Parameters = e.Parameters.Concat(pta);
                                });
                            }
                            else
                            {
                                var JoinTableName = string.Empty;

                                if (j.Join != null && j.JoinQuery == null)
                                    JoinTableName = j.Join.GetMappedName();

                                if (j.Join == null && j.JoinQuery != null)
                                    JoinTableName = j.JoinQuery.From.GetMappedName();

                                output.Append(JoinTableName + ".[" + j.JoinOnField.Name + "] = " + j.JoinEqualsField.ReflectedType.GetMappedName() + ".[" + j.JoinEqualsField.Name + "]");

                            }

                        });

                    if (e.Applies != null)
                        e.Applies.ForEach(a => {
                            output.Append(" " + a.ToString());
                            e.Parameters = e.Parameters.Concat(a.Query.Parameters);
                        });

                    output.Append(" WHERE ");

                    if (!e.IncludeRemoved)
                        output.Append("(" + FromTableName + ".`Flags` >= 0) AND (" + FromTableName + ".`Flags` & " + ((long)ODataFlags.Deleted).ToString() + ") != " + ((long)ODataFlags.Deleted).ToString() + " ");

                    if (e.Where != null && !e.IncludeRemoved)
                        output.Append(" AND ");

                    if (e.Where != null)
                        e.Where.ForEach(condition => {
                            output.Append(condition.ToString(out IEnumerable<DbParameter> pta, (e.Joins != null || e.Applies != null || e.UseFieldPrefixing)));
                            e.Parameters = e.Parameters.Concat(pta);
                        });

                    if (e.Group != null)
                    {
                        output.Append(" GROUP BY ");
                        e.Group.ForEach(g => { output.Append(g.ToString(e.UseFieldPrefixing) + ", "); });
                        output.Remove(output.Length - 2, 2);
                    }

                    if (e.Having != null)
                    {
                        output.Append(" HAVING ");
                        e.Having.ForEach(h => {
                            output.Append(h.ToString(out IEnumerable<DbParameter> pta, e.UseFieldPrefixing) + " ");
                            e.Parameters = e.Parameters.Concat(pta);
                        });
                        output.Remove(output.Length - 2, 2);
                    }

                    if (e.Order != null)
                    {
                        output.Append(" ORDER BY ");
                        e.Order.ForEach(o => { output.Append(o.ToString((e.Joins != null || e.Applies != null || e.UseFieldPrefixing) ? o.Column.ReflectedType.GetMappedName() + "." : string.Empty) + ", "); });
                        output.Remove(output.Length - 2, 2);
                    }

                }

                if (e.Top > 0)
                    output.Append("LIMIT " + e.Top.ToString() + " ");

                if (e.TakeSkip != null)
                    output.Append(" " + e.TakeSkip.ToString());

                //if (e.ForPath != null)
                //    output.Append(" " + e.ForPath.ToString());

                if (e.IsSubQuery)
                    output.Append(") ");

            }

            if (!string.IsNullOrEmpty(e.Alias) && (e.IsSubQuery || e.Stuff != null))
                output.Append("AS `" + e.Alias + "` ");

            return output.ToString();

        }

    }

}
