using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

using K2host.Core;
using K2host.Data.Classes;
using K2host.Data.Attributes;
using K2host.Data.Enums;
using K2host.Data.Interfaces;
using K2host.Data.Extentions.ODataConnection;

using gl = K2host.Core.OHelpers;
using gd = K2host.Data.OHelpers;

namespace K2host.Data.Factories
{
    
    public class OMsSqlFactory : ISqlFactory
    {

        public OMsSqlFactory() { }

        public string SetMigrationVersion(string version)
        {
            return "UPDATE [MigrationVersion] SET [Version] = '" + version + "';";
        }

        public string GetMigrationVersion()
        {
            return "SELECT [Version] FROM [MigrationVersion];";
        }

        public string TruncateDatabaseTable(Type obj)
        {
            return "TRUNCATE TABLE " + obj.GetTypeInfo().GetMappedName();
        }

        public string DropDatabaseStoredProc(string typeName)
        {
            return "DROP PROCEDURE IF EXISTS spr_" + typeName;
        }

        public string DropDatabaseTable(string typeName)
        {
            return "DROP TABLE IF EXISTS " + typeName;
        }

        public string DropDatabaseStoredProc(Type obj)
        {
            return "DROP PROCEDURE IF EXISTS spr_" + obj.GetTypeInfo().GetMappedName();
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

            output.Append("CREATE PROCEDURE [dbo].[spr_" + tnm + "]" + Environment.NewLine);
            output.Append(" @Uid BIGINT = 0" + Environment.NewLine);
            output.Append(",@ParentId BIGINT = 0" + Environment.NewLine);
            output.Append(",@ParentType NVARCHAR(255) = ''" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .Each(p => {
                    output.Append(",@" + p.Name + " " + ((ODataTypeAttribute)p.GetCustomAttributes(true)[0]).ToString() + Environment.NewLine);
                    return true;
                });

            output.Append(",@Flags BIGINT = 0" + Environment.NewLine);
            output.Append(",@Updated DATETIME = getdate" + Environment.NewLine);
            output.Append(",@Datestamp DATETIME = getdate" + Environment.NewLine);
            output.Append("AS" + Environment.NewLine);
            output.Append("BEGIN" + Environment.NewLine);
            output.Append("    SET NOCOUNT ON;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("    IF (@Uid > 0) BEGIN" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        IF (@Flags = -2) BEGIN -- delete" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("            DELETE FROM " + tnm + " WHERE [Uid] = @Uid;" + Environment.NewLine);
            output.Append("            SELECT @Uid;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        END ELSE IF (@Flags = -1) BEGIN -- select" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("            SELECT " + Environment.NewLine);
            output.Append("            [Uid]" + Environment.NewLine);
            output.Append("            ,[ParentId]" + Environment.NewLine);
            output.Append("            ,[ParentType]" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .Each(p => {
                    output.Append("            ,[" + p.Name + "]" + Environment.NewLine);
                    return true;
                });

            output.Append("            ,[Flags]" + Environment.NewLine);
            output.Append("            ,[Updated]" + Environment.NewLine);
            output.Append("            ,[Datestamp]" + Environment.NewLine);
            output.Append("            FROM " + tnm + " WHERE [Uid] = @Uid;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        END ELSE BEGIN -- update" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("            UPDATE " + tnm + " SET " + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => (p.GetCustomAttribute<ODataTypeAttribute>(true) != null && !ignore.Contains(p.Name)) && (p.GetCustomAttribute<ODataExceptionAttribute>(true) == null || p.GetCustomAttribute<ODataExceptionAttribute>(true) != null && !p.GetCustomAttribute<ODataExceptionAttribute>(true).ODataExceptionType.HasFlag(ODataExceptionType.NON_UPDATE)))
                .ToArray()
                .ForEach(p => {
                    output.Append("            [" + p.Name + "] = @" + p.Name + "," + Environment.NewLine);
                });

            output.Append("            [ParentId] = @ParentId," + Environment.NewLine);
            output.Append("            [ParentType] = @ParentType," + Environment.NewLine);
            output.Append("            [Flags] = @Flags," + Environment.NewLine);
            output.Append("            [Updated] = @Updated, " + Environment.NewLine);
            output.Append("            [Datestamp] = @Datestamp " + Environment.NewLine);
            output.Append("            WHERE [Uid] = @Uid;" + Environment.NewLine);
            output.Append("            SELECT @Uid;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        END" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("    END ELSE BEGIN -- insert" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        INSERT INTO " + tnm + " (" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => (p.GetCustomAttribute<ODataTypeAttribute>(true) != null && !ignore.Contains(p.Name)) && (p.GetCustomAttribute<ODataExceptionAttribute>(true) == null || p.GetCustomAttribute<ODataExceptionAttribute>(true) != null && !p.GetCustomAttribute<ODataExceptionAttribute>(true).ODataExceptionType.HasFlag(ODataExceptionType.NON_INSERT)))
                .ToArray()
                .ForEach(p => {
                    output.Append("            [" + p.Name + "]," + Environment.NewLine);
                });

            output.Append("            [ParentId]," + Environment.NewLine);
            output.Append("            [ParentType]," + Environment.NewLine);
            output.Append("            [Flags]," + Environment.NewLine);
            output.Append("            [Updated]," + Environment.NewLine);
            output.Append("            [Datestamp]" + Environment.NewLine);
            output.Append("        ) VALUES (" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => (p.GetCustomAttribute<ODataTypeAttribute>(true) != null && !ignore.Contains(p.Name)) && (p.GetCustomAttribute<ODataExceptionAttribute>(true) == null || p.GetCustomAttribute<ODataExceptionAttribute>(true) != null && !p.GetCustomAttribute<ODataExceptionAttribute>(true).ODataExceptionType.HasFlag(ODataExceptionType.NON_INSERT)))
                .ToArray()
                .ForEach(p => {
                    output.Append("            @" + p.Name + "," + Environment.NewLine);
                });

            output.Append("            @ParentId," + Environment.NewLine);
            output.Append("            @ParentType," + Environment.NewLine);
            output.Append("            @Flags," + Environment.NewLine);
            output.Append("            @Updated," + Environment.NewLine);
            output.Append("            @Datestamp" + Environment.NewLine);
            output.Append("        )" + Environment.NewLine);
            output.Append("        SELECT [Uid] FROM " + tnm + " WHERE [Uid] = SCOPE_IDENTITY();" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("    END" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("END" + Environment.NewLine);

            return output.ToString();
        }

        public string CreateDatabaseTable(Type obj, IDataConnection Connection)
        {

            List<string> ignore = new() { "Uid", "ParentId", "ParentType", "Flags", "Updated", "Datestamp" };

            StringBuilder output = new();

            var tnm = obj.GetTypeInfo().GetMappedName();

            output.Append("USE [" + Connection.Database + "]" + Environment.NewLine);
            output.Append(";SET ANSI_NULLS ON" + Environment.NewLine);
            output.Append(";SET QUOTED_IDENTIFIER ON" + Environment.NewLine);
            output.Append(";CREATE TABLE [dbo].[" + tnm + "](" + Environment.NewLine);
            output.Append("    [Uid] BIGINT IDENTITY(1,1) NOT NULL," + Environment.NewLine);
            output.Append("    [ParentId] BIGINT NULL," + Environment.NewLine);
            output.Append("    [ParentType] NVARCHAR(255) NULL," + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .ForEach(p => {
                    output.Append("    [" + p.Name + "] " + ((ODataTypeAttribute)p.GetCustomAttributes(true)[0]).ToString() + " NULL," + Environment.NewLine);
                });

            output.Append("    [Flags] BIGINT NULL," + Environment.NewLine);
            output.Append("    [Updated] DATETIME NULL," + Environment.NewLine);
            output.Append("    [Datestamp] DATETIME NULL," + Environment.NewLine);
            output.Append(" CONSTRAINT [PK_" + tnm + "] PRIMARY KEY CLUSTERED " + Environment.NewLine);
            output.Append("(" + Environment.NewLine);
            output.Append("	[Uid] ASC" + Environment.NewLine);
            output.Append(")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]" + Environment.NewLine);
            output.Append(") ON [PRIMARY] ");

            if (output.ToString().Contains("NVARCHAR(MAX)") || output.ToString().Contains("VARCHAR(MAX)") || output.ToString().Contains("NTEXT") || output.ToString().Contains("TEXT") || output.ToString().Contains("IMAGE") || output.ToString().Contains("VARBINARY(MAX)") || output.ToString().Contains("XML"))
                output.Append("TEXTIMAGE_ON [PRIMARY]");

            output.Append(Environment.NewLine);
            output.Append(";ALTER TABLE [dbo].[" + tnm + "] ADD  CONSTRAINT [DF_" + tnm + "_ParentId]  DEFAULT ((0)) FOR [ParentId]" + Environment.NewLine);
            output.Append(";ALTER TABLE [dbo].[" + tnm + "] ADD  CONSTRAINT [DF_" + tnm + "_ParentType]  DEFAULT ('') FOR [ParentType]" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .ForEach(p => {
                    output.Append(";ALTER TABLE [dbo].[" + tnm + "] ADD  CONSTRAINT [DF_" + obj.GetTypeInfo().Name + "_" + p.Name + "]  DEFAULT ((" + Connection.GetSqlDefaultValueRepresentation(Connection.GetDbDataType(p.PropertyType)) + ")) FOR [" + p.Name + "]" + Environment.NewLine);
                });

            output.Append(";ALTER TABLE [dbo].[" + tnm + "] ADD  CONSTRAINT [DF_" + tnm + "_Flags]  DEFAULT ((0)) FOR [Flags]" + Environment.NewLine);
            output.Append(";ALTER TABLE [dbo].[" + tnm + "] ADD  CONSTRAINT [DF_" + tnm + "_Updated]  DEFAULT (getdate()) FOR [Updated]" + Environment.NewLine);
            output.Append(";ALTER TABLE [dbo].[" + tnm + "] ADD  CONSTRAINT [DF_" + tnm + "_Datestamp]  DEFAULT (getdate()) FOR [Datestamp]" + Environment.NewLine);

            return output.ToString();

        }

        public string CreateMigrationVersionTable(string DatabaseName)
        {

            StringBuilder output = new();

            output.Append("USE [" + DatabaseName + "]" + Environment.NewLine);
            output.Append(";SET ANSI_NULLS ON" + Environment.NewLine);
            output.Append(";SET QUOTED_IDENTIFIER ON" + Environment.NewLine);
            output.Append(";CREATE TABLE [dbo].[MigrationVersion] ([Version] NVARCHAR(255) NULL)");
            output.Append(";INSERT INTO [dbo].[MigrationVersion] ([Version]) VALUES ('0');");

            return output.ToString();

        }

        public bool MergeMigrationBackupAndRemove(Type obj, IDataConnection Connection)
        {
            var tnm = obj.GetTypeInfo().GetMappedName();

            StringBuilder output = new();

            output.Append("IF (OBJECT_ID (N'" + tnm + "', N'U') IS NOT NULL)" + Environment.NewLine);
            output.Append("BEGIN" + Environment.NewLine);
            output.Append("    IF ((SELECT COUNT([Uid]) FROM [dbo].[" + tnm + "]) > 0)" + Environment.NewLine);
            output.Append("    BEGIN" + Environment.NewLine);
            output.Append("        DECLARE @a VARCHAR(255)" + Environment.NewLine);
            output.Append("        DECLARE cc CURSOR" + Environment.NewLine);
            output.Append("        FOR" + Environment.NewLine);
            output.Append("        SELECT [name] FROM sys.objects WHERE [type_desc] = 'DEFAULT_CONSTRAINT' AND OBJECT_NAME(parent_object_id) = '" + tnm + "'" + Environment.NewLine);
            output.Append("        OPEN cc" + Environment.NewLine);
            output.Append("        FETCH NEXT FROM cc INTO @a" + Environment.NewLine);
            output.Append("        WHILE @@FETCH_STATUS = 0" + Environment.NewLine);
            output.Append("            BEGIN" + Environment.NewLine);
            output.Append("                DECLARE @b VARCHAR(255) = 'sp_rename N''' + @a + ''', N''' + @a + '_OLD'', N''OBJECT'''" + Environment.NewLine);

            //Used to revert but not used here.
            //output.Append("                DECLARE @b VARCHAR(255) = 'sp_rename N''' + @a + ''', N''' + REPLACE(@a, '_OLD', '') + ''', N''OBJECT'''" + Environment.NewLine);

            output.Append("                EXEC(@b)" + Environment.NewLine);
            output.Append("                FETCH NEXT FROM cc INTO @a" + Environment.NewLine);
            output.Append("            END" + Environment.NewLine);
            output.Append("        CLOSE cc" + Environment.NewLine);
            output.Append("        DEALLOCATE cc" + Environment.NewLine);
            output.Append("        EXEC sp_rename N'PK_" + tnm + "', N'PK_" + tnm + "_OLD', N'OBJECT';" + Environment.NewLine);
            output.Append("        EXEC sp_rename '" + tnm + "', 'old_" + tnm + "'; " + Environment.NewLine);
            output.Append("    END" + Environment.NewLine);
            output.Append("    ELSE" + Environment.NewLine);
            output.Append("    BEGIN" + Environment.NewLine);
            output.Append("        DROP TABLE IF EXISTS " + tnm + Environment.NewLine);
            output.Append("    END" + Environment.NewLine);
            output.Append("    DROP PROCEDURE IF EXISTS spr_" + tnm + Environment.NewLine);
            output.Append("END" + Environment.NewLine);

            //Lets merge migration backup and / or remove
            return Connection.Query(
                CommandType.Text,
                output.ToString(),
                null
            );

        }

        public bool MergeMigrationRestoreAndRemove(Type obj, IDataConnection Connection)
        {

            var tnm = obj.GetTypeInfo().GetMappedName();

            List<string> ignore = new() { "Uid", "ParentId", "ParentType", "Flags", "Updated", "Datestamp" };

            StringBuilder output = new();

            output.Append("IF (OBJECT_ID (N'old_" + tnm + "', N'U') IS NOT NULL)" + Environment.NewLine);
            output.Append("BEGIN" + Environment.NewLine);
            output.Append("    SET IDENTITY_INSERT [" + tnm + "] ON;" + Environment.NewLine);

            output.Append("    DECLARE @query NVARCHAR(MAX) ='" + Environment.NewLine);

            output.Append("    SELECT" + Environment.NewLine);
            output.Append("    [Uid]," + Environment.NewLine);
            output.Append("    [ParentId]," + Environment.NewLine);
            output.Append("    [ParentType],' + " + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .Each(p => {
                    int dbt = Connection.GetDbDataType(p.PropertyType);
                    output.Append("    (CASE WHEN EXISTS (SELECT 1 FROM syscolumns WHERE name = '" + p.Name + "' AND id = OBJECT_ID('old_" + tnm + "')) THEN '[" + p.Name + "],' ELSE '" + Connection.GetSqlDefaultValueRepresentation(dbt, true) + " AS [" + p.Name + "],' END) + " + Environment.NewLine);
                    return true;
                });

            output.Append("   '[Flags]," + Environment.NewLine);
            output.Append("    [Updated]," + Environment.NewLine);
            output.Append("    [Datestamp]" + Environment.NewLine);
            output.Append("    FROM [old_" + tnm + "]'" + Environment.NewLine);

            output.Append("    INSERT INTO [" + tnm + "] (" + Environment.NewLine);
            output.Append("    [Uid]," + Environment.NewLine);
            output.Append("    [ParentId]," + Environment.NewLine);
            output.Append("    [ParentType]," + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .Each(p => {
                    output.Append("    [" + p.Name + "]," + Environment.NewLine);
                    return true;
                });

            output.Append("    [Flags]," + Environment.NewLine);
            output.Append("    [Updated]," + Environment.NewLine);
            output.Append("    [Datestamp]" + Environment.NewLine);

            output.Append("    ) EXEC sp_executesql @query;" + Environment.NewLine);

            output.Append("    SET IDENTITY_INSERT [" + tnm + "] OFF;" + Environment.NewLine);
            output.Append("END" + Environment.NewLine);
            output.Append("DROP TABLE IF EXISTS [old_" + tnm + "];" + Environment.NewLine);

            return Connection.Query(
                CommandType.Text,
                output.ToString(),
                null
            );
        }

        public string GetTypeAttributeRepresentation(ODataTypeAttribute e)
        { 
            return e.SQLDataType switch
            {
                (int)SqlDbType.BigInt =>            "BIGINT",
                (int)SqlDbType.Binary =>            "BINARY(" + (e.DataTypeSize < 0 ? "MAX" : e.DataTypeSize.ToString()) + ")",
                (int)SqlDbType.Bit =>               "BIT",
                (int)SqlDbType.Char =>              "CHAR(" + (e.DataTypeSize < 0 ? "MAX" : e.DataTypeSize.ToString()) + ")",
                (int)SqlDbType.DateTime =>          "DATETIME",
                (int)SqlDbType.Decimal =>           "DECIMAL(" + e.DataTypeSize.ToString() + ", " + e.DataTypePlaces.ToString() + ")",
                (int)SqlDbType.Float =>             "FLOAT",
                (int)SqlDbType.Image =>             "IMAGE",
                (int)SqlDbType.Int =>               "INT",
                (int)SqlDbType.Money =>             "MONEY",
                (int)SqlDbType.NChar =>             "NCHAR",
                (int)SqlDbType.NText =>             "NTEXT",
                (int)SqlDbType.NVarChar =>          "NVARCHAR(" + (e.DataTypeSize < 0 ? "MAX" : e.DataTypeSize.ToString()) + ")",
                (int)SqlDbType.Real =>              "REAL",
                (int)SqlDbType.UniqueIdentifier =>  "UNIQUEIDENTIFIER",
                (int)SqlDbType.SmallDateTime =>     "SMALLDATETIME",
                (int)SqlDbType.SmallInt =>          "SMALLINT",
                (int)SqlDbType.SmallMoney =>        "SMALLMONEY",
                (int)SqlDbType.Text =>              "TEXT",
                (int)SqlDbType.Timestamp =>         "TIMESTAMP",
                (int)SqlDbType.TinyInt =>           "TINYINT",
                (int)SqlDbType.VarBinary =>         "VARBINARY(" + (e.DataTypeSize < 0 ? "MAX" : e.DataTypeSize.ToString()) + ")",
                (int)SqlDbType.VarChar =>           "VARCHAR(" + (e.DataTypeSize < 0 ? "MAX" : e.DataTypeSize.ToString()) + ")",
                (int)SqlDbType.Variant =>           "VARIANT",
                (int)SqlDbType.Xml =>               "XML",
                (int)SqlDbType.Udt =>               "UDT",
                (int)SqlDbType.Structured =>        "STRUCTURED",
                (int)SqlDbType.Date =>              "DATE",
                (int)SqlDbType.Time =>              "TIME(" + e.DataTypeSize.ToString() + ")",
                (int)SqlDbType.DateTime2 =>         "DATETIME2",
                (int)SqlDbType.DateTimeOffset =>    "DATETIMEOFFSET",
                _ =>                                "NVARCHAR(MAX)",
            };     
        }

        public DbDataAdapter GetNewDataAdapter()
        {
            return new SqlDataAdapter();
        }

        public DbCommand GetNewCommand(string cmdText)
        {
            return new SqlCommand(cmdText);
        }

        public DbConnection GetNewConnection(IDataConnection Connection)
        {
            return new SqlConnection(Connection.ToString());
        }

        public DbParameter CreateParam(int datatype, ParameterDirection direction, object value, string paramname)
        {
            try
            {
                return gd.CreateParam((SqlDbType)datatype, direction, value, paramname);
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
                return gd.CreateParam((SqlDbType)datatype, value, paramname);
            }
            catch
            {
                return default;
            }
        }

        public string GetSqlDefaultValueRepresentation(int datatype, bool encapsulate = false)
        {

            return (datatype) switch
            {
                (int)SqlDbType.Char             => encapsulate ? "''''" : "''",
                (int)SqlDbType.VarChar          => encapsulate ? "''''" : "''",
                (int)SqlDbType.NVarChar         => encapsulate ? "''''" : "''",
                (int)SqlDbType.NChar            => encapsulate ? "''''" : "''",
                (int)SqlDbType.NText            => encapsulate ? "''''" : "''",
                (int)SqlDbType.Bit              => "0",
                (int)SqlDbType.TinyInt          => "0",
                (int)SqlDbType.SmallInt         => "0",
                (int)SqlDbType.Int              => "0",
                (int)SqlDbType.BigInt           => "0",
                (int)SqlDbType.SmallMoney       => "0.00",
                (int)SqlDbType.Real             => "0.00",
                (int)SqlDbType.Decimal          => "0.00",
                (int)SqlDbType.Float            => "0.00",
                (int)SqlDbType.Money            => "0.00",
                (int)SqlDbType.Date             => "getdate()",
                (int)SqlDbType.SmallDateTime    => "getdate()",
                (int)SqlDbType.DateTime         => "getdate()",
                (int)SqlDbType.DateTime2        => "getdate()",
                (int)SqlDbType.Time             => encapsulate ? "''00:00:00''" : "'00:00:00'",
                (int)SqlDbType.Binary           => "0x00",
                (int)SqlDbType.Image            => "0x00",
                (int)SqlDbType.VarBinary        => "0x00",
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
                        parameters = parameters.Append(Connection.CreateParam((int)SqlDbType.Int, value, "@Value" + uparam));
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

                parameters = parameters.Append(gd.CreateParam((SqlDbType)xxDbType, value, "@Value" + uparam));

            }

            return output.ToString();

        }

        public int GetDbDataType(IDataConnection Connection, Type t)
        {

            int _result;

            //Grab the embbed type if nullable.
            t = t.NullSafeType();

            if (t.BaseType.Name == "Enum")
                _result = (int)SqlDbType.Int;
            else
                _result = (t.Name.ToString()) switch
                {
                    "String"    => (int)SqlDbType.NVarChar,
                    "Int16"     => (int)SqlDbType.SmallInt,
                    "Int32"     => (int)SqlDbType.Int,
                    "Int64"     => (int)SqlDbType.BigInt,
                    "UInt16"    => (int)SqlDbType.SmallInt,
                    "UInt32"    => (int)SqlDbType.Int,
                    "UInt64"    => (int)SqlDbType.BigInt,
                    "Decimal"   => (int)SqlDbType.Decimal,
                    "Double"    => (int)SqlDbType.Float,
                    "Single"    => (int)SqlDbType.Float,
                    "DateTime"  => (int)SqlDbType.DateTime,
                    "Date"      => (int)SqlDbType.Date,
                    "Time"      => (int)SqlDbType.Time,
                    "TimeSpan"  => (int)SqlDbType.Time,
                    "Boolean"   => (int)SqlDbType.Bit,
                    "Byte[]"    => (int)SqlDbType.Binary,
                    _           => (int)SqlDbType.NVarChar,
                };

            return _result;

        }

        public bool DeleteDatabase(IDataConnection Connection)
        {

            StringBuilder   output  = new();
            IDataConnection clone   = Connection.Clone();

            clone.Database = "master";

            output.Append("ALTER DATABASE [" + Connection.Database + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;" + Environment.NewLine);
            output.Append("DROP DATABASE [" + Connection.Database + "];" + Environment.NewLine);

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

            input.Append("SELECT [TABLE_NAME] FROM [" + Connection.Database + "].INFORMATION_SCHEMA.TABLES WHERE [TABLE_TYPE] = 'BASE TABLE' AND [TABLE_NAME] <> 'MigrationVersion';");

            try
            {

                DataSet dt = clone.Get(CommandType.Text, input.ToString(), null);

                if (dt.Tables[0].Rows.Count <= 0)
                    return Array.Empty<string>();
                else
                {
                    output = new();
                    dt.Tables[0].Rows.ForEach(r => {
                        if (r[0].ToString() != "MigrationVersion")
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

            clone.Database = "master";

            output.Append("SELECT * FROM master.dbo.sysdatabases WHERE ('[' + name + ']' = '" + Connection.Database + "' OR name = '" + Connection.Database + "');");

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

            StringBuilder   output  = new();
            IDataConnection clone   = Connection.Clone();

            clone.Database = "master";

            output.Append("CREATE DATABASE [" + Connection.Database + "]" + Environment.NewLine);
            output.Append(" CONTAINMENT = NONE" + Environment.NewLine);
            output.Append(" ON  PRIMARY" + Environment.NewLine);
            output.Append("( NAME = N'" + Connection.Database + "', FILENAME = N'" + filePath + "\\" + Connection.Database + ".mdf' , SIZE = 8192KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )" + Environment.NewLine);
            output.Append(" LOG ON " + Environment.NewLine);
            output.Append("( NAME = N'" + Connection.Database + "_log', FILENAME = N'" + filePath + "\\" + Connection.Database + "_log.ldf' , SIZE = 8192KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )" + Environment.NewLine);
            output.Append(";" + Environment.NewLine);
            output.Append("IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))" + Environment.NewLine);
            output.Append("begin" + Environment.NewLine);
            output.Append("EXEC [" + Connection.Database + "].[dbo].[sp_fulltext_database] @action = 'enable'" + Environment.NewLine);
            output.Append("end;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + Connection.Database + "] SET ANSI_NULL_DEFAULT OFF;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + Connection.Database + "] SET ANSI_NULLS OFF;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + Connection.Database + "] SET ANSI_PADDING OFF;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + Connection.Database + "] SET ANSI_WARNINGS OFF;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + Connection.Database + "] SET TRUSTWORTHY OFF;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + Connection.Database + "] SET AUTO_SHRINK OFF;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + Connection.Database + "] SET AUTO_UPDATE_STATISTICS ON;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + Connection.Database + "] SET QUOTED_IDENTIFIER OFF;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + Connection.Database + "] SET RECOVERY FULL;" + Environment.NewLine);
            output.Append("ALTER DATABASE [" + Connection.Database + "] SET  MULTI_USER;" + Environment.NewLine);

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

            output.Append("OFFSET " + e.Skip.ToString() + " ROWS ");
            output.Append("FETCH NEXT " + e.Take.ToString() + " ROWS ONLY");

            return output.ToString();

        }

        public string StuffFunctionBuildString(ODataStuffFunction e)
        {

            StringBuilder output = new();

            output.Append(" STUFF((");
            output.Append(e.Query.ToString());
            output.Append("), " + e.StartPosistion.ToString() + ", " + e.NumberOfChars.ToString() + ", '" + e.ReplacementExpression + "')");

            return output.ToString();

        }

        public string OrderBuildString(ODataOrder e, string prefix = "")
        {

            StringBuilder output = new();

            if (e.Random)
                output.Append("NEWID()");
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

            output.Append("[" + e.Column.Name + "]");

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
                output.Append(ODataContext.Connection().GetSqlRepresentation(-1, ((PropertyInfo)e.Value).ReflectedType.GetMappedName() + ".[" + ((PropertyInfo)e.Value).Name + "]", out IEnumerable<DbParameter> pta));
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
                output.Append(e.Column.ReflectedType.GetMappedName() + ".");

            output.Append("[" + e.Column.Name + "]");

            return output.ToString();

        }

        public string ForPathBuildString(ODataForPath e)
        {

            StringBuilder output = new();

            output.Append("FOR " + e.PathType.ToString() + " ");

            if (e.PathType == ODataForPathType.JSON)
            {
                if (string.IsNullOrEmpty(e.Root) && string.IsNullOrEmpty(e.Path))
                    output.Append("AUTO ");
                else if (!string.IsNullOrEmpty(e.Root) && string.IsNullOrEmpty(e.Path))
                    output.Append("PATH, ROOT('" + e.Root + "') ");
                else if (string.IsNullOrEmpty(e.Root) && !string.IsNullOrEmpty(e.Path))
                    output.Append("PATH ");
            }

            if (e.PathType == ODataForPathType.XML)
            {
                output.Append("PATH('" + e.Path + "') ");
                if (!string.IsNullOrEmpty(e.Root))
                    output.Append(", ROOT('" + e.Root + "') ");
                if (e.EnableXmlXSI)
                    output.Append(", ELEMENTS XSINIL ");
            }

            return output.ToString();

        }

        public string ApplySetBuildString(ODataApplySet e)
        {

            StringBuilder output = new();

            output.Append(e.ApplyType.ToString() + " APPLY (");

            output.Append(e.Query.ToString());

            output.Append(") " + e.Alias + " ");

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
                output.Append(ODataContext.Connection().GetSqlRepresentation(-1, "[" + ((PropertyInfo)e.Then).Name + "]", out IEnumerable<DbParameter> pta));
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
                    output.Append(ODataContext.Connection().GetSqlRepresentation(-1, "[" + ((PropertyInfo)e.Else).Name + "]", out IEnumerable<DbParameter> pta));
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

                e.Case.ForEach(c => {
                    output.Append(c.ToString() + " ");
                });

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
                            output.Append(e.Column.ReflectedType.GetMappedName() + ".[" + e.Column.Name + "]");
                        else
                            output.Append("[" + e.Column.Name + "]");
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
                                output.Append(ODataContext.Connection().GetSqlRepresentation(-1, ((PropertyInfo)v).ReflectedType.GetMappedName() + ".[" + ((PropertyInfo)v).Name, out IEnumerable<DbParameter> pta) + "],");
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

            output.Append("[" + e.Column.Name + "]");

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

            output.Append("[" + e.Column.Name + "]");

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
                    output.Append("AS [" + e.Alias + "]");

            }

            if (e.Case == null && e.Column != null && e.SubQuery == null)
            {

                if (UseFieldPrefixing)
                    output.Append(e.Column.ReflectedType.GetMappedName() + ".");

                output.Append("[" + e.Column.Name + "]");

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
                    output.Append(" AS [" + e.Column.ReflectedType.GetMappedName() + "." + e.Column.Name + "]");

                if (!UseFieldDefaultAlias && !string.IsNullOrEmpty(e.Alias))
                    output.Append(" AS [" + e.Alias + "]");

            }

            if (e.Case == null && e.Column == null && e.SubQuery != null)
            {

                output.Append('(');
                output.Append(e.SubQuery.ToString());
                output.Append(") ");
                
                parameters = parameters.Concat(e.SubQuery.Parameters);
                e.SubQuery.Parameters = Array.Empty<DbParameter>();

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
                    output.Append(" AS [" + e.Alias + "]");
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
                    output.Append(" AS [" + e.Alias + "]");

            }

            return output.ToString();

        }

        public string InsertQueryBuildString(ODataInsertQuery e)
        {

            StringBuilder output = new();

            var ToTableName = e.To.GetMappedName();

            if (e.UseIdentityInsert)
                output.Append("SET IDENTITY_INSERT " + ToTableName + " ON;");

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


            if (e.UseIdentityInsert)
                output.Append("SET IDENTITY_INSERT " + ToTableName + " OFF;");

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

            if (e.Where != null)  {
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

            if(e.IsSubQuery)
                output.Append('(');

            output.Append("SELECT ");

            if (e.Stuff != null) 
            {
                output.Append(e.Stuff.ToString() + " ");
                e.Parameters = e.Parameters.Concat(e.Stuff.Query.Parameters);
            }
            else
            {

                if (e.Top > 0)
                    output.Append("TOP " + e.Top.ToString() + " ");

                if (e.Distinct)
                    output.Append("DISTINCT ");

                if (e.Fields != null)
                    e.Fields.ForEach(f => { 
                        output.Append(f.ToSelectString(out IEnumerable<DbParameter> pta, e.UseFieldPrefixing, e.UseFieldDefaultAlias) + ", ");
                        e.Parameters = e.Parameters.Concat(pta);
                    });

                if (e.Joins != null && e.IncludeJoinFields)
                    e.Joins.ForEach(j =>
                    {
                       
                        if (j.Join != null && j.JoinQuery == null)
                            j.Join.GetFieldSets(ODataFieldSetType.SELECT).ForEach(f => { 
                                output.Append(f.ToSelectString(out IEnumerable<DbParameter> pta, true, true) + ", ");
                                e.Parameters = e.Parameters.Concat(pta);
                            });
                        
                        if (j.Join == null && j.JoinQuery != null)
                            j.JoinQuery.Fields.ForEach(f => { 
                                output.Append(f.ToSelectString(out IEnumerable<DbParameter> pta, j.JoinQuery.UseFieldPrefixing, j.JoinQuery.UseFieldDefaultAlias) + ", ");
                                e.Parameters = e.Parameters.Concat(pta);
                            });
                    
                    });

                if (e.Applies != null && e.IncludeApplyFields)
                    e.Applies.ForEach(a => { a.Query.Fields.ForEach(f => { output.Append(a.Alias + ".[" + a.Alias + "." + f.Column.Name + "] AS [" + a.Alias + "." + f.Column.Name + "], "); });});

                //https://www.sqlshack.com/overview-of-the-sql-row-number-function/
                //ROW_NUMBER() OVER ( PARTITION / ORDER BY ) AS RowNumber       

                output.Remove(output.Length - 2, 2);

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

                    //https://www.w3schools.com/sql/sql_union.asp
                    //UNION     -- distince values
                    //UNION ALL -- allows dupe values

                    output.Append(" WHERE ");
                
                    if (!e.IncludeRemoved)
                        output.Append("(" + FromTableName + ".[Flags] >= 0) AND (" + FromTableName + ".[Flags] & " + ((long)ODataFlags.Deleted).ToString() + ") != " + ((long)ODataFlags.Deleted).ToString() + " ");

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

                if (e.TakeSkip != null)
                    output.Append(" " + e.TakeSkip.ToString());

                if (e.ForPath != null)
                    output.Append(" " + e.ForPath.ToString());

                if (e.IsSubQuery)
                    output.Append(") ");

            }

            if (!string.IsNullOrEmpty(e.Alias) && (e.IsSubQuery || e.Stuff != null))
                output.Append("AS [" + e.Alias + "] ");

            return output.ToString();

        }


    }

}
