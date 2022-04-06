/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System;
using System.Data;
using K2host.Data.Classes;
using K2host.Data.Enums;
using K2host.Data.Extentions.ODataConnection;
using K2host.Data.Interfaces;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;

namespace K2host.Data.Attributes
{
    /// <summary>
    /// Used on properties as a attribute for defining links to a data source.
    /// </summary>
    public class ODataTypeAttribute : ODataAttribute
    {

        readonly int        DbSqlType       = -1;
        readonly int        DbMySqlType     = -1;
        readonly int        DbOracleType    = -1;
        readonly int        TypeSize;
        readonly int        TypePlaces;
        readonly string     TypeFormat;

        public int          SQLDataType         { get { return DbSqlType; } }
        public int          MySQLDataType       { get { return DbMySqlType; } }
        public int          OracleSQLDataType   { get { return DbOracleType; } }

        public int          DataTypeSize        { get { return TypeSize; } }
        public int          DataTypePlaces      { get { return TypePlaces; } }
        public string       DataTypeFormat      { get { return TypeFormat; } }

        public OConnectionDbType DataDbType { get; private set; }

        /// <summary>
        /// Creates the attribute on a property to define an data source link
        /// </summary>
        /// <param name="typeName">Sqls data type.</param>
        /// <param name="size">Using size as -1 will invoke the max in the field, this is also used as a format provider for some types like TIME(0)</param>
        /// <param name="places">Decimal places in a number field</param>
        public ODataTypeAttribute(SqlDbType typeName, int size = 0, int places = 0)
        {
            DbSqlType       = (int)typeName;
            TypeSize        = size;
            TypePlaces      = places;
            DataDbType      = OConnectionDbType.SqlDbType;
        }

        /// <summary>
        /// Creates the attribute on a property to define an data source link
        /// </summary>
        /// <param name="typeName">Sqls data type.</param>
        /// <param name="size">Using size as -1 will invoke the max in the field, this is also used as a format provider for some types like TIME(0)</param>
        /// <param name="places">Decimal places in a number field</param>
        public ODataTypeAttribute(MySqlDbType typeName, int size = 0, int places = 0, string typeFormat = "")
        {
            DbMySqlType     = (int)typeName;
            TypeSize        = size;
            TypePlaces      = places;
            DataDbType      = OConnectionDbType.MySqlDbType;
            TypeFormat      = typeFormat;
        }
        
        /// <summary>
        /// Creates the attribute on a property to define an data source link
        /// </summary>
        /// <param name="typeName">Sqls data type.</param>
        /// <param name="size">Using size as -1 will invoke the max in the field, this is also used as a format provider for some types like TIME(0)</param>
        /// <param name="places">Decimal places in a number field</param>
        public ODataTypeAttribute(MySqlDbTypeExt typeName, int size = 0, int places = 0, string typeFormat = "")
        {
            DbMySqlType     = (int)typeName;
            TypeSize        = size;
            TypePlaces      = places;
            DataDbType      = OConnectionDbType.MySqlDbType;
            TypeFormat      = typeFormat;
        }

        /// <summary>
        /// Creates the attribute on a property to define an data source link
        /// </summary>
        /// <param name="typeName">Sqls data type.</param>
        /// <param name="size">Using size as -1 will invoke the max in the field, this is also used as a format provider for some types like TIME(0)</param>
        /// <param name="places">Decimal places in a number field</param>
        public ODataTypeAttribute(OracleDbType typeName, int size = 0, int places = 0)
        {
            DbOracleType    = (int)typeName;
            TypeSize        = size;
            TypePlaces      = places;
            DataDbType      = OConnectionDbType.OracleDbType;
        }

        /// <summary>
        /// Creates the attribute on a property to define an data source link
        /// </summary>
        /// <param name="typeName">Sqls data type.</param>
        /// <param name="size">Using size as -1 will invoke the max in the field, this is also used as a format provider for some types like TIME(0)</param>
        /// <param name="places">Decimal places in a number field</param>
        public ODataTypeAttribute(SqlDbType typeNameSql, MySqlDbType typeNameMySql, OracleDbType typeNameOracle, int size = 0, int places = 0)
        {
            DbSqlType       = (int)typeNameSql;
            DbMySqlType     = (int)typeNameMySql;
            DbOracleType    = (int)typeNameOracle;
            TypeSize        = size;
            TypePlaces      = places;
            DataDbType      = OConnectionDbType.SqlDbType | OConnectionDbType.MySqlDbType | OConnectionDbType.OracleDbType;
        }

        /// <summary>
        /// Creates the attribute on a property to define an data source link
        /// </summary>
        /// <param name="typeName">Sqls data type.</param>
        /// <param name="size">Using size as -1 will invoke the max in the field, this is also used as a format provider for some types like TIME(0)</param>
        /// <param name="places">Decimal places in a number field</param>
        public ODataTypeAttribute(SqlDbType typeNameSql, MySqlDbTypeExt typeNameMySql, OracleDbType typeNameOracle, int size = 0, int places = 0)
        {
            DbSqlType       = (int)typeNameSql;
            DbMySqlType     = (int)typeNameMySql;
            DbOracleType    = (int)typeNameOracle;
            TypeSize        = size;
            TypePlaces      = places;
            DataDbType      = OConnectionDbType.SqlDbType | OConnectionDbType.MySqlDbType | OConnectionDbType.OracleDbType;
        }

        /// <summary>
        /// Returns the string rep of the type for inline sql
        /// </summary>
        /// <returns></returns>
        public override string ToString() 
        {
            return ODataContext
                .Connection()
                .GetFactory()
                .GetTypeAttributeRepresentation(this);
            
        }

    }

}
