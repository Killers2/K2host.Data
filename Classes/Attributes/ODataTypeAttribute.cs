/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System;
using System.Data;

using K2host.Data.Interfaces;

namespace K2host.Data.Attributes
{
    /// <summary>
    /// Used on properties as a attribute for defining links to a data source.
    /// </summary>
    public class ODataTypeAttribute : ODataAttribute
    {

        readonly SqlDbType      MsSqlType;
        readonly string         StrType;
        readonly int            TypeSize;
        readonly int            TypePlaces;
        readonly int            SelectedType = 0;

        public string           StrDataType         { get { return StrType; } }
        public SqlDbType        MsSQLDataType       { get { return MsSqlType; } }
        public int              DataTypeSize        { get { return TypeSize; } }
        public int              DataTypePlaces      { get { return TypePlaces; } }

        /// <summary>
        /// Creates the attribute on a property to define an data source link
        /// </summary>
        /// <param name="typeName">Sql datatype name</param>
        public ODataTypeAttribute(string typeName)
        {
            StrType     = typeName;
            TypeSize    = 0;
            TypePlaces  = 0;
        }

        /// <summary>
        /// Creates the attribute on a property to define an data source link
        /// </summary>
        /// <param name="typeName">Sqls data type.</param>
        /// <param name="size">Using size as -1 will invoke the max in the field, this is also used as a format provider for some types like TIME(0)</param>
        /// <param name="places">Decimal places in a number field</param>
        public ODataTypeAttribute(SqlDbType typeName, int size = 0, int places = 0)
        {
            MsSqlType       = typeName;
            TypeSize        = size;
            TypePlaces      = places;
            SelectedType    = 1;
        }

        /// <summary>
        /// Returns the string rep of the type for inline sql
        /// </summary>
        /// <returns></returns>
        public override string ToString() 
        {
            string output = string.Empty;

            if (SelectedType == 0)
            {
                output = StrDataType;
            }
            else if (SelectedType == 1)
            {
                output = MsSQLDataType switch
                {
                    SqlDbType.BigInt =>             "BIGINT",
                    SqlDbType.Binary =>             "BINARY(" + (DataTypeSize < 0 ? "MAX" : DataTypeSize.ToString()) + ")",
                    SqlDbType.Bit =>                "BIT",
                    SqlDbType.Char =>               "CHAR(" + (DataTypeSize < 0 ? "MAX" : DataTypeSize.ToString()) + ")",
                    SqlDbType.DateTime =>           "DATETIME",
                    SqlDbType.Decimal =>            "DECIMAL(" + DataTypeSize.ToString() + ", " + DataTypePlaces.ToString() + ")",
                    SqlDbType.Float =>              "FLOAT",
                    SqlDbType.Image =>              "IMAGE",
                    SqlDbType.Int =>                "INT",
                    SqlDbType.Money =>              "MONEY",
                    SqlDbType.NChar =>              "NCHAR",
                    SqlDbType.NText =>              "NTEXT",
                    SqlDbType.NVarChar =>           "NVARCHAR(" + (DataTypeSize < 0 ? "MAX" : DataTypeSize.ToString()) + ")",
                    SqlDbType.Real =>               "REAL",
                    SqlDbType.UniqueIdentifier =>   "UNIQUEIDENTIFIER",
                    SqlDbType.SmallDateTime =>      "SMALLDATETIME",
                    SqlDbType.SmallInt =>           "SMALLINT",
                    SqlDbType.SmallMoney =>         "SMALLMONEY",
                    SqlDbType.Text =>               "TEXT",
                    SqlDbType.Timestamp =>          "TIMESTAMP",
                    SqlDbType.TinyInt =>            "TINYINT",
                    SqlDbType.VarBinary =>          "VARBINARY(" + (DataTypeSize < 0 ? "MAX" : DataTypeSize.ToString()) + ")",
                    SqlDbType.VarChar =>            "VARCHAR(" + (DataTypeSize < 0 ? "MAX" : DataTypeSize.ToString()) + ")",
                    SqlDbType.Variant =>            "VARIANT",
                    SqlDbType.Xml =>                "XML",
                    SqlDbType.Udt =>                "UDT",
                    SqlDbType.Structured =>         "STRUCTURED",
                    SqlDbType.Date =>               "DATE",
                    SqlDbType.Time =>               "TIME(" + DataTypeSize.ToString() + ")",
                    SqlDbType.DateTime2 =>          "DATETIME2",
                    SqlDbType.DateTimeOffset =>     "DATETIMEOFFSET",
                    _ => "NVARCHAR(MAX)",
                };
            }
            
            return output;

        }

    }

}
