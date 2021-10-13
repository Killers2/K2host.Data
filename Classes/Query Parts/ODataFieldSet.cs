/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System;
using System.Data;
using System.Reflection;
using System.Text;

using K2host.Core;
using K2host.Data.Enums;

using gd = K2host.Data.OHelpers;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where conditions on the ODataObject
    /// </summary>
    public class ODataFieldSet : IDisposable
    {

        /// <summary>
        /// The prefix to the start of this segment condition.
        /// You can only have either a subqueries, cases or a column.
        /// </summary>
        public PropertyInfo Column { get; set; }
       
        /// <summary>
        /// The function around the column
        /// </summary>
        public ODataFunction Function { get; set; }
        
        /// <summary>
        /// The cast type around the column
        /// </summary>
        public ODataCast Cast { get; set; }

        /// <summary>
        /// The field / column as a case set of the table / type in question
        /// You can only have either a subqueries, cases or a column.
        /// </summary>
        public ODataCase[] Case { get; set; }
       
        /// <summary>
        /// The field / column as a sub query set of the table / type in question
        /// You can only have either a subqueries, cases or a column.
        /// </summary>
        public ODataSelectQuery SubQuery { get; set; }

        /// <summary>
        /// The suffix column alias.
        /// For xml use single quotes and string name or path. ('@FirstName' or 'table/MiddleName')
        /// </summary>
        public string Alias { get; set; }

        /// <summary>
        /// The value(s) in question
        /// </summary>
        public object NewValue { get; set; }

        /// <summary>
        /// The suffix value to plus on the end of the column
        /// for example : SELECT ',' + Column FROM
        /// </summary>
        public object Prefix { get; set; }

        /// <summary>
        /// The suffix value to plus on the end of the column
        /// for example : SELECT Column + ',' FROM
        /// </summary>
        public object Suffix { get; set; }

        /// <summary>
        /// The data type of the value(s)
        /// </summary>
        public SqlDbType DataType { get; set; }

        /// <summary>
        /// The data update operator
        /// </summary>
        public ODataUpdateOperator UpdateOperator { get; set; }
        
        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataFieldSet() 
        {

            Column          = null;
            Alias           = string.Empty;
            NewValue        = string.Empty;
            DataType        = SqlDbType.NVarChar;
            UpdateOperator  = ODataUpdateOperator.EQUAL;
            Function        = ODataFunction.NONE;
            Cast            = ODataCast.NONE;
            Prefix          = null;
            Suffix          = null;

        }

        /// <summary>
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public string ToUpdateString() 
        {
            StringBuilder output = new();

            output.Append("[" + Column.Name + "]");
           
            if (UpdateOperator == ODataUpdateOperator.EQUAL)
                output.Append(" = ");
           
            if (UpdateOperator == ODataUpdateOperator.PLUS_EQUAL)
                output.Append(" += ");

            output.Append(gd.GetSqlRepresentation(Column, NewValue));

            return output.ToString();
        }

        /// <summary>
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public string ToInsertString()
        {
            StringBuilder output = new();

            output.Append("[" + Column.Name + "]");

            return output.ToString();
        }

        /// <summary>
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public string ToSelectString(bool UseFieldPrefixing = false, bool UseFieldDefaultAlias = false)
        {
            
            StringBuilder output = new();

            if (Function != ODataFunction.NONE)
                output.Append(Function.ToString() + "(");

            if (Cast != ODataCast.NONE)
                output.Append("CAST(");

            if (Prefix != null)
                output.Append(gd.GetSqlRepresentation(gd.GetSqlDbType(Prefix.GetType()), Prefix) + " + ");

            if (Case != null && Column == null && SubQuery == null) 
            {

                output.Append("CASE ");

                Case.ForEach(c => {
                    output.Append(c.ToString(UseFieldPrefixing) + " ");
                });

                output.Remove(output.Length - 1, 1);

                output.Append(" END ");
             
                if (Cast != ODataCast.NONE)
                    output.Append(" AS " + Cast.ToString() + ") ");

                if (Suffix != null)
                    output.Append("+ " + gd.GetSqlRepresentation(gd.GetSqlDbType(Suffix.GetType()), Suffix) + " ");

                if (Function != ODataFunction.NONE)
                    output.Append(") ");
            
                if (!string.IsNullOrEmpty(Alias))
                    output.Append("AS [" + Alias + "]");

            }

            if (Case == null && Column != null && SubQuery == null)
            {

                if (UseFieldPrefixing)
                    output.Append(Column.ReflectedType.GetMappedName() + ".");

                output.Append("[" + Column.Name + "]");
               
                if (Cast != ODataCast.NONE)
                    output.Append(" AS " + Cast.ToString() + ") ");

                if (Suffix != null)
                    output.Append("+ " + gd.GetSqlRepresentation(gd.GetSqlDbType(Suffix.GetType()), Suffix) + " ");

                if (Function != ODataFunction.NONE)
                    output.Append(") ");
               
                if (UseFieldDefaultAlias)
                    output.Append(" AS [" + Column.ReflectedType.GetMappedName() + "." + Column.Name + "]");

                if (!UseFieldDefaultAlias && !string.IsNullOrEmpty(Alias))
                    output.Append(" AS [" + Alias + "]");

            }

            if (Case == null && Column == null && SubQuery != null)
            {

                output.Append('(');
                output.Append(SubQuery.ToString());
                output.Append(") ");

                if (Cast != ODataCast.NONE)
                    output.Append(" AS " + Cast.ToString() + ") ");

                if (Suffix != null)
                    output.Append("+ " + gd.GetSqlRepresentation(gd.GetSqlDbType(Suffix.GetType()), Suffix) + " ");
                
                if (Function != ODataFunction.NONE)
                    output.Append(") ");

                if (!string.IsNullOrEmpty(Alias))
                    output.Append(" AS [" + Alias + "]");
            }

            if (Case == null && Column == null && SubQuery == null && NewValue != null)
            {

                output.Append(gd.GetSqlRepresentation(DataType, NewValue));
              
                if (Cast != ODataCast.NONE)
                    output.Append(" AS " + Cast.ToString() + ") ");

                if (Suffix != null)
                    output.Append("+ " + gd.GetSqlRepresentation(gd.GetSqlDbType(Suffix.GetType()), Suffix) + " ");

                if (Function != ODataFunction.NONE)
                    output.Append(") ");

                if (!string.IsNullOrEmpty(Alias))
                    output.Append(" AS [" + Alias + "]");

            }

            return output.ToString();

        }


        #region Deconstuctor

        private bool IsDisposed = false;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!IsDisposed)
                if (disposing)
                {


                }
            IsDisposed = true;
        }

        #endregion

    }

}
