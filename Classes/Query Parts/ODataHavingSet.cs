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

using K2host.Data.Enums;

using gd = K2host.Data.OHelpers;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where conditions on the ODataObject
    /// </summary>
    public class ODataHavingSet : IDisposable
    {

        /// <summary>
        /// The function around the column
        /// </summary>
        public ODataFunction Function { get; set; }

        /// <summary>
        /// The prefix to the start of this segment group.
        /// </summary>
        public PropertyInfo Column { get; set; }
       
        /// <summary>
        /// The operator of the query associated with the value(s)
        /// </summary>
        public ODataOperator Operator { get; set; }
        
        /// <summary>
        /// The value in question
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// This is used to join another part to the having query building.
        /// </summary>
        public ODataFollower FollowBy { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataHavingSet() 
        {
            Function    = ODataFunction.NONE;
            Column      = null;
            Operator    = ODataOperator.NONE;
            Value       = null;
            FollowBy    = ODataFollower.NONE;
        }

        /// <summary>
        /// This returns the field as a having by field statment.
        /// </summary>
        /// <param name="UseFieldPrefixing"></param>
        /// <returns></returns>
        public string ToString(bool UseFieldPrefixing = false)
        {

            StringBuilder output = new();

            if (Function != ODataFunction.NONE)
                output.Append(Function.ToString() + "(");

            if (UseFieldPrefixing)
                output.Append(Column.ReflectedType.GetMappedName() + ".");

            output.Append("[" + Column.Name + "]");

            if (Function != ODataFunction.NONE)
                output.Append(')');

            switch (Operator)
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

            if (Value.GetType().Name == "RuntimePropertyInfo")
                output.Append(gd.GetSqlRepresentation(SqlDbType.Structured, ((PropertyInfo)Value).ReflectedType.GetMappedName() + ".[" + ((PropertyInfo)Value).Name) + "]");
            else
                output.Append(gd.GetSqlRepresentation(Column, Value));

            if (Operator == ODataOperator.IN || Operator == ODataOperator.NOT_IN)
                output.Append(") ");

            switch (FollowBy)
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
