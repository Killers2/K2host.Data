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
    public class ODataCondition : IDisposable
    {

        /// <summary>
        /// The prefix to the start of this segment condition.
        /// </summary>
        public ODataFollower Prefix { get; set; }

        /// <summary>
        /// This helps contain the query with the bracket
        /// </summary>
        public ODataConditionContainer Container { get; set; }

        /// <summary>
        /// The field / column name of the table / type in question
        /// You can only have either a subqueries, cases or a column.
        /// </summary>
        public PropertyInfo Column { get; set; }
        
        /// <summary>
        /// The field / column name of the table / type in question
        /// You can only have either a subqueries, cases or a column.
        /// </summary>
        public string ColumnText { get; set; }

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
        /// The operator of the query associated with the value(s)
        /// </summary>
        public ODataOperator Operator { get; set; }

        /// <summary>
        /// If using the "like" operator you can select the type of "like" operator.
        /// </summary>
        public ODataLikeOperator LikeOperator { get; set; }

        /// <summary>
        /// The value(s) in question, this can also be of type property info for joins and cross applies
        /// </summary>
        public object[] Values { get; set; }

        /// <summary>
        /// This is used to join another part to the query building.
        /// </summary>
        public ODataFollower FollowBy { get; set; }

        /// <summary>
        /// The function around the field column
        /// </summary>
        public ODataFunction FieldFunction { get; set; }

        /// <summary>
        /// The function around the value / value column
        /// </summary>
        public ODataFunction ValueFunction { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataCondition() 
        {

            Prefix          = ODataFollower.NONE;
            Container       = ODataConditionContainer.NONE;
            Column          = null;
            ColumnText      = string.Empty;
            Operator        = ODataOperator.NONE;
            LikeOperator    = ODataLikeOperator.NONE;
            Values          = Array.Empty<object>();
            FollowBy        = ODataFollower.NONE;
            FieldFunction   = ODataFunction.NONE;
            ValueFunction   = ODataFunction.NONE;
        }

        /// <summary>
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public string ToString(bool UsePrefix = false) 
        {
            StringBuilder output = new();

            switch (Prefix)
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

            if (Container == ODataConditionContainer.OPEN)
                output.Append("( ");

            if (FieldFunction != ODataFunction.NONE)
                output.Append(FieldFunction.ToString() + "(");

            if (Case != null && SubQuery == null)
            {
                output.Append("(CASE ");

                Case.ForEach(c => { 
                    output.Append(c.ToString() + " "); 
                });

                output.Remove(output.Length - 1, 1);

                output.Append(" END) ");
            }
            else if (Case == null && SubQuery != null)
            {
                output.Append(SubQuery.ToString());
            }
            else
            {
                if (Column == null)
                    output.Append(ColumnText);
                else
                {
                    if (Column.GetType().Name == "ODataPropertyInfo")
                        output.Append(Column.Name);
                    else
                    {
                        if (UsePrefix)
                            output.Append(Column.ReflectedType.Name + ".[" + Column.Name + "]");
                        else
                            output.Append("[" + Column.Name + "]");
                    }
                }
            }

            if (FieldFunction != ODataFunction.NONE)
                output.Append(") ");

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

            if (ValueFunction != ODataFunction.NONE)
                output.Append(FieldFunction.ToString() + "(");

            foreach (object v in Values)
            {
                
                if (Operator == ODataOperator.LIKE || Operator == ODataOperator.NOT_LIKE)
                {
                    switch (LikeOperator)
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
                        output.Append(gd.GetSqlRepresentation(null, v) + ",");
                    else
                    {
                        if (Case != null || SubQuery != null)
                            output.Append(gd.GetSqlRepresentation(gd.GetSqlDbType(v.GetType()), v) + ",");
                        else
                        {
                            if (v.GetType().Name == "RuntimePropertyInfo")
                                output.Append(gd.GetSqlRepresentation(SqlDbType.Structured, ((PropertyInfo)v).ReflectedType.Name + ".[" + ((PropertyInfo)v).Name) + "],");
                            else if (v.GetType().Name == "ODataPropertyInfo")
                                output.Append(gd.GetSqlRepresentation(SqlDbType.Structured, ((ODataPropertyInfo)v).Name) + ",");
                            else
                                output.Append(gd.GetSqlRepresentation(Column, v) + ",");
                        }
                    }

                }
            }

            output.Remove(output.Length - 1, 1);

            if (Operator == ODataOperator.IN || Operator == ODataOperator.NOT_IN)
                output.Append(") ");

            if (ValueFunction != ODataFunction.NONE)
                output.Append(") ");

            if (Container == ODataConditionContainer.CLOSE)
                output.Append(" ) ");

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
