/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Text;

using K2host.Core;
using K2host.Data.Enums;
using K2host.Data.Interfaces;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where cases on the ODataObject
    /// </summary>
    public class ODataSelectQuery : IDataQuery
    {

        /// <summary>
        /// Optional: Enables placing the object / table name in front of the field xxxx.[xxxxx].
        /// </summary>
        public bool UseFieldPrefixing { get; set; }

        /// <summary>
        /// Optional: Enables placing a default alias name table.[field] AS [table.field].
        /// </summary>
        public bool UseFieldDefaultAlias { get; set; }
        
        /// <summary>
        /// Optional: Enables the join as a filter and dosen't return the objects.
        /// </summary>
        public bool IncludeJoinFields { get; set; }
        
        /// <summary>
        /// Optional: Enables placing a default alias name table.[field] AS [table.field].
        /// </summary>
        public bool IncludeApplyFields { get; set; }
       
        /// <summary>
        /// Optional: Disables the deleted flag on any query to view old items
        /// </summary>
        public bool IncludeRemoved { get; set; }

        /// <summary>
        /// Optional: Make this select distinct rows.
        /// </summary>
        public bool IsSubQuery { get; set; }

        /// <summary>
        /// Optional: Make this select distinct rows.
        /// </summary>
        public bool Distinct { get; set; }

        /// <summary>
        /// Optional: Select the top X rows.
        /// </summary>
        public int Top { get; set; }

        /// <summary>
        /// This helps define a query in a query of a suff function.
        /// </summary>
        public ODataStuffFunction Stuff { get; set; }

        /// <summary>
        /// The prefix to the start of this segment condition.
        /// You can only have either a subqueries, cases or a column.
        /// </summary>
        public ODataFieldSet[] Fields { get; set; }

        /// <summary>
        /// The object you are selecting from mapped in the database (table name).
        /// </summary>
        public Type From { get; set; }

        /// <summary>
        /// Optional: Add Join sets the the select query
        /// </summary>
        public ODataJoinSet[] Joins { get; set; }
        
        /// <summary>
        /// Optional: Add applies to the query
        /// </summary>
        public ODataApplySet[] Applies { get; set; }

        /// <summary>
        /// Optional: The condition list based on the the when is question
        /// </summary>
        public ODataCondition[] Where { get; set; }

        /// <summary>
        /// Optional: The group by fields if required
        /// </summary>
        public ODataGroupSet[] Group { get; set; }

        /// <summary>
        /// Optional: The having by fields if required for this statment
        /// </summary>
        public ODataHavingSet[] Having { get; set; }

        /// <summary>
        /// Optional: If listed you can add an order
        /// </summary>
        public ODataOrder[] Order { get; set; }

        /// <summary>
        /// Optional: Add a take skip for paging
        /// </summary>
        public ODataTakeSkip TakeSkip { get; set; }
 
        /// <summary>
        /// Optional: Add a "for path"
        /// </summary>
        public ODataForPath ForPath { get; set; }

        /// <summary>
        /// Optional: The suffix alias is this a sub query.
        /// </summary>
        public string Alias { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataSelectQuery() 
        {

            From                    = null;
            Distinct                = false;
            Where                   = null;
            Order                   = null;
            Alias                   = string.Empty;
            UseFieldDefaultAlias    = true;
            UseFieldPrefixing       = true;
            IncludeJoinFields       = true;
            IncludeApplyFields      = true;
            IncludeRemoved          = false;

        }

        /// <summary>
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public override string ToString() 
        {

            StringBuilder output = new();

            if(IsSubQuery)
                output.Append('(');

            output.Append("SELECT ");

            if (Stuff != null) 
            {
                output.Append(Stuff.ToString() + " ");
            }
            else
            {

                if (Top > 0)
                    output.Append("TOP " + Top.ToString() + " ");

                if (Distinct)
                    output.Append("DISTINCT ");

                if (Fields != null)
                    Fields.ForEach(f => { output.Append(f.ToSelectString(UseFieldPrefixing, UseFieldDefaultAlias) + ", "); });

                if (Joins != null && IncludeJoinFields)
                    Joins.ForEach(j =>
                    {
                        if (j.Join != null && j.JoinQuery == null)
                            j.Join.GetFieldSets(ODataFieldSetType.SELECT).ForEach(f => { output.Append(f.ToSelectString(true, true) + ", "); });
                        if (j.Join == null && j.JoinQuery != null)
                            j.JoinQuery.Fields.ForEach(f => { output.Append(f.ToSelectString(j.JoinQuery.UseFieldPrefixing, j.JoinQuery.UseFieldDefaultAlias) + ", "); });
                    });

                if (Applies != null && IncludeApplyFields)
                    Applies.ForEach(a => { a.Query.Fields.ForEach(f => { output.Append(a.Alias + ".[" + a.Alias + "." + f.Column.Name + "] AS [" + a.Alias + "." + f.Column.Name + "], "); }); ; });

                //https://www.sqlshack.com/overview-of-the-sql-row-number-function/
                //ROW_NUMBER() OVER ( PARTITION / ORDER BY ) AS RowNumber       

                output.Remove(output.Length - 2, 2);

                if (From != null)
                {

                    output.Append(" FROM tbl_" + From.Name + " " + From.Name);

                    if (Joins != null)
                        Joins.ForEach(j =>
                        {
                            if (j.Join != null && j.JoinQuery == null)
                                output.Append(" " + j.JoinType.ToString().Replace("_", " ") + " JOIN tbl_" + j.Join.Name + " " + j.Join.Name + " ON ");

                            if (j.Join == null && j.JoinQuery != null)
                                output.Append(" " + j.JoinType.ToString().Replace("_", " ") + " JOIN ( " + j.JoinQuery.ToString() + " ) " + j.JoinQuery.From.Name + " ON ");

                            if (j.JoinConditions != null && j.JoinConditions.Length > 0)
                            {
                                j.JoinConditions.ForEach(condition =>
                                {
                                    output.Append(condition.ToString(true));
                                });
                            }
                            else
                            {

                                if (j.Join != null && j.JoinQuery == null)
                                    output.Append(j.Join.Name + ".[" + j.JoinOnField.Name + "] = " + j.JoinEqualsField.ReflectedType.Name + ".[" + j.JoinEqualsField.Name + "]");

                                if (j.Join == null && j.JoinQuery != null)
                                    output.Append(j.JoinQuery.From.Name + ".[" + j.JoinOnField.Name + "] = " + j.JoinEqualsField.ReflectedType.Name + ".[" + j.JoinEqualsField.Name + "]");
                            }

                        });

                    if (Applies != null)
                        Applies.ForEach(a => { output.Append(" " + a.ToString()); });

                    //https://www.w3schools.com/sql/sql_union.asp
                    //UNION     -- distince values
                    //UNION ALL -- allows dupe values

                    output.Append(" WHERE ");
                
                    if (!IncludeRemoved)
                        output.Append("(" + From.Name + ".[Flags] >= 0) AND (" + From.Name + ".[Flags] & " + ((long)ODataFlags.Deleted).ToString() + ") != " + ((long)ODataFlags.Deleted).ToString() + " ");

                    if (Where != null && !IncludeRemoved)
                        output.Append(" AND ");
            
                    if (Where != null)
                        Where.ForEach(condition => { output.Append(condition.ToString((Joins != null || Applies != null || UseFieldPrefixing))); });

                    if (Group != null)
                    {
                        output.Append(" GROUP BY ");
                        Group.ForEach(g => { output.Append(g.ToString(UseFieldPrefixing) + ", "); });
                        output.Remove(output.Length - 2, 2);
                    }

                    if (Having != null)
                    {
                        output.Append(" HAVING ");
                        Having.ForEach(h => { output.Append(h.ToString(UseFieldPrefixing) + " "); });
                        output.Remove(output.Length - 2, 2);
                    }

                    if (Order != null)
                    {
                        output.Append(" ORDER BY ");
                        Order.ForEach(o => { output.Append(o.ToString((Joins != null || Applies != null || UseFieldPrefixing) ? o.Column.ReflectedType.Name + "." : string.Empty) + ", "); });
                        output.Remove(output.Length - 2, 2);
                    }

                }

                if (TakeSkip != null)
                    output.Append(" " + TakeSkip.ToString());

                if (ForPath != null)
                    output.Append(" " + ForPath.ToString());

                if (IsSubQuery)
                    output.Append(") ");

            }

            if (!string.IsNullOrEmpty(Alias) && (IsSubQuery || Stuff != null))
                output.Append("AS [" + Alias + "] ");

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
