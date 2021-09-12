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
using K2host.Data.Interfaces;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where cases on the ODataObject
    /// </summary>
    public class ODataUpdateQuery : IDataQuery
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
        /// This creates the instance of the class.
        /// </summary>
        public ODataUpdateQuery() 
        {
            From                    = null;
            Where                   = null;
            UseFieldDefaultAlias    = false;
            UseFieldPrefixing       = false;
        }

        /// <summary>
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public override string ToString() 
        {

            StringBuilder output = new();

            output.Append("UPDATE tbl_" + From.Name + " SET ");

            if (Fields != null)
                Fields.ForEach(f => { output.Append(f.ToUpdateString() + ", "); });

            output.Remove(output.Length - 2, 2);

            if (Joins != null || Applies != null)
            {

                output.Append(" FROM tbl_" + From.Name + " ");

                if (Joins != null)
                    Joins.ForEach(j => { output.Append(" " + j.JoinType.ToString().Replace("_", " ") + " JOIN tbl_" + j.Join.Name + " " + j.Join.Name + " ON " + j.Join.Name + ".[" + j.JoinOnField.Name + "] = " + j.JoinEqualsField.ReflectedType.Name + ".[" + j.JoinEqualsField.Name + "]"); });

                if (Applies != null)
                    Applies.ForEach(a => { output.Append(" " + a.ToString()); });

            }

            if (Where != null)  {
                output.Append(" WHERE ");
                Where.ForEach(condition => {
                    output.Append(condition.ToString((Joins != null || Applies != null || UseFieldPrefixing))); 
                });
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
