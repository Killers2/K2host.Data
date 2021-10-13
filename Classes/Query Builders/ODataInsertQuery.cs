/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Collections.Generic;
using System.Text;

using K2host.Core;

using gd = K2host.Data.OHelpers;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where cases on the ODataObject
    /// </summary>
    public class ODataInsertQuery : IDisposable
    {
     
        /// <summary>
        /// Optional: Enables Primary Key inserting.
        /// </summary>
        public bool UseIdentityInsert { get; set; }

        /// <summary>
        /// The object you are selecting from mapped in the database (table name).
        /// </summary>
        public Type To { get; set; }

        /// <summary>
        /// The prefix to the start of this segment condition.
        /// You can only have use values from either Fields, ValueSets, Select.
        /// </summary>
        public ODataFieldSet[] Fields { get; set; }

        /// <summary>
        /// The prefix to the start of this segment condition.
        /// You can only have use values from either Fields, ValueSets, Select.
        /// </summary>
        public List<ODataFieldSet[]> ValueSets { get; set; }

        /// <summary>
        /// The prefix to the start of this segment condition.
        /// You can only have use values from either Fields, ValueSets, Select.
        /// </summary>
        public ODataSelectQuery Select { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataInsertQuery() 
        {
            To                  = null;
            Fields              = null;
            ValueSets           = new List<ODataFieldSet[]>();
            Select              = null;
            UseIdentityInsert   = false;
        }

        /// <summary>
        /// This returns and builds the string representation of the query segment.
        /// </summary>
        /// <returns></returns>
        public override string ToString() 
        {

            StringBuilder output = new();

            var ToTableName = To.GetMappedName();

            if (UseIdentityInsert)
                output.Append("SET IDENTITY_INSERT tbl_" + ToTableName + " ON;");

            output.Append("INSERT INTO tbl_" + ToTableName + " ( ");

            Fields.ForEach(f => { output.Append(f.ToInsertString() + ", "); });

            output.Remove(output.Length - 2, 2);

            output.Append(')');

            if (ValueSets.Count <= 0 && Select == null) {
                output.Append(" VALUES (");
                Fields.ForEach(field => { output.Append(field.NewValue + ", "); });
                output.Remove(output.Length - 2, 2);
                output.Append(')');
            }

            if (ValueSets.Count > 0 && Select == null) {
                output.Append(" VALUES ");
                ValueSets.ForEach(values => {
                    output.Append('(');
                    values.ForEach(field => { output.Append(field.NewValue + ", "); });
                    output.Remove(output.Length - 2, 2);
                    output.Append("), ");
                });
                output.Remove(output.Length - 2, 2);
            }

            if (ValueSets.Count <= 0 && Select != null)
                output.Append(Select.ToString());


            if (UseIdentityInsert)
                output.Append("SET IDENTITY_INSERT tbl_" + ToTableName + " OFF;");

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
