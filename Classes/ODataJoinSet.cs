/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Reflection;

using K2host.Data.Enums;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create joins on the ODataObject
    /// </summary>
    public class ODataJoinSet : IDisposable
    {
       
        /// <summary>
        /// The object of the type you want to join to
        /// </summary>
        public Type Join { get; set; }

        /// <summary>
        /// The select statment of the join you want to use instead of the 'Type' join 
        /// </summary>
        public ODataSelectQuery JoinQuery { get; set; }

        /// <summary>
        /// The type of join this table.
        /// </summary>
        public ODataJoinType JoinType { get; set; }

        /// <summary>
        /// A list of join conditions "join on (XX =? XX) and? (XX =? XX)".
        /// </summary>
        public ODataCondition[] JoinConditions { get; set; }

        /// <summary>
        /// The field to join the match join.
        /// </summary>
        public PropertyInfo JoinOnField { get; set; }

        /// <summary>
        /// The field to match the join.
        /// </summary>
        public PropertyInfo JoinEqualsField { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataJoinSet() 
        {
            Join            = null;
            JoinQuery       = null;
            JoinType        = ODataJoinType.INNER;
            JoinEqualsField = null;
            JoinOnField     = null;
            JoinConditions  = null;
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
