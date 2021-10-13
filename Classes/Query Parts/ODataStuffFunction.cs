/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Text;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where cases on the ODataObject
    /// </summary>
    public class ODataStuffFunction : IDisposable
    {

        /// <summary>
        /// The query wrapped in the apply type, which would have the linking within.
        /// </summary>
        public ODataSelectQuery Query { get; set; }

        /// <summary>
        /// The Stuff function start posistion
        /// </summary>
        public int StartPosistion { get; set; }

        /// <summary>
        /// The Stuff function No. of chars
        /// </summary>
        public int NumberOfChars { get; set; }

        /// <summary>
        /// The replacement expression
        /// </summary>
        public string ReplacementExpression { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataStuffFunction() 
        {
            Query                   = null;
            StartPosistion          = 0;
            NumberOfChars           = 0;
            ReplacementExpression   = string.Empty;
        }

        /// <summary>
        /// This returns and builds the string representation of the case segment.
        /// </summary>
        /// <returns></returns>
        public override string ToString() 
        {

            StringBuilder output = new();

            output.Append(" STUFF((");
            output.Append(Query.ToString());
            output.Append("), " + StartPosistion.ToString() + ", " + NumberOfChars.ToString() + ", '" + ReplacementExpression + "')");

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
