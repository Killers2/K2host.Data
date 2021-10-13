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
    public class ODataTakeSkip : IDisposable
    {

        /// <summary>
        /// The amount of records to take from a list for pagination
        /// </summary>
        public int Take { get; set; }

        /// <summary>
        /// The amount of records to skip from a list for pagination
        /// </summary>
        public int Skip { get; set; }

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataTakeSkip() 
        {

            Take = 0;
            Skip = 0;
        }

        /// <summary>
        /// This returns and builds the string representation of the take and skip segment.
        /// </summary>
        /// <returns></returns>
        public override string ToString() 
        {

            StringBuilder output = new();

            output.Append("OFFSET " + Skip.ToString() + " ROWS ");
            output.Append("FETCH NEXT " + Take.ToString() + " ROWS ONLY");

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
