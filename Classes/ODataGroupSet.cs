/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System;
using System.Reflection;
using System.Text;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create where conditions on the ODataObject
    /// </summary>
    public class ODataGroupSet : IDisposable
    {

        /// <summary>
        /// The prefix to the start of this segment group.
        /// </summary>
        public PropertyInfo Column { get; set; }
        
        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataGroupSet() 
        {
            Column  = null;
        }

        /// <summary>
        /// This returns the field as a group by field statment.
        /// </summary>
        /// <param name="UseFieldPrefixing"></param>
        /// <returns></returns>
        public string ToString(bool UseFieldPrefixing = false)
        {

            StringBuilder output = new StringBuilder();

            if (UseFieldPrefixing)
                output.Append(Column.ReflectedType.Name + ".");

            output.Append("[" + Column.Name + "]");

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
