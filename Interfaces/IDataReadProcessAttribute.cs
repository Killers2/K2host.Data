/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

namespace K2host.Data.Interfaces
{

    public interface IDataReadProcessAttribute
    {

        /// <summary>
        /// An attributes process method if implemented.
        /// </summary>
        T OnReadValue<T>(T data);

    }

}
