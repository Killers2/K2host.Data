/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

namespace K2host.Data.Interfaces
{

    public interface IDataWriteProcessAttribute
    {

        /// <summary>
        /// An attributes process method if implemented.
        /// </summary>
        T OnWriteValue<T>(T data);

    }

}
