/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

namespace K2host.Data.Interfaces
{

    public interface IDataOrderAttribute
    {

        /// <summary>
        /// The order to process the attributes as a list.
        /// </summary>
        int Order { get; }

    }

}
