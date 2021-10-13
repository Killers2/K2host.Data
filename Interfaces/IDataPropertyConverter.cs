/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System.Reflection;

namespace K2host.Data.Interfaces
{

    public interface IDataPropertyConverter
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="property"></param>
        /// <returns></returns>
        bool CanConvert(PropertyInfo property);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="property"></param>
        /// <param name="model"></param>
        object OnConvertTo(PropertyInfo property, object value, IDataObject model);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="property"></param>
        /// <param name="value"></param>
        /// <param name="model"></param>
        object OnConvertFrom(PropertyInfo property, object value, IDataObject model);

    }

}
