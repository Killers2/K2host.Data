/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System;
using System.Linq;
using System.Reflection;

using K2host.Data.Interfaces;

namespace K2host.Data.Attributes
{

    /// <summary>
    /// Used on properties as an converter for reading and writing columns.
    /// I is the property type in the model.
    /// </summary>
    public abstract class ODataPropertyConverter : IDataPropertyConverter
    {

        /// <summary>
        /// The abstract constructor
        /// </summary>
        public ODataPropertyConverter() 
        { 
        
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="property"></param>
        /// <returns></returns>
        public virtual bool CanConvert(PropertyInfo property)
        {
            return property.GetCustomAttributes<ODataTypeAttribute>().Any() && !property.GetCustomAttributes<ODataPropertyAttribute>().Any();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="property"></param>
        /// <param name="model"></param>
        /// <returns></returns>
        public virtual object OnConvertTo(PropertyInfo property, object value, IDataObject model)
        {
            //No Implimented Here.
            return default;
        }
       
        /// <summary>
        /// 
        /// </summary>
        /// <param name="property"></param>
        /// <param name="value"></param>
        /// <param name="model"></param>
        public virtual object OnConvertFrom(PropertyInfo property, object value, IDataObject model)
        {
            //No Implimented Here.
            return default;
        }

    }

}
