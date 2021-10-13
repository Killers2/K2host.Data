/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System;
using System.Net;
using System.Linq;
using System.Reflection;

using K2host.Data.Interfaces;
using K2host.Data.Delegates;

namespace K2host.Data.Attributes
{



    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="I"></typeparam>
    public class ODataConverterGeneric<I> : ODataPropertyConverter
    {
        
        /// <summary>
        /// 
        /// </summary>
        public OnConvertEvent OnConvertToTrigger { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public OnConvertEvent OnConvertFromTrigger { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public ODataConverterGeneric() 
        { 
        
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="property"></param>
        /// <returns></returns>
        public override bool CanConvert(PropertyInfo property)
        {
            return property.PropertyType == typeof(I) && property.GetCustomAttributes<ODataTypeAttribute>().Any() && !property.GetCustomAttributes<ODataPropertyAttribute>().Any();
        }
       
        /// <summary>
        /// 
        /// </summary>
        /// <param name="property"></param>
        /// <param name="model"></param>
        /// <returns></returns>
        public override object OnConvertTo(PropertyInfo property, object value, IDataObject model)
        {

            var att = property.GetCustomAttribute<ODataTypeAttribute>();

            return OnConvertToTrigger?.Invoke(value, att);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="property"></param>
        /// <param name="value"></param>
        /// <param name="model"></param>
        public override object OnConvertFrom(PropertyInfo property, object value, IDataObject model)
        {

            var att = property.GetCustomAttribute<ODataTypeAttribute>();

            return OnConvertFromTrigger?.Invoke(value, att);

        }

    }

}
