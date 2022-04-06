/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System.Linq;
using System.Reflection;

using K2host.Data.Interfaces;
using K2host.Data.Delegates;

namespace K2host.Data.Attributes
{


    /// <summary>
    /// Used on properties as an converter for reading and writing columns.
    /// I is the property type in the model.
    /// </summary>
    /// <typeparam name="I"></typeparam>
    public class ODataConverterGeneric<I> : ODataPropertyConverter
    {
        
        /// <summary>
        /// The event to trigger when the property needs to convert to.
        /// </summary>
        public OnConvertEvent OnConvertToTrigger { get; set; }

        /// <summary>
        /// The event to trigger when the property needs to convert from.
        /// </summary>
        public OnConvertEvent OnConvertFromTrigger { get; set; }

        /// <summary>
        /// The constructor
        /// </summary>
        public ODataConverterGeneric() 
        { 
        
        }

        /// <summary>
        /// Checks to see if this converter can canvert by type of prop and does not have the <see cref="ODataPropertyAttribute"/>
        /// </summary>
        /// <param name="property"></param>
        /// <returns></returns>
        public override bool CanConvert(PropertyInfo property)
        {
            return property.PropertyType == typeof(I) && property.GetCustomAttributes<ODataTypeAttribute>().Any() && !property.GetCustomAttributes<ODataPropertyAttribute>().Any();
        }

        /// <summary>
        /// Invokes the ConvertTo <see cref="OnConvertEvent"/> for the user to convert outside of this method
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
        /// Invokes the ConvertFrom <see cref="OnConvertEvent"/> for the user to convert outside of this method
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
