/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System;

using K2host.Data.Interfaces;

namespace K2host.Data.Attributes
{


    /// <summary>
    /// Used on properties as a attribute for defining links to a data source.
    /// </summary>
    public abstract class ODataPropertyAttribute : Attribute, IDataOrderAttribute, IDataReadProcessAttribute, IDataWriteProcessAttribute
    {
        /// <summary>
        /// The order to process the attributes as a list.
        /// </summary>
        public virtual int Order { get; private set; }

        /// <summary>
        /// The abstract constructor for the order of the listed attr
        /// </summary>
        /// <param name="order"></param>
        public ODataPropertyAttribute(int order)
        {
            Order = order;
        }

        /// <summary>
        /// An attributes processing method if implemented.
        /// </summary>
        public virtual T OnWriteValue<T>(T data)
        {
            //Not implemented here.
            return default;
        }

        /// <summary>
        /// An attributes processed method if implemented.
        /// </summary>
        public virtual T OnReadValue<T>(T data)
        {
            //Not implemented here.
            return default;
        }

    }

}
