/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Globalization;
using System.Reflection;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class help the user create properties as place holders for the query builders
    /// </summary>
    public class ODataPropertyInfo : PropertyInfo, IDisposable
    {

        #region Non Implemented Features

        public override PropertyAttributes Attributes => throw new NotImplementedException();

        public override bool CanRead => throw new NotImplementedException();

        public override bool CanWrite => throw new NotImplementedException();

        public override MethodInfo[] GetAccessors(bool nonPublic)
        {
            throw new NotImplementedException();
        }

        public override MethodInfo GetGetMethod(bool nonPublic)
        {
            throw new NotImplementedException();
        }

        public override ParameterInfo[] GetIndexParameters()
        {
            throw new NotImplementedException();
        }

        public override MethodInfo GetSetMethod(bool nonPublic)
        {
            throw new NotImplementedException();
        }

        public override object GetValue(object obj, BindingFlags invokeAttr, Binder binder, object[] index, CultureInfo culture)
        {
            throw new NotImplementedException();
        }

        public override void SetValue(object obj, object value, BindingFlags invokeAttr, Binder binder, object[] index, CultureInfo culture)
        {
            throw new NotImplementedException();
        }

        public override object[] GetCustomAttributes(bool inherit)
        {
            throw new NotImplementedException();
        }

        public override object[] GetCustomAttributes(Type attributeType, bool inherit)
        {
            throw new NotImplementedException();
        }

        public override bool IsDefined(Type attributeType, bool inherit)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Implemented Properties

        public override Type PropertyType { get; }

        public override Type DeclaringType { get; }

        public override string Name { get; }

        public override Type ReflectedType { get; }

        #endregion

        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataPropertyInfo(string name, Type propertyType, Type declaringType, Type reflectedType) 
            : base ()
        {
            Name            = name;
            PropertyType    = propertyType;
            ReflectedType   = declaringType;
            DeclaringType   = reflectedType;
        }
       
        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataPropertyInfo(string name, Type propertyType) 
            : base()
        {
            Name = name;
            PropertyType = propertyType;
            ReflectedType = null;
            DeclaringType = null;
        }
      
        /// <summary>
        /// This creates the instance of the class.
        /// </summary>
        public ODataPropertyInfo(string name) 
            : base()
        {
            Name = name;
            PropertyType = typeof(String);
            ReflectedType = null;
            DeclaringType = null;
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
