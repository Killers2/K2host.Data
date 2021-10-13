/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System;
using System.Text;
using System.Security.Cryptography.X509Certificates;

using gl = K2host.Core.OHelpers;

namespace K2host.Data.Attributes
{
    /// <summary>
    /// Used on properties as a attribute for encryption on a data value.
    /// </summary>
    public class ODataEncryptionAttribute : ODataPropertyAttribute
    {

        public static string            GlobalEncryptionKey { get; set; }
        public static X509Certificate2  GlobalCertificate { get; set; }

        public string                   EncryptionKey { get; private set; }
        public X509Certificate2         Certificate { get; private set; }

        /// <summary>
        /// Creates the attribute on a property to encrypt the string column data
        /// This will assume you have set the properties statically
        /// </summary>
        /// <param name="order"></param>
        public ODataEncryptionAttribute(int order = 0) 
            : base(order)
        {

            if (!string.IsNullOrEmpty(GlobalEncryptionKey))
                EncryptionKey = GlobalEncryptionKey;

            if(GlobalCertificate != null)
                Certificate = GlobalCertificate;

        }

        /// <summary>
        /// Creates the attribute on a property to encrypt the string column data
        /// </summary>
        /// <param name="key">Sql datatype name</param>
        /// <param name="order"></param>
        public ODataEncryptionAttribute(string key, int order = 0) 
            : base(order)
        {
            EncryptionKey = key;
        }

        /// <summary>
        /// Creates the attribute on a property to encrypt the string column data
        /// </summary>
        /// <param name="cert"></param>
        public ODataEncryptionAttribute(X509Certificate2 cert, int order = 0)
            : base(order)
        {
            Certificate = cert;
        }

        /// <summary>
        /// This will encrypt the string value of the input data.
        /// </summary>
        public override T OnWriteValue<T>(T data)
        {

            if (data.GetType() != typeof(string))
                return data;

            string result = string.Empty;

            try
            {
              
                if (!string.IsNullOrEmpty(EncryptionKey) && Certificate == null)
                    result = gl.EncryptAes(data.ToString(), EncryptionKey, Encoding.Unicode.GetBytes(EncryptionKey));
              
                if (string.IsNullOrEmpty(EncryptionKey) && Certificate != null)
                    result = gl.EncryptRSASha1(Certificate, data.ToString());
                
            }
            catch(Exception)
            {
                result = data.ToString();
            }

            return (T)(object)result;

        }
       
        /// <summary>
        /// This will decrypt the string value of the input data.
        /// </summary>
        public override T OnReadValue<T>(T data)
        {

            if (data.GetType() != typeof(string))
                return data;

            string result = string.Empty;
           
            try 
            { 

                if (!string.IsNullOrEmpty(EncryptionKey) && Certificate == null)
                    result = gl.DecryptAes(data.ToString(), EncryptionKey, Encoding.Unicode.GetBytes(EncryptionKey));

                if (string.IsNullOrEmpty(EncryptionKey) && Certificate != null)
                    result = gl.DecryptRSASha1(Certificate, data.ToString());

            }
            catch (Exception)
            {
                result = data.ToString();
            }

            return (T)(object)result;

        }

    }

}
