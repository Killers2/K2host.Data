/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

namespace K2host.Data.Attributes
{

    /// <summary>
    /// Used to remap the db context to an alternative model name
    /// </summary>
    public class ODataReMapAttribute : ODataAttribute
    {

        readonly string Model;

        public string ModelName { get { return Model; } }

        /// <summary>
        /// Construct a new attribute.
        /// </summary>
        /// <param name="e">The alternative model name in the db context</param>
        public ODataReMapAttribute(string e)
        {
            Model = e;
        }

    }

}
