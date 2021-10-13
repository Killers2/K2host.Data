/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;

using K2host.Data.Interfaces;

namespace K2host.Data.Classes
{
    public static class ODataContext
    {

        public static List<IDataPropertyConverter> PropertyConverters { get; } = new();

    }
}
