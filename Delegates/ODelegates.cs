/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Collections.Generic;

using K2host.Data.Classes;
using K2host.Data.Interfaces;

namespace K2host.Data.Delegates
{

    public delegate void OTriggeredEventHandler(IDataTriggerRequest e);

    public delegate IEnumerable<Type> OnGetDbContext();

    public delegate void OnGetDbContextCustom(OConnection DbConnection);

}
