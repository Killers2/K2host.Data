/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;

using K2host.Data.Attributes;
using K2host.Data.Interfaces;

namespace K2host.Data.Delegates
{

    public delegate void OTriggeredEventHandler(IDataTriggerRequest e);

    public delegate IEnumerable<Type> OnGetDbContext();

    public delegate void OnGetDbContextCustom(IDataConnection DbConnection);

    public delegate object OnConvertEvent(object value, ODataTypeAttribute attribute);

    public delegate void OnConfiguringDbContext(DbContextOptionsBuilder e);

    public delegate void OnModelCreatingDbContext(ModelBuilder e);

}
