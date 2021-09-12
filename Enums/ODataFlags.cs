/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
namespace K2host.Data.Enums
{
    public enum ODataFlags
    {
        DBDelete = -2,
        DBSelect = -1,
        Active = 1,
        Deleted = 2,
        Hot = 4,
        Warm = 8,
        Cold = 16,
        New = 32,
        Old = 64,
        HasReferences = 128,
        Disabled = 256,
        GlobalByAction = 512,
        OnlyPersonalView = 1024,
        AdminView = 2048
    }
}
