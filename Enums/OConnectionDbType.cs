/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/


namespace K2host.Data.Enums
{
    /// <summary>
    /// This is used on the OConnection to help create the instance.
    /// </summary>
    public enum OConnectionDbType: int
    {
        SqlDbType       = 1,
        MySqlDbType     = 2,
        OracleDbType    = 4
    }
}
