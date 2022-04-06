/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/


namespace MySql.Data.MySqlClient
{
    /// <summary>
    /// This is used extend the MySql.Data.MySqlClient.MySqlDbType
    /// </summary>
    public enum MySqlDbTypeExt : int
    {
        NONE    = 9999999,
        Boolean = 1001,
    }
}
