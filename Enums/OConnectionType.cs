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
    public enum OConnectionType
    {
        SQLStandardSecurity,
        SQLTrustedConnection,
        SQLTrustedConnectionCE,
        OracleStandardSecurity,
        OracleCredentialsSecurity,
        MySqlStandardSecurity,

    }
}
