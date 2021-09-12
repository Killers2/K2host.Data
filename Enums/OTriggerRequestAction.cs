/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using K2host.Data.Interfaces;

namespace K2host.Data.Enums
{
    /// <summary>
    /// The type of action between <see cref="IDataTriggerRequest"/>s interaction.
    /// </summary>

    public enum OTriggerRequestAction
    {
        INSERT = 1,
        UPDATE = 2,
        DELETE = 3
    }
}
