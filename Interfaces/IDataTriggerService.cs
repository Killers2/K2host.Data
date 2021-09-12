/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using K2host.Core.Delegates;
using K2host.Data.Classes;
using K2host.Data.Delegates;
using K2host.Sockets.Tcp;

namespace K2host.Data.Interfaces
{

    /// <summary>
    /// This interface allows you to create a class derived from the core components needed on storing information. 
    /// </summary>
    public interface IDataTriggerService : IDisposable
    {

        /// <summary>
        /// The TCP Server for listening to the requests.
        /// </summary>
        OServer TriggerServer { get; set; }

        /// <summary>
        /// The event handler when a trigger has occured.
        /// </summary>
        OTriggeredEventHandler OnTriggered { get; set; }

    }

}
