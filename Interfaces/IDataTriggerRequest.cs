/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Data;

using K2host.Data.Enums;

namespace K2host.Data.Interfaces
{

    /// <summary>
    /// This interface allows you to create a class derived from the core components needed on storing information. 
    /// </summary>
    public interface IDataTriggerRequest : IDisposable
    {

        /// <summary>
        /// The id of the trigger on the trigger event on the sql server.
        /// </summary>
        long TriggerId { get; set; }

        /// <summary>
        /// The action of this trigger
        /// </summary>
        OTriggerRequestAction Action { get; set; }

        /// <summary>
        /// The coloumn indexes of the changes made on the record effected.
        /// </summary>
        int[] Updated { get; set; }

        /// <summary>
        /// The databasename of which this was triggered.
        /// </summary>
        string DatabaseName { get; set; }

        /// <summary>
        /// The table target of where this trigger occured.
        /// </summary>
        string Tablename { get; set; }

        /// <summary>
        /// The primary key column name of the target table.
        /// </summary>
        string PkColumnName { get; set; }

        /// <summary>
        /// The record id of the data that was manipulated.
        /// </summary>
        long RecordId { get; set; }

        /// <summary>
        /// The record itself contained in the dataset that was changed.
        /// </summary>
        DataSet Data { get; set; }
       
        /// <summary>
        /// This is the service name of the plugin to which requests will go.
        /// </summary>
        string ServiceName { get; set; }

        /// <summary>
        /// This is the instance name of the plugin in the stream.
        /// </summary>
        string AddressName { get; set; }

        /// <summary>
        /// This is the key to which the server will allow data trasactions.
        /// </summary>
        string AuthenticationKey { get; set; }

    }

}
