/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Data;

using K2host.Data.Interfaces;
using K2host.Data.Enums;

using gd = K2host.Data.OHelpers;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class helps create a database connections string. 
    /// </summary>
    public class ODataTriggerRequest : IDataTriggerRequest
    {

        #region Properties

        /// <summary>
        /// The id of the trigger record from the database on read.
        /// </summary>
        public long TriggerId { get; set; }

        /// <summary>
        /// The action type when this was triggered.
        /// </summary>
        public OTriggerRequestAction Action { get; set; }

        /// <summary>
        /// The columns on the table of which where changed, updated etc..
        /// </summary>
        public int[] Updated { get; set; }

        /// <summary>
        /// The name of the database this request came from.
        /// </summary>
        public string DatabaseName { get; set; }

        /// <summary>
        /// The table target of where the record was changed.
        /// </summary>
        public string Tablename { get; set; }

        /// <summary>
        /// The primary key column of the table.
        /// </summary>
        public string PkColumnName { get; set; }

        /// <summary>
        /// The id number of the record using <see cref="PkColumnName"/>.
        /// </summary>
        public long RecordId { get; set; }

        /// <summary>
        /// The record data which was minipulated on the database.
        /// </summary>
        public DataSet Data { get; set; }

        /// <summary>
        /// This is the service name of the plugin to which requests will go.
        /// </summary>
        public string ServiceName { get; set; }

        /// <summary>
        /// This is the instance name of the plugin in the stream.
        /// </summary>
        public string AddressName { get; set; }

        /// <summary>
        /// This is the key to which the server will allow data trasactions.
        /// </summary>
        public string AuthenticationKey { get; set; }

        #endregion

        #region Constuctor

        /// <summary>
        /// This will create the instance of the object to send over to the <see cref="OLiquidStreamEngine"/>.
        /// </summary>
        public ODataTriggerRequest()
        {

            TriggerId           = -1;
            Action              = OTriggerRequestAction.INSERT;
            Updated             = Array.Empty<int>();
            DatabaseName        = string.Empty;
            Tablename           = string.Empty;
            PkColumnName        = string.Empty;
            RecordId            = -1;
            Data                = new DataSet();
            ServiceName         = string.Empty;
            AddressName         = string.Empty;
            AuthenticationKey   = string.Empty;
        }

        #endregion

        #region Destuctor

        private bool IsDisposed = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!IsDisposed)
            {
                if (disposing)
                {

                    gd.Clear(Data);

                }

            }
            IsDisposed = true;
        }

        public void Dispose()
        {
            // Do not change this code.  Put cleanup code in Dispose(ByVal disposing As Boolean) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

    }

}
