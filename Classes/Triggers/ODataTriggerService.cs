/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2020-11-01                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Data;
using System.Net;
using System.Text;

using Newtonsoft.Json;

using K2host.Threading.Classes;
using K2host.Data.Interfaces;
using K2host.Data.Extentions;
using K2host.Data.Delegates;
using K2host.Sockets.Tcp;
using K2host.Sockets.Delegates;
using K2host.Threading.Interface;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class helps create a database connections string. 
    /// </summary>
    public class ODataTriggerService : IDataTriggerService
    {
        
        #region Properties

        /// <summary>
        /// This is the system Thread Manager.
        /// </summary>
        public IThreadManager ThreadManager { get; }

        /// <summary>
        /// The TCP Server object required for listening to the port awaiting data.
        /// </summary>
        public OServer TriggerServer { get; set; }

        /// <summary>
        /// When a request is returned we process and send on via this event handler.
        /// </summary>
        public OTriggeredEventHandler OnTriggered { get; set; }

        #endregion
        
        #region Constructor

        /// <summary>
        /// Creates an instance of this class sql trigger service.
        /// </summary>
        /// <param name="ThreadManager"></param>
        /// <param name="port"></param>
        public ODataTriggerService(IThreadManager threadManager, IPAddress ipAddress, int port)
        {
            ThreadManager               = threadManager;
            TriggerServer               = new OServer(ThreadManager, ipAddress, port);
            TriggerServer.DataReceived += new ODataReceivedEventHandler(TriggerServerDataReceived);
        }

        #endregion

        #region Methods

        /// <summary>
        /// Starts the tcp server as part of this service
        /// </summary>
        public void Start()
        {
            if (TriggerServer != null)
                TriggerServer.StartServer();
        }

        /// <summary>
        /// Stops the tcp server as part of this service
        /// </summary>
        public void Stop()
        {
            if (TriggerServer != null)
                TriggerServer.StopServer();
        }

        /// <summary>
        /// Handler for processing data and sending on to linked method.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="data"></param>
        public void TriggerServerDataReceived(Sockets.Tcp.OConnection sender, byte[] data)
        {

            if (OnTriggered == null)
                return;

            try
            {

                string output = Encoding.ASCII.GetString(data);

                try
                {
                    TriggerServer.CloseClient(sender);
                }
                catch (Exception) { }

                OnTriggered?.Invoke(
                    ODataTriggerRequestExtention.BuildTriggerRequest(
                            JsonConvert.DeserializeObject<DataSet>(output)
                        )
                    );

            }
            catch (Exception)
            {
                throw;
            }

        }

        #endregion

        #region Destructor

        bool IsDisposed = false;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            GC.Collect(0, GCCollectionMode.Optimized, true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!IsDisposed)
            {
                if (disposing)
                {
                    if (TriggerServer != null)
                    {

                        try
                        {
                            TriggerServer.Dispose();
                        }
                        catch { }
                    }
                }
            }

            IsDisposed = true;
        }

        #endregion

    }

}
