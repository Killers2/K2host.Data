/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using K2host.Data.Classes;
using System;


namespace K2host.Data.Interfaces
{

    /// <summary>
    /// This interface allows you to create a class derived from the core components needed on storing information. 
    /// </summary>
    public interface IDataTrigger : IDisposable
    {

        /// <summary>
        /// The string type of which its either MS SQL Server or Agent
        /// </summary>
        string Type { get; set; }

        /// <summary>
        /// The Connection string to the database.
        /// </summary>
        string Connection { get; set; }

        /// <summary>
        /// This is to determin weather the Uninstall process removes the setup or and uninstall locally.
        /// </summary>
        bool IsLocal { get; set; }

        /// <summary>
        /// This is to authenticate transactions / triggers from database servers.
        /// </summary>
        string AuthenticationKey { get; set; }

        /// <summary>
        /// This will either by the setup <see cref="IDataTriggerFileSetup"/> or a Json string form from an Agent install
        /// </summary>
        byte[] Configuration { get; set; }

    }

}
