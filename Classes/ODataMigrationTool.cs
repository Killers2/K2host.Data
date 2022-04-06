/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using K2host.Core;
using K2host.Data.Delegates;
using K2host.Data.Extentions.ODataConnection;
using K2host.Data.Interfaces;

using gd = K2host.Data.OHelpers;

namespace K2host.Data.Classes
{

    /// <summary>
    /// This class helps create a database migration tool set for your applciation. 
    /// </summary>
    public class ODataMigrationTool : IDisposable
    {
        
        /// <summary>
        /// This the version of the database in your application.
        /// </summary>
        public string Version { get; set; }
        
        /// <summary>
        /// This is the database path (full file path to the data mdf file)
        /// </summary>
        public string Path { get; set; }

        /// <summary>
        /// Used to collect the classes that will be migrated to the database.
        /// </summary>
        public OnGetDbContext GetDbContext { get; set; }
       
        /// <summary>
        /// This is used for any other classes made to migrate manually before the completion of the migration method.
        /// </summary>
        public OnGetDbContextCustom GetDbContextCustom { get; set; }

        /// <summary>
        /// This the connection string to the database on the server.
        /// </summary>
        IDataConnection Connection { get; }

        /// <summary>
        /// Creates the instance for this class
        /// </summary>
        public ODataMigrationTool(IDataConnection connection) 
        {
            Connection          = connection;
            Version             = string.Empty;
            Path                = string.Empty;
            GetDbContext        = null;
            GetDbContextCustom  = null;
        }

        /// <summary>
        /// This is used at your application launch to create / migrate and make auto changes to the data if the version has changed.
        /// </summary>
        public ODataMigrationTool Initiate() 
        {
            
            if (string.IsNullOrEmpty(Version))
                throw new NullReferenceException("The database version string applied was empty.");

            //Create db if it dose't exist. and set up a version number for the system.
            if (!Connection.TestDatabase(out DataTable record))
            {
                Connection.CreateDatabase(Path);
                Connection.Query(CommandType.Text, ODataObject<ODataTrigger>.CreateMigrationVersionTable(Connection.Database), null);
            }

            //Lets make any changes to the database if there version has changed.
            DataSet MigrationVersion = Connection.Get(CommandType.Text, ODataObject<ODataTrigger>.GetMigrationVersion(), null);

            string cv = MigrationVersion.Tables[0].Rows[0][0].ToString();

            gd.Clear(MigrationVersion);

            if (!Version.Equals(cv))
            {
                IEnumerable<Type> DbContext = GetDbContext?.Invoke();

                Type odo = AppDomain.CurrentDomain
                    .GetAssemblies()
                    .SelectMany(t => t.GetTypes())
                    .Where(t => t.FullName == "K2host.Data.Classes.ODataObject`1")
                    .FirstOrDefault();

                DbContext.ForEach(i => {
                    object o = Activator.CreateInstance(odo.MakeGenericType(new[] { i }));
                    o.GetType().GetMethod("MergeMigration").Invoke(o, new object[] { Connection });
                    o.GetType().GetMethod("Dispose").Invoke(o, null);
                });

                string[] TablesToRemove = Connection
                    .GetTables()
                    .Filter(t => {
                        return !DbContext.Where(c => c.GetMappedName() == t.Remove(0, 4) || c.GetMappedName().ToLower() == t.Remove(0, 4)).Any();
                    });

                TablesToRemove.ForEach(t => {
                    Connection.Query(CommandType.Text, ODataObject<ODataTrigger>.DropDatabaseTable(t.Remove(0, 4)), null);
                    Connection.Query(CommandType.Text, ODataObject<ODataTrigger>.DropDatabaseStoredProc(t.Remove(0, 4)), null);
                });

                GetDbContextCustom?.Invoke(Connection);

                Connection.Query(CommandType.Text, ODataObject<ODataTrigger>.SetMigrationVersion(Version), null);

            }

            return this;

        }

        #region Deconstuctor

        private bool IsDisposed = false;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!IsDisposed)
                if (disposing)
                {
                    GetDbContext = null;
                    GetDbContextCustom = null;
                }
            IsDisposed = true;
        }

        #endregion

    }

}
