/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/

using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.EntityFrameworkCore;

using K2host.Data.Factories;
using K2host.Data.Interfaces;
using K2host.Data.Enums;
using K2host.Data.Delegates;
using K2host.Core;

namespace K2host.Data.Classes
{
    public class ODataContext : DbContext
    {
        
        /// <summary>
        /// The self instance which can be accessed globally set when the data context is created.
        /// </summary>
        public static ODataContext Instance { get; set; } = default;

        /// <summary>
        /// The database connection in the context of the domain.
        /// </summary>
        protected static IDataOptions Options { get; set; }

        /// <summary>
        /// Used to collect the classes that will be migrated to the database.
        /// </summary>
        public OnGetDbContext GetTypeContext { get; set; }

        /// <summary>
        /// The call back for getting the list of entity types from the applcation.
        /// </summary>
        public OnGetDbContext OnGetDbTypes { get; set; }
        
        /// <summary>
        /// The call back for method overrided OnConfiguring using the <see cref="DbContextOptionsBuilder"/> .
        /// </summary>
        public OnConfiguringDbContext OnConfigure { get; set; }

        /// <summary>
        /// The call back for method overrided OnModelCreating using the <see cref="ModelBuilder"/> .
        /// </summary>
        public OnModelCreatingDbContext OnModelCreate { get; set; }
       
        /// <summary>
        /// The self contained list of entity types defind in this instance.
        /// </summary>
        private IEnumerable<Type> DbTypes { get; set; } = Array.Empty<Type>();

        /// <summary>
        /// The constructor
        /// </summary>
        public ODataContext()
            : base()
        {
            Instance = this;
        }

        /// <summary>
        /// The constructor
        /// </summary>
        /// <param name="options"></param>
        public ODataContext(DbContextOptions options)
            : base(options)
        {
            Instance = this;
        }

        /// <summary>
        /// The db context configuration override.
        /// </summary>
        /// <param name="optionsBuilder"></param>
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            base.OnConfiguring(optionsBuilder);

            OnConfigure?.Invoke(optionsBuilder);

            DbTypes = OnGetDbTypes?.Invoke();

        }

        /// <summary>
        /// The db context model creating override.
        /// </summary>
        /// <param name="modelBuilder"></param>
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            //Doing this dynamically means the table names in the db will have to match the entity name .
            DbTypes?.ForEach(type => {
                modelBuilder
                    .GetType()
                    .GetMethods()
                    .Where(m => m.Name == "Entity")
                    .FirstOrDefault()
                    .MakeGenericMethod(new Type[] { type })
                    .Invoke(modelBuilder, null);
            });

            OnModelCreate?.Invoke(modelBuilder);

        }
        
        /// <summary>
        /// Returns a dbset for running IQueryable expressions on a type.
        /// </summary>
        /// <typeparam name="I"></typeparam>
        /// <returns></returns>
        public static IQueryable<I> From<I>() 
            where I : class, IDataObject

        {
            return Instance.Set<I>();
        }

        /// <summary>
        /// Used to apply options to the db context used in the lib
        /// </summary>
        /// <typeparam name="I"></typeparam>
        /// <param name="e"></param>
        public static void UseWithDMM(Action<ODataOptions> e) 
        {

            if (Options != null)
                Options.Dispose();

            Options = Activator.CreateInstance<ODataOptions>();
            Options.Connections = new();

            Options.SqlFactories.Add(OConnectionDbType.SqlDbType,     new OMsSqlFactory());
            Options.SqlFactories.Add(OConnectionDbType.MySqlDbType,   new OMySqlFactory());
            Options.SqlFactories.Add(OConnectionDbType.OracleDbType,  new OOracleSqlFactory());

            e.Invoke((ODataOptions)Options);

            if (Options.Connections.Count > 0)
                Options.Primary = Options.Connections.Values.First();

        }
      
        /// <summary>
        /// Used to apply options to the db context used in the lib
        /// </summary>
        /// <typeparam name="I"></typeparam>
        /// <param name="e"></param>
        public static ODataContext UseWithEFCore(Action<ODataOptions> e, DbContextOptions efcoreoptions = null)
        {

            if (Options != null)
                Options.Dispose();

            Options = Activator.CreateInstance<ODataOptions>();
            Options.Connections = new();

            Options.SqlFactories.Add(OConnectionDbType.SqlDbType, new OMsSqlFactory());
            Options.SqlFactories.Add(OConnectionDbType.MySqlDbType, new OMySqlFactory());
            Options.SqlFactories.Add(OConnectionDbType.OracleDbType, new OOracleSqlFactory());

            e.Invoke((ODataOptions)Options);

            if (Options.Connections.Count > 0)
                Options.Primary = Options.Connections.Values.First();

            if (efcoreoptions != null)
                _ = new ODataContext(efcoreoptions);
            else
                _ = new ODataContext();

            Instance.OnConfigure = (optionsBuilder) => { 
                optionsBuilder.UseSqlServer(Options.Primary.ToString()); 
            };

            return Instance;

        }

        /// <summary>
        /// Used to return the <see cref="IDataConnection"/> object
        /// </summary>
        /// <returns></returns>
        public static bool SetPrimary(string key)
        {

            if (!Options.Connections.ContainsKey(key))
                throw new ODataException("There were no connection found with the key supplied.");

            Options.Primary = Options.Connections[key];

            return true;
        }

        /// <summary>
        /// Used to return the <see cref="IDataConnection"/> object
        /// </summary>
        /// <returns></returns>
        public static IDataConnection Connection()
        {
            return Options.Primary;
        }

        /// <summary>
        /// Used to return the <see cref="IDataConnection"/> object
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static IDataConnection Connection(string key)
        {
            if (!Options.Connections.ContainsKey(key))
                throw new ODataException("There were no connection found with the key supplied.");

            return Options.Connections[key];
        }

        /// <summary>
        /// Used to return the <see cref="IDataConnection"/> object
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static IDataConnection ConnectionSearch(string serverName)
        {
            return Options.Connections.Values.FirstOrDefault(c => c.Server.ToLower() == serverName.ToLower());
        }
       
        /// <summary>
        /// Used to return the <see cref="IDataConnection"/> object
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static IDataConnection Connection(OConnectionType connectionType)
        {
            return Options.Connections.Values.FirstOrDefault(c => c.ConnectionType == connectionType);
        }
       
        /// <summary>
        /// Used to return the <see cref="IDataPropertyConverter"/> list
        /// </summary>
        /// <typeparam name="I"></typeparam>
        /// <returns></returns>
        public static List<IDataPropertyConverter> PropertyConverters()
        {
            return Options.PropertyConverters;
        }

        /// <summary>
        /// Returns the <see cref="ISqlFactory"/> based on type of enum dbtype
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public static ISqlFactory GetFactory(OConnectionDbType e)
        {
            return Options.SqlFactories[e];
        }

    }
}
