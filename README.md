
# K2host.Data

An alternative to just using Entity Framework Core by combining a simple DMM (Data Model Mapping)
The objects that can inherit this base can still be used with Entity Framework.

Also supports external data triggers using the trigger service to get any requests from a remote or local sqltrigger.

Nuget Package: https://www.nuget.org/packages/K2host.Data/

For use with Efcore we set the ODataContext to use with EFCore.

    ODataContext server = ODataContext.UseWithEFCore(o => {
        o.Connections.Add("Main", new ODataConnection("XXXXXXXXXX", OConnectionType.SQLStandardSecurity));
    });

    server.OnGetDbTypes = () => {
        return AppDomain.CurrentDomain
            .GetAssemblies()
            .SelectMany(t => t.GetTypes())
            .Where(t => t.IsClass && t.IsPublic && (t.Namespace == "K2host.Erp.Classes"))
            .ToArray()
            .Append(typeof(ODataTrigger))
            .ToArray();
    };

    server.OnModelCreate = (modelBuilder) => {
        modelBuilder.SetQueryFilterOnAllEntities<IDataObject>(p => (p.Flags >= 0) && ((p.Flags & (long)ODataFlags.Deleted) != (long)ODataFlags.Deleted));
    };

    server.Database.EnsureCreated();


If we do not want to use efcore we can just:

    ODataContext.UseWithDMM(o => {
        o.Connections.Add("Main", new ODataConnection("XXXXXXXXXX", OConnectionType.SQLStandardSecurity));
    });

    server.OnGetDbTypes = () => {
        return AppDomain.CurrentDomain
            .GetAssemblies()
            .SelectMany(t => t.GetTypes())
            .Where(t => t.IsClass && t.IsPublic && (t.Namespace == "K2host.Erp.Classes"))
            .ToArray()
            .Append(typeof(ODataTrigger))
            .ToArray();
    };

So now with this enabled we can used the odata objects with efcore like this:

    OErpUser[] users2 = ODataContext.Instance
        .Set<OErpUser>()
        .Where(u => u.Gender == OGenderStatus.Male)
        .ToArray();

ODATA with EFCore (supports none linked joins of types, custom sql building, efcore IQueryable) with IDataObject

    OErpUser[] users1 = OErpUser
        .Select()
        .Where(u => u.Gender == OGenderStatus.Male)
        .ToArrayOrDefault(out _);

    OErpUser user3 = ODataContext.Instance
        .Set<OErpUser>()
        .Where(u => u.Uid == 1)
        .ToFirstOrDefault(out _);

But we can still use custom query building.

    OErpUser user = OErpUser.Retrieve(
        new ODataFieldSet[] {
            new ODataFieldSet() { 
                Case = new ODataCase[] { 
                    new ODataCase() { 
                        When = new ODataCondition[] { 
                            new ODataCondition() { 
                                Column      = typeof(OErpUser).GetProperty("Gender"),
                                Operator    = ODataOperator.EQUAL,
                                Values      = new object [] { 0 }
                            }
                        },
                        Then = "Male",
                        Else = "Female"
                    }
                },
                Alias = "GenderName"
            }
        },
        new ODataCondition[] {
            new ODataCondition() {
                Column      = typeof(OErpUser).GetProperty("Firstname"),
                Operator    = ODataOperator.EQUAL,
                Values      = new object[] { "Tony" }
            }
        },
        null, 
        null,
        out ODataException _
    );