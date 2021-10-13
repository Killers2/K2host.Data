
# K2host.Data

An alternative to Entity Framework Core using simple DMM (Data Model Mapping) from code to MsSQL
The objects that can inherit this base can still be used with Entity Framework.

Also supports external data triggers using the trigger service to get any requests from a remote or local sqltrigger.

Nuget Package: https://www.nuget.org/packages/K2host.Data/

------------------------------------------------------------------------------------------------------------------------

# Getting Started.

Any classes will have to implement an interface which will inherit the IDataObject interface too.<br />
So we have a user interface which will inherit the data interface.

```c#
public interface IErpUser : IDataObject
{

}
```
At this point we can either create a model directly implementing the interface or we can create an abstract class using Types.<br />

This exmample is directly implementing the interface:

```c#
public class OErpUser : IErpUser
{


}
```

This exmample shows an example of the abstract that the user class will inherit based on the type.<br />

The IErpAttachment interface allows the model to be an attacment type to other models, this is optional.<br />
The important part to note is the ODataObject<T> which extends the models functions to allow for DMM.

```c#
public abstract class ErpObject<T> : ODataObject<T>, IErpAttachment
{


}
```

Now we can create our model class and defining the attributes on the properties.

The TSQLDataType attribute tells the ODataObject extentions how to compile when calling the database.
The ODataExceptionType attribute tells the ODataObject extentions to either ignore or only include based on the ODataExceptionType.

```c#
public class OErpUser : ErpObject<OErpUser>, IErpUser
{

    [TSQLDataType(SqlDbType.BigInt)]
    public long CompanyId { get; set; }

    [TSQLDataType(SqlDbType.BigInt)]
    public long DepartmentId { get; set; }

    [TSQLDataType(SqlDbType.NVarChar, 255)]
    public string Username { get; set; }

    [TSQLDataException(ODataExceptionType.NON_INSERT | ODataExceptionType.NON_UPDATE | ODataExceptionType.NON_SELECT | ODataExceptionType.NON_DELETE | ODataExceptionType.NON_CREATE)]
    public IErpUserRole[] Roles { get; set; }

    public OErpUser(string connectionString)
        : base(connectionString)
    {

    }

}
```
Every model you create will always need the connection string to your database passed from some other instance or / and variable.<br /><br />
We can also add some attributes, this first one is for remapping the object to the database, for example:

```c#
[ODataReMap("OErpUser")]
public class TErpUser : ErpObject<TErpUser>, IErpUser
{

    [TSQLDataType(SqlDbType.BigInt)]
    public long CompanyId { get; set; }

    [TSQLDataType(SqlDbType.BigInt)]
    public long DepartmentId { get; set; }

    public TErpUser(string connectionString)
        : base(connectionString)
    {

    }

}
```
However we can also create and add property attributes.

```c#
public class TErpUser : ErpObject<TErpUser>, IErpUser
{

    [ODataEncryption(0)]
    [ODataType(SqlDbType.NVarChar, 4000)]
    public string AesHash { get; set; }

    public TErpUser(string connectionString)
        : base(connectionString)
    {

    }

}
```
Where the ODataEncryption attribute will deal with changing the value of the property when reading and writing to database to model.<br />
We can also set the order for the attribute if there are more than one assigned.



# Example of database migration.

To migrate the database and setup you can use the below.<br />
The migrator takes care of changes like changing a model and adding or removing a property the migrator will add or remove a column.<br />
But like EntityFramework, be aware removing a property will remove the data when removing a column, the same when removing an unwanted model.

```c#
new ODataMigrationTool("YOUR CONNECTION STRING")
{
    Path            = "THE FULL PATH TO THE DATABASE MDF FILE",
    Version         = "YOUR MIGRATION VERSION CODE NAME",
    GetDbContext    = () => {
        return AppDomain.CurrentDomain
            .GetAssemblies()
            .SelectMany(t => t.GetTypes())
            .Where(t => t.IsClass && t.IsPublic && t.Namespace == "K2host.Erp.Classes")
            .Append(typeof(OCertification))
            .Append(typeof(ODataTrigger))
            .Append(typeof(OLogEntrie));
    },
    GetDbContextCustom = (e) => { }
}
.Initiate()
.Dispose();
```

# Examples of database queries.

This example uses a CASE as a field set based on the gender and condition set based on the firstname.<br />
The query will return a single user with the gender name string field based on the gender column.<br />
The above will throw back an exception on error using the out method while return a default / null value, which inturn will allow options for error handling.

# Examples of the CASE.

```c#
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
            Values      = new object[] { "David" }
        }
    },
    null, 
    null,
    "YOUR CONNECTION STRING", 
    out ODataException Exception
);
```

To extend the case in a select type query we can create and array of them show below:
Specifing the Limit / Top count to 0 we are saying get all.

```c#
OErpUser[] users = OErpUser.List(0,
    new ODataCondition[] {
        new ODataCondition() { 
            Case = new ODataCase[] {
                new ODataCase() {
                    When = new ODataCondition[] {
                        new ODataCondition()
                        {
                            Column      = typeof(OErpUser).GetProperty("Firstname"),
                            Operator    = ODataOperator.EQUAL,
                            Values      = new object [] { "David" },
                            FollowBy    = ODataFollower.AND
                        },
                        new ODataCondition()
                        {
                            Column      = typeof(OErpUser).GetProperty("Surname"),
                            Operator    = ODataOperator.EQUAL,
                            Values      = new object [] { "Host" }
                        }
                    },
                    Then = "Dont Care"
                },
                new ODataCase() {
                    When = new ODataCondition[] {
                        new ODataCondition()
                        {
                            Column      = typeof(OErpUser).GetProperty("Firstname"),
                            Operator    = ODataOperator.EQUAL,
                            Values      = new object [] { "David" }
                        }
                    },
                    Then = "David Host",
                    Else = typeof(OErpUser).GetProperty("Firstname")
                }

            },
            Operator = ODataOperator.EQUAL,
            Values = new object[] { "David Host" } 
        }
    },
    null, 
    null,
    "YOUR CONNECTION STRING", 
    out ODataException Exception
).ToArray();
```

# Examples of the SUB QUERIES.

This next example shows sub queries in fields.

```c#
OErpUser user = OErpUser.Retrieve(
    new ODataFieldSet[] {
        new ODataFieldSet() {
            SubQuery = new ODataSelectQuery() {
                Fields = new ODataFieldSet[]{
                    new ODataFieldSet() { 
                        Function    = ODataFunction.COUNT,
                        Column      = typeof(OErpCompany).GetProperty("Uid")
                    }
                },
                From = typeof(OErpCompany),
                Where = new ODataCondition[] {
                    new ODataCondition() {
                        Column      = typeof(OErpCompany).GetProperty("Uid"),
                        Operator    = ODataOperator.EQUAL,
                        Values      = new object[] { 1 }
                    }
                }
            },
            Alias = "CompanyCount"
        }
    },
    new ODataCondition[] {
        new ODataCondition() {
            Column      = typeof(OErpUser).GetProperty("Firstname"),
            Operator    = ODataOperator.EQUAL,
            Values      = new object[] { "David" }
        }
    },
    null,
    null,
    "YOUR CONNECTION STRING", 
    out ODataException Exception
);
```

This next example shows sub queries in a where clause.

```c#
OErpUser[] users = OErpUser.List(0,
    new ODataCondition[] {
        new ODataCondition() {
            SubQuery = new ODataSelectQuery() {
                Fields = new ODataFieldSet[] {
                    new ODataFieldSet() {
                        Column = typeof(OErpCompany).GetProperty("Uid")
                    }
                },
                From = typeof(OErpCompany),
                Where = new ODataCondition[] {
                    new ODataCondition() {
                        Column      = typeof(OErpCompany).GetProperty("Name"),
                        Operator    = ODataOperator.EQUAL,
                        Values      = new object[] { "K2host Services" }
                    }
                }
            },
            Operator    = ODataOperator.EQUAL,
            Values      = new object[] { 1 }
        }
    },
    null,
    null,
    "YOUR CONNECTION STRING", 
    out ODataException Exception
).ToArray();
```
# Examples of the TAKE SKIP.

The take and skip is realy used for pagination with datasets or to limit a sert of daya for analytics etc...

```c#
OErpCommerceManufacturer[] Manufacturers = OErpCommerceManufacturer.List(0,
    new ODataOrder[] {
        new ODataOrder() {
            Column = typeof(OErpCommerceManufacturer).GetProperty("Uid"),
            Order = ODataOrderType.ASC
        }
    },
    new ODataTakeSkip {
        Take = 10,
        Skip = 0
    },
    "YOUR CONNECTION STRING", 
    out ODataException Exception
).ToArray();
```
# Examples of query building.

You can use the builders in this way to compile the query to value the query string used to pull data from the database.<br />
This is done by using the ToString() method on the builders instance. (Unit testing is also a good way using the builders).
This example shows the select query builder.

```c#
ODataSelectQuery s = new() {
    UseFieldDefaultAlias    = false, 
    UseFieldPrefixing       = false,
    Fields = new ODataFieldSet[] {
        new ODataFieldSet() { Column = typeof(OErpCommerceProduct).GetProperty("Datestamp") },
        new ODataFieldSet() {
            Function    = ODataFunction.COUNT,
            Column      = typeof(OErpCommerceProduct).GetProperty("Uid"),
            Alias       = "ProductCount"
        },
        new ODataFieldSet() {
            Function    = ODataFunction.SUM,
            Column      = typeof(OErpCommerceProduct).GetProperty("CostPrice"),
            Alias       = "TotalCostPrice"
        }
    },
    From = typeof(OErpCommerceProduct),
    Joins = new ODataJoinSet[] {
        new ODataJoinSet() {
            JoinType        = ODataJoinType.INNER,
            JoinQuery       = new ODataSelectQuery() {
                UseFieldPrefixing       = true,
                UseFieldDefaultAlias    = false,
                Fields  = OErpCommerceManufacturer.GetFields(ODataFieldSetType.SELECT),
                From    = typeof(OErpCommerceManufacturer),
                Where   = new ODataCondition[] {
                    new ODataCondition() {
                        Column      = typeof(OErpCommerceManufacturer).GetProperty("Uid"),
                        Operator    = ODataOperator.LESS_THAN_OR_EQUAL,
                        Values      = new object[] { 200 }
                    }
                }
            },
            JoinOnField     = typeof(OErpCommerceManufacturer).GetProperty("Uid"),
            JoinEqualsField = typeof(OErpCommerceProduct).GetProperty("ManufacturerId")
        }
    },
    Where = new ODataCondition[] {
        new ODataCondition() { 
            Column      = typeof(OErpCommerceProduct).GetProperty("ManufacturerId"),
            Operator    = ODataOperator.EQUAL,
            Values      = new object[] { 96 }
        }
    },
    Group = new ODataGroupSet[] { new ODataGroupSet() { Column = typeof(OErpCommerceProduct).GetProperty("Datestamp") } },
    Having = new ODataHavingSet[] {
        new ODataHavingSet() { 
            Function    = ODataFunction.COUNT,
            Column      = typeof(OErpCommerceProduct).GetProperty("Uid"),
            Operator    = ODataOperator.GREATER_THAN,
            Value       = 1, 
            FollowBy    = ODataFollower.AND
        },
        new ODataHavingSet() {
            Function    = ODataFunction.SUM,
            Column      = typeof(OErpCommerceProduct).GetProperty("CostPrice"),
            Operator    = ODataOperator.GREATER_THAN,
            Value       = 5.00M
        }
    },
    Order = new ODataOrder[] { new ODataOrder() { Column = typeof(OErpCommerceProduct).GetProperty("Datestamp"), Order = ODataOrderType.DESC } }
};

Debug.Write(s.ToString());
```

This example shows the update query builder.

```c#
ODataUpdateQuery s = new() {
      UseFieldDefaultAlias    = false
    , UseFieldPrefixing       = false
    , Fields = new ODataFieldSet[] {
        new ODataFieldSet() { 
            Column      = typeof(OErpCommerceSupplier).GetProperty("Name"), 
            NewValue    = "Computer 2000 NewTest"
        },
        new ODataFieldSet() {
            Column      = typeof(OErpCommerceSupplier).GetProperty("Credit"),
            NewValue    = 26000.00M
        }
    }
    , From = typeof(OErpCommerceSupplier)
    , Where = new ODataCondition[] {
        new ODataCondition() {
            Column      = typeof(OErpCommerceSupplier).GetProperty("Uid"),
            Operator    = ODataOperator.EQUAL,
            Values      = new object[] { 1 }
        }
    }
};

Debug.Write(s.ToString());
```

This example shows the update query builder.

```c#
ODataInsertQuery s = new()
{
      UseIdentityInsert = false
    , To = typeof(OErpCommerceSupplier)
    , Fields = new ODataFieldSet[] {
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("ParentId"),        NewValue = 0 },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("CompanyId"),       NewValue = 0 },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("DepartmentId"),    NewValue = 0 },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("Name"),            NewValue = "Computers 2020" },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("Credit"),          NewValue = 27000.00M },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("Logo"),            NewValue = string.Empty },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("Url"),             NewValue = string.Empty },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("Username"),        NewValue = string.Empty },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("Password"),        NewValue = string.Empty },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("Status"),          NewValue = 0 },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("AccountManagerId"),NewValue = 0 },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("ViewOrder"),       NewValue = 0 },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("CategoryId"),      NewValue = 0 },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("Flags"),           NewValue = 0 },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("Updated"),         NewValue = DateTime.Now },
        new ODataFieldSet() { Column = typeof(OErpCommerceSupplier).GetProperty("Datestamp"),       NewValue = DateTime.Now },
    }
};

Debug.Write(s.ToString());
```

# Example of query with applies and stuff.

This example shows off the Stuff function query and an advanced query.

```c#
ODataSelectQuery s = new() {
    UseFieldDefaultAlias    = false,
    UseFieldPrefixing       = true,
    IncludeApplyFields      = false, 
    IncludeJoinFields       = false,
    Fields = typeof(OErpUserRole).GetFieldSets(ODataFieldSetType.SELECT).Append(
        new ODataFieldSet() { 
            Case = new ODataCase[] {
                new ODataCase() {
                    When = new ODataCondition[] {
                        new ODataCondition() { 
                            Column = new ODataPropertyInfo("p.[Result]", typeof(String)),
                            Operator = ODataOperator.IS,
                            Values = new object[] { null }
                        }
                    },
                    Then = "0,",
                    Else = new ODataPropertyInfo("p.[Result]", typeof(String))
                }
            },
            Alias = "Policies"
        }
    ).ToArray(),
    From = typeof(OErpUserRole),
    Joins = new ODataJoinSet[] {
        new ODataJoinSet() { 
            JoinType = ODataJoinType.INNER,
            Join = typeof(OErpUserRoleConnector),
            JoinOnField  = typeof(OErpUserRoleConnector).GetProperty("EntityId"),
            JoinEqualsField = typeof(OErpUserRole).GetProperty("Uid")
        }
    },
    Applies = new ODataApplySet[] {
        new ODataApplySet() { 
            ApplyType = ODataApplyType.CROSS,
            Query = new ODataSelectQuery() { 
                UseFieldDefaultAlias = false,
                UseFieldPrefixing = false,
                Stuff = new ODataStuffFunction() { 
                    Query = new ODataSelectQuery() {
                        UseFieldDefaultAlias = false,
                        UseFieldPrefixing = false,
                        Fields = new ODataFieldSet[] {
                            new ODataFieldSet() { 
                                Cast = ODataCast.VARCHAR,
                                Column = typeof(OErpUserRolePolicy).GetProperty("Ident"),
                                Suffix = ",",
                                Alias = "text()"
                            }
                        },
                        From = typeof(OErpUserRolePolicy),
                        Where = new ODataCondition[] {
                            new ODataCondition() {
                                Column = typeof(OErpUserRolePolicy).GetProperty("ParentId"),
                                Operator = ODataOperator.EQUAL,
                                Values = new object[] { typeof(OErpUserRoleConnector).GetProperty("EntityId") }
                            }
                        },
                        ForPath = new ODataForPath() { 
                            PathType = ODataForPathType.XML,
                            Path = string.Empty
                        }
                    },
                    StartPosistion = 1, 
                    NumberOfChars = 0, 
                    ReplacementExpression = string.Empty
                },
                Alias = "Result"
            },
            Alias = "p"
        }
    },
    Where = new ODataCondition[] {
        new ODataCondition() {
            Column = typeof(OErpUserRoleConnector).GetProperty("UserId"),
            Operator = ODataOperator.EQUAL,
            Values = new object[] { 1 }
        }
    }
};

Debug.Write(s.ToString());
```
For more examples you can look at the K2host.Erp and navigate to the K2host.Erp.Abstarct > ErpObject which implements the IErpAttachment.<br />
There all of the k2host libraries will mostly use the K2host.Data Library.
