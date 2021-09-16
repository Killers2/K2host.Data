
# K2host.Data

An alternative to Entity Framework Core using simple DMM from code to MsSQL
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
Every model you create will always need the connection string to your database passed from some other instance or / and variable.

# Examples of database queries.

This example uses a CASE as a field set based on the gender and condition set based on the firstname.<br />
The query will return a single user with the gender name string field based on the gender column.<br />
The above will throw back an exception on error using the out method while return a default / null value, which inturn will allow options for error handling.
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




