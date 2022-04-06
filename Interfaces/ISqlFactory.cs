using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Reflection;

using K2host.Data.Attributes;
using K2host.Data.Classes;

namespace K2host.Data.Interfaces
{

    public interface ISqlFactory
    {

        string DropDatabaseStoredProc(string typeName);

        string GetMigrationVersion();

        string SetMigrationVersion(string version);

        string TruncateDatabaseTable(Type obj);

        string DropDatabaseTable(string typeName);

        string DropDatabaseStoredProc(Type obj);

        string DropDatabaseTable(Type obj);

        string CreateDatabaseStoredProc(Type obj);

        string CreateDatabaseTable(Type obj, IDataConnection Connection);

        string CreateMigrationVersionTable(string DatabaseName);

        bool MergeMigrationBackupAndRemove(Type obj, IDataConnection Connection);

        bool MergeMigrationRestoreAndRemove(Type obj, IDataConnection Connection);

        string GetTypeAttributeRepresentation(ODataTypeAttribute e);

        DbDataAdapter GetNewDataAdapter();

        DbCommand GetNewCommand(string cmdText);

        DbConnection GetNewConnection(IDataConnection Connection);

        DbParameter CreateParam(int datatype, ParameterDirection direction, object value, string paramname);

        DbParameter CreateParam(int datatype, object value, string paramname);

        string GetSqlDefaultValueRepresentation(int datatype, bool Encapsulate = false);

        string GetSqlRepresentation(IDataConnection Connection, PropertyInfo p, object value, out IEnumerable<DbParameter> parameters);

        string GetSqlRepresentation(IDataConnection Connection, int xxDbType, object value, out IEnumerable<DbParameter> parameters);

        int GetDbDataType(IDataConnection Connection, Type t);

        bool DeleteDatabase(IDataConnection Connection);

        string[] GetTables(IDataConnection Connection);

        bool TestDatabase(IDataConnection Connection, out DataTable record);

        bool CreateDatabase(IDataConnection Connection, string filePath);

        string TakeSkipBuildString(ODataTakeSkip e);
        
        string StuffFunctionBuildString(ODataStuffFunction e);

        string OrderBuildString(ODataOrder e, string prefix = "");

        string HavingSetBuildString(ODataHavingSet e, out IEnumerable<DbParameter> parameters, bool UseFieldPrefixing = false);
        
        string GroupSetBuildString(ODataGroupSet e, bool UseFieldPrefixing = false);

        string ForPathBuildString(ODataForPath e);

        string ApplySetBuildString(ODataApplySet e);

        string CaseBuildString(ODataCase e, out IEnumerable<DbParameter> parameters, bool UseFieldPrefixing = false);

        string ConditionBuildString(ODataCondition e, out IEnumerable<DbParameter> parameters, bool UsePrefix = false);

        string FieldSetUpdateBuildString(ODataFieldSet e, out IEnumerable<DbParameter> parameters);

        string FieldSetInsertBuildString(ODataFieldSet e, out IEnumerable<DbParameter> parameters);

        string FieldSetSelectBuildString(ODataFieldSet e, out IEnumerable<DbParameter> parameters, bool UseFieldPrefixing = false, bool UseFieldDefaultAlias = false);

        string InsertQueryBuildString(ODataInsertQuery e);

        string DeleteQueryBuildString(ODataDeleteQuery e);

        string UpdateQueryBuildString(ODataUpdateQuery e);

        string SelectQueryBuildString(ODataSelectQuery e);

    }

}