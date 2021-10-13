/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Data;
using System.Reflection;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Linq;

using Newtonsoft.Json;

using K2host.Core;
using K2host.Data.Enums;
using K2host.Data.Interfaces;
using K2host.Data.Attributes;

using gl = K2host.Core.OHelpers;
using gd = K2host.Data.OHelpers;

namespace K2host.Data.Classes
{
    
    /// <summary>
    /// This object allows you to save a type to an sql database using generic types.
    /// </summary>
    /// <typeparam name="I"></typeparam>
    [Serializable]
    public class ODataObject<I> : IDataObject, IDisposable
    {

        #region Properties

        /// <summary>
        /// The record auto id of this type in its own database table.
        /// </summary>
        [ODataType(SqlDbType.BigInt)]
        [ODataException(ODataExceptionType.NON_INSERT | ODataExceptionType.NON_UPDATE)]
        public long Uid { get; private set; }

        /// <summary>
        /// The parent Id of the given type maps to a record in a table in the database. 
        /// </summary>
        [ODataType(SqlDbType.BigInt)]
        public long ParentId { get; set; }

        /// <summary>
        /// The type name as a string which maps to a table in the database.
        /// </summary>
        [ODataType(SqlDbType.NVarChar, 255)]
        public string ParentType { get; set; }

        /// <summary>
        /// The record flags if any.
        /// </summary>
        [ODataType(SqlDbType.BigInt)]
        public long Flags { get; set; }

        /// <summary>
        /// The date and time of the recored being updated.
        /// </summary>
        [ODataType(SqlDbType.DateTime)]
        public DateTime Updated { get; set; }

        /// <summary>
        /// The date and time of the recored was inserted.
        /// </summary>
        [ODataType(SqlDbType.DateTime)]
        [ODataException(ODataExceptionType.NON_UPDATE)]
        public DateTime Datestamp { get; set; }

        /// <summary>
        /// The objects database connection string.
        /// </summary>
        [JsonIgnore]
        [ODataException(ODataExceptionType.NON_INSERT | ODataExceptionType.NON_UPDATE | ODataExceptionType.NON_SELECT | ODataExceptionType.NON_DELETE | ODataExceptionType.NON_CREATE)]
        public string ConnectionString { get; set; }

        /// <summary>
        /// This holding space allows to hold information / object data while in use.
        /// </summary>
        [JsonIgnore]
        [ODataException(ODataExceptionType.NON_INSERT | ODataExceptionType.NON_UPDATE | ODataExceptionType.NON_SELECT | ODataExceptionType.NON_DELETE | ODataExceptionType.NON_CREATE)]
        public object Tag { get; set; }

        /// <summary>
        /// This holds the joined objects when using <see cref="ODataJoinSet"/> from sql.
        /// </summary>
        [ODataException(ODataExceptionType.NON_INSERT | ODataExceptionType.NON_UPDATE | ODataExceptionType.NON_SELECT | ODataExceptionType.NON_DELETE | ODataExceptionType.NON_CREATE)]
        public List<IDataObject> Joins { get; set; }

        /// <summary>
        /// This holds the extended field values forn cases, static and other manual fields.
        /// </summary>
        [ODataException(ODataExceptionType.NON_INSERT | ODataExceptionType.NON_UPDATE | ODataExceptionType.NON_SELECT | ODataExceptionType.NON_DELETE | ODataExceptionType.NON_CREATE)]
        public Dictionary<string, object> ExtendedColumns { get; set; }

        #endregion

        #region Constructor

        /// <summary>
        /// This creates the instance of the generic type
        /// </summary>
        /// <param name="connectionString"></param>
        public ODataObject(string connectionString)
        {

            ConnectionString = connectionString;

            Uid             = -1;
            ParentId        = 0;
            ParentType      = string.Empty;
            Flags           = 0;
            Updated         = DateTime.Now;
            Datestamp       = DateTime.Now;
            Tag             = null;
            Joins           = new List<IDataObject>();
            ExtendedColumns = new Dictionary<string, object>();
        }

        #endregion

        #region Methods

        /// <summary>
        /// This saves the object to the database.
        /// </summary>
        public IDataObject Save(bool sproc = true, string typeName = "")
        {

            if (string.IsNullOrEmpty(typeName))
                typeName = this.GetType().GetMappedName();

            string error = "K2host.Data." + typeName + ": Error getting this object from the database. ";

            DataSet dts;

            //Lets set the date stamp of the save action
            Updated = DateTime.Now;

            if (!sproc)
                dts = gd.Get(Translate(this, false), ConnectionString);
            else
            {
                // The last item will be the connection string for connecting to the database
                List<SqlParameter> parms = new();
                this.GetType()
                    .GetProperties()
                    .ForEach(p => {
                        if ((p.GetCustomAttributes(typeof(ODataExceptionAttribute), true).Length == 0) || (p.GetCustomAttributes(typeof(ODataExceptionAttribute), true).Length > 0 && !((ODataExceptionAttribute)p.GetCustomAttributes(typeof(ODataExceptionAttribute), true)[0]).ODataExceptionType.HasFlag(ODataExceptionType.NON_CREATE)))
                        {

                            //if there is any ODataProcessorAttribute loop though and read value base on attr.
                            object value = p.GetValue(this, null);
                            p.GetCustomAttributes<ODataPropertyAttribute>()?.OrderBy(a => a.Order).ForEach(a => { value = a.OnWriteValue(value); });

                            ODataContext.PropertyConverters.Where(c => c.CanConvert(p)).ForEach(c => { 
                                value = c.OnConvertTo(p, value, null); 
                            });

                            parms.Add(gd.ParamMsSql(gd.GetSqlDbType(p.PropertyType), value, "@" + p.Name)); 
                        
                        }
                    });

                dts = gd.Get("spr_" + typeName, parms.ToArray(), ConnectionString);
            }

            if (dts == null)
                throw new ODataException(error + "No Dataset(s) Returned.");

            if (dts.Tables.Count <= 0)
                throw new ODataException(error + "No Dataset Table(s) Returned.");

            if (dts.Tables[0].Rows.Count <= 0)
                throw new ODataException(error + "No Dataset Table Row(s) Returned.");

            if (Uid <= 0)
                this.Uid = (long)dts.Tables[0].Rows[0][0];

            gd.Clear(dts);

            return this;

        }

        /// <summary>
        /// This removes the object from the database.
        /// </summary>
        public IDataObject Remove(bool sproc = true)
        {
            try
            {
                Updated = DateTime.Now;

                if ((Flags & (long)ODataFlags.Deleted) == (long)ODataFlags.Deleted)
                    return this;

                Flags += (long)ODataFlags.Deleted;

                Save(sproc);

                return this;

            }
            catch
            {
                return this;
            }
        }

        /// <summary>
        /// This removes the object from the database.
        /// </summary>
        public IDataObject PermenentlyRemove(bool sproc = true)
        {
            try
            {
                Updated = DateTime.Now;

                Flags = (long)ODataFlags.DBDelete;

                Save(sproc);

                return this;

            }
            catch
            {
                return this;
            }
        }

        /// <summary>
        /// This returns the object from the database.
        /// </summary>
        public IDataObject Retrieve(long pUid)
        {
            try
            {

                DataSet dts = gd.Get(Select(this, pUid), ConnectionString);

                string error = "K2host.Data." + this.GetType().Name + ": Error getting this object from the database. ";

                if (dts == null)
                    throw new ODataException(error + "No Dataset(s) Returned.");

                if (dts.Tables.Count <= 0)
                    throw new ODataException(error + "No Dataset Table(s) Returned.");

                if (dts.Tables[0].Rows.Count <= 0)
                    throw new ODataException(error + "No Dataset Table Row(s) Returned.");

                dts.Tables[0].Columns.Each(c => {
                    
                    PropertyInfo p = this.GetType()
                        .GetProperty(c.Caption)
                        .DeclaringType
                        .GetProperty(c.Caption, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
                    
                    //if there is any ODataProcessorAttribute loop though and read value base on attr.
                    object value = dts.Tables[0].Rows[0][c.Caption];
                    p.GetCustomAttributes<ODataPropertyAttribute>()?.OrderBy(a => a.Order).ForEach(a => { value = a.OnReadValue(value); });

                    p.SetValue(this, value, null);

                });

                gd.Clear(dts);

                return this;

            }
            catch
            {
                return this;
            }
        }

        /// <summary>
        /// Returns and array of ODataFieldSet based on this instance properties and values.
        /// </summary>
        /// <returns></returns>
        public ODataFieldSet[] GetFieldSets(ODataFieldSetType e)
        {
            List<ODataFieldSet> output = new();

            ODataExceptionType NonInteract = e switch
            {
                ODataFieldSetType.SELECT => ODataExceptionType.NON_SELECT,
                ODataFieldSetType.INSERT => ODataExceptionType.NON_INSERT,
                ODataFieldSetType.UPDATE => ODataExceptionType.NON_UPDATE,
                _ => ODataExceptionType.NON_SELECT,
            };

            this.GetType()
                .GetProperties()
                .ForEach(p => {
                    if ((p.GetCustomAttributes(typeof(ODataExceptionAttribute), true).Length == 0) || (p.GetCustomAttributes(typeof(ODataExceptionAttribute), true).Length > 0 && !((ODataExceptionAttribute)p.GetCustomAttributes(typeof(ODataExceptionAttribute), true)[0]).ODataExceptionType.HasFlag(NonInteract)))
                        output.Add(
                            new ODataFieldSet()
                            {
                                Column      = p,
                                NewValue    = gd.GetSqlRepresentation(p, p.GetValue(this)),
                                DataType    = gd.GetSqlDbType(p.PropertyType)
                            }
                        );
                });

            return output.ToArray();

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="identityInsert"></param>
        /// <returns></returns>
        public string Insert(ODataObject<I> obj, bool identityInsert = false)
        {
            return new ODataInsertQuery()
            {
                UseIdentityInsert = identityInsert,
                To = obj.GetType(),
                Fields = obj.GetFieldSets(ODataFieldSetType.INSERT)
            }.ToString() + "; SELECT Uid FROM tbl_" + obj.GetType().Name + " WHERE Uid = SCOPE_IDENTITY();";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public string Update(ODataObject<I> obj)
        {
            return new ODataUpdateQuery()
            {
                From = obj.GetType(),
                Fields = obj.GetFieldSets(ODataFieldSetType.UPDATE),
                Where = new ODataCondition[] {
                    new ODataCondition() {
                        Column      = obj.GetType().GetProperty("Uid"),
                        Operator    = ODataOperator.EQUAL,
                        Values      = new object[] { obj.Uid }
                    }
                }
            }.ToString() + "; SELECT " + obj.Uid.ToString() + " ";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="uid"></param>
        /// <returns></returns>
        public string Delete(ODataObject<I> obj, long uid)
        {
            return new ODataDeleteQuery()
            {
                From = obj.GetType(),
                Where = new ODataCondition[] {
                    new ODataCondition() {
                        Column      = obj.GetType().GetProperty("Uid"),
                        Operator    = ODataOperator.EQUAL,
                        Values      = new object[] { uid }
                    }
                }
            }.ToString() + "; SELECT " + uid.ToString() + ";";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public string Delete(ODataObject<I> obj, ODataCondition[] conditions)
        {
            return new ODataDeleteQuery()
            {
                From = obj.GetType(),
                Where = conditions
            }.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="uid"></param>
        /// <returns></returns>
        public string Select(ODataObject<I> obj, long uid)
        {
            return new ODataSelectQuery()
            {
                Fields = obj.GetFieldSets(ODataFieldSetType.SELECT),
                From = obj.GetType(),
                Where = new ODataCondition[] {
                    new ODataCondition() {
                        Column      = obj.GetType().GetProperty("Uid"),
                        Operator    = ODataOperator.EQUAL,
                        Values      = new object[] { uid }
                    }
                }
            }.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="top"></param>
        /// <param name="order"></param>
        /// <returns></returns>
        public string Select(ODataObject<I> obj, int top, ODataOrder[] order, ODataTakeSkip takeskip)
        {
            return new ODataSelectQuery()
            {
                Top = top,
                Fields = obj.GetFieldSets(ODataFieldSetType.SELECT),
                From = obj.GetType(),
                Order = order,
                TakeSkip = takeskip
            }.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="top"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <param name="type"></param>
        /// <param name="order"></param>
        /// <returns></returns>
        public string Select(ODataObject<I> obj, int top, ODataCondition[] conditions, ODataOrder[] order, ODataTakeSkip takeskip)
        {
            return new ODataSelectQuery()
            {
                Top = top,
                Fields = obj.GetFieldSets(ODataFieldSetType.SELECT),
                From = obj.GetType(),
                Where = conditions,
                Order = order,
                TakeSkip = takeskip
            }.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="top"></param>
        /// <param name="join"></param>
        /// <param name="joinType"></param>
        /// <param name="joinFromField"></param>
        /// <param name="joinToField"></param>
        /// <param name="order"></param>
        /// <returns></returns>
        public string Select(ODataObject<I> obj, int top, ODataJoinSet[] joins, ODataOrder[] order, ODataTakeSkip takeskip)
        {
            return new ODataSelectQuery()
            {
                Top = top,
                Fields = obj.GetFieldSets(ODataFieldSetType.SELECT),
                From = obj.GetType(),
                Joins = joins,
                Order = order,
                TakeSkip = takeskip
            }.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="top"></param>
        /// <param name="join"></param>
        /// <param name="joinType"></param>
        /// <param name="joinFromField"></param>
        /// <param name="joinToField"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <param name="type"></param>
        /// <param name="order"></param>
        /// <returns></returns>
        public string Select(ODataObject<I> obj, int top, ODataJoinSet[] joins, ODataCondition[] conditions, ODataOrder[] order, ODataTakeSkip takeskip)
        {
            return new ODataSelectQuery()
            {
                Top = top,
                Fields = obj.GetFieldSets(ODataFieldSetType.SELECT),
                From = obj.GetType(),
                Joins = joins,
                Where = conditions,
                Order = order,
                TakeSkip = takeskip
            }.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="identityInsert"></param>
        /// <returns></returns>
        public string Translate(ODataObject<I> obj, bool identityInsert = false)
        {
            string result;

            if (obj.Uid > 0)
            {
                if (obj.Flags == (long)ODataFlags.DBDelete)
                    result = Delete(obj, obj.Uid);
                else if (obj.Flags == (long)ODataFlags.DBSelect)
                    result = Select(obj, obj.Uid);
                else
                    result = Update(obj);
            }
            else
                result = Insert(obj, identityInsert);

            return result;
        }

        /// <summary>
        /// To set the Uid manually for non database DB objects
        /// </summary>
        /// <param name="uid"></param>
        protected void SetUidManually(long uid)
        {
            Uid = uid;
        }

        #endregion

        #region Object, Json and XML based Mapping

        /// <summary>
        /// Used to run the INSERT SQL command and builds the query based on the parameters.
        /// </summary>
        /// <param name="pInto">The list fields you are going to input data.</param>
        /// <param name="pSelect">The Instance IDataObject you are using to select against.</param>
        /// <param name="pColumns">The list fields you are select from the IDataObject.</param>
        /// <param name="pClause">The where clause, a list of conditions.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static bool Insert(ODataFieldSet[] pInto, Type pSelect, ODataFieldSet[] pColumns, ODataCondition[] pClause, string pConnectionString, out ODataException pException)
        {

            pException = null;

            try
            {

                return gd.Query(new ODataInsertQuery()
                {
                    To = typeof(I),
                    Fields = pInto,
                    Select = new ODataSelectQuery()
                    {
                        From = pSelect,
                        Fields = pColumns,
                        Where = pClause
                    }
                }.ToString(),
                    pConnectionString
                );

            }
            catch (Exception ex)
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".Insert():", ex);
                return false;
            }
        }

        /// <summary>
        /// Used to run the INSERT SQL command and builds the query based on the parameters.
        /// </summary>
        /// <param name="pInto">The list fields you are going to input data.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static bool Insert(ODataFieldSet[] pInto, string pConnectionString, out ODataException pException)
        {

            pException = null;

            try
            {
                return gd.Query(new ODataInsertQuery()
                {
                    To = typeof(I),
                    Fields = pInto
                }.ToString(),
                    pConnectionString
                );

            }
            catch (Exception ex)
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".Insert():", ex);
                return false;
            }

        }

        /// <summary>
        /// Used to run the INSERT SQL command and builds the query based on the parameters.
        /// </summary>
        /// <param name="pInto">The list fields you are going to input data.</param>
        /// <param name="pValues">The list sets of values that match the fields you are going to input data</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static bool Insert(ODataFieldSet[] pInto, List<ODataFieldSet[]> pValues, string pConnectionString, out ODataException pException)
        {
            pException = null;

            try
            {

                return gd.Query(new ODataInsertQuery()
                {
                    To = typeof(I),
                    Fields = pInto,
                    ValueSets = pValues
                }.ToString(),
                    pConnectionString
                );

            }
            catch (Exception ex)
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".Insert():", ex);
                return false;
            }

        }

        /// <summary>
        /// Used to run the INSERT SQL command and builds the query based on the ODataInsertQuery object.
        /// </summary>
        /// <param name="pQuery"></param>
        /// <param name="pConnectionString"></param>
        /// <param name="pException"></param>
        /// <returns></returns>
        public static bool Insert(ODataInsertQuery pQuery, string pConnectionString, out ODataException pException)
        {
            pException = null;

            try
            {

                return gd.Query(pQuery.ToString(), pConnectionString);

            }
            catch (Exception ex)
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".Insert():", ex);
                return false;
            }

        }

        /// <summary>
        /// Used to run the UPDATE SQL command and builds the query based on the parameters.
        /// </summary>
        /// <param name="pColumns">The list fields with the values you are going to use.</param>
        /// <param name="pWhereClause">The where clause, a list of conditions.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static bool Update(ODataFieldSet[] pColumns, ODataCondition[] pWhereClause, string pConnectionString, out ODataException pException)
        {

            pException = null;

            try
            {
                return gd.Query(new ODataUpdateQuery()
                {
                    From = typeof(I),
                    Fields = pColumns,
                    Where = pWhereClause
                }.ToString(),
                    pConnectionString
                );
            }
            catch (Exception ex)
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".Update():", ex);
                return false;
            }
        }

        /// <summary>
        /// Used to run the UPDATE SQL command and builds the query based on ODataUpdateQuery object.
        /// </summary>
        /// <param name="pQuery"></param>
        /// <param name="pConnectionString"></param>
        /// <param name="pException"></param>
        /// <returns></returns>
        public static bool Update(ODataUpdateQuery pQuery, string pConnectionString, out ODataException pException)
        {

            pException = null;

            try
            {
                return gd.Query(pQuery.ToString(), pConnectionString);
            }
            catch (Exception ex)
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".Update():", ex);
                return false;
            }
        }

        /// <summary>
        /// Used to run the DELETE SQL command and builds the query based on the parameters.
        /// </summary>
        /// <param name="pUid">The Uid of the IDataObject.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static bool Delete(long pUid, string pConnectionString, out ODataException pException)
        {

            pException = null;

            try
            {

                DataSet dts = gd.Get(
                    new ODataDeleteQuery()
                    {
                        From = typeof(I),
                        Where = new ODataCondition[] {
                            new ODataCondition() {
                                Column      = typeof(I).GetProperty("Uid"),
                                Operator    = ODataOperator.EQUAL,
                                Values      = new object[] { pUid }
                            }
                        }
                    }.ToString() + "; SELECT " + pUid.ToString() + ";",
                    pConnectionString
                );

                if (dts == null)
                    throw new Exception("No Dataset(s) Returned.");

                if (dts.Tables.Count <= 0)
                    throw new Exception("No Dataset Table(s) Returned.");

                if (dts.Tables[0].Rows.Count <= 0)
                    throw new Exception("No Dataset Table Row(s) Returned.");

                return true;

            }
            catch (Exception ex)
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".Delete():", ex);
                return false;
            }

        }

        /// <summary>
        /// Used to run the DELETE SQL command and builds the query based on the parameters.
        /// </summary>
        /// <param name="pWhereClause">The where clause, a list of conditions.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static bool Delete(ODataCondition[] pWhereClause, string pConnectionString, out ODataException pException)
        {

            pException = null;

            try
            {

                return gd.Query(
                    new ODataDeleteQuery()
                    {
                        From = typeof(I),
                        Where = pWhereClause
                    }.ToString(),
                    pConnectionString
                );

            }
            catch (Exception ex)
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".Delete():", ex);
                return false;
            }
        }

        /// <summary>
        /// Used to run the DELETE SQL command and builds the query based on the ODataDeleteQuery object.
        /// </summary>
        /// <param name="pQuery"></param>
        /// <param name="pConnectionString"></param>
        /// <param name="pException"></param>
        /// <returns></returns>
        public static bool Delete(ODataDeleteQuery pQuery, string pConnectionString, out ODataException pException)
        {

            pException = null;

            try
            {

                return gd.Query(pQuery.ToString(), pConnectionString);

            }
            catch (Exception ex)
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".Delete():", ex);
                return false;
            }
        }

        /// <summary>
        /// Used to delete an item from the database as hidden using the Deleted flag on any IDataObject.
        /// </summary>
        /// <param name="items">An array of items to remove.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static bool Remove(IDataObject[] items, string pConnectionString, out ODataException pException)
        {
            pException = null;

            if (items[0].GetType().ReflectedType != typeof(I))
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".Remove(): The types do not match.");
                return false;
            }

            try
            {

                return gd.Query(
                    new ODataUpdateQuery()
                    {
                        From = typeof(I),
                        Fields = new ODataFieldSet[] {
                            new ODataFieldSet(){ Column = typeof(I).GetProperty("Updated"), UpdateOperator = ODataUpdateOperator.EQUAL,         NewValue = DateTime.Now },
                            new ODataFieldSet(){ Column = typeof(I).GetProperty("Flags"),   UpdateOperator = ODataUpdateOperator.PLUS_EQUAL,    NewValue = (long)ODataFlags.Deleted }
                        },
                        Where = new ODataCondition[] {
                            new ODataCondition() {
                                Column      = typeof(I).GetProperty("Uid"),
                                Operator    = ODataOperator.IN,
                                Values      = items.Select(i => i.Uid.ToString()).ToArray()
                            }
                        }
                    }.ToString(),
                    pConnectionString
                );

            }
            catch (Exception ex)
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".Remove():", ex);
                return false;
            }
        }

        /// <summary>
        /// Used to delete an item from the database as hidden using the DBDelete flag on any IDataObject.
        /// </summary>
        /// <param name="items">An array of items to permenently remove.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static bool PermenentlyRemove(IDataObject[] items, string pConnectionString, out ODataException pException)
        {
            pException = null;

            if (items[0].GetType().ReflectedType != typeof(I))
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".PermenentlyRemove(): The types do not match.");
                return false;
            }

            try
            {

                return gd.Query(
                    new ODataUpdateQuery()
                    {
                        From = typeof(I),
                        Fields = new ODataFieldSet[] {
                            new ODataFieldSet(){ Column = typeof(I).GetProperty("Updated"), UpdateOperator = ODataUpdateOperator.EQUAL, NewValue = DateTime.Now },
                            new ODataFieldSet(){ Column = typeof(I).GetProperty("Flags"),   UpdateOperator = ODataUpdateOperator.EQUAL, NewValue = (long)ODataFlags.DBDelete }
                        },
                        Where = new ODataCondition[] {
                            new ODataCondition() {
                                Column      = typeof(I).GetProperty("Uid"),
                                Operator    = ODataOperator.IN,
                                Values      = items.Select(i => i.Uid.ToString()).ToArray()
                            }
                        }
                    }.ToString(),
                    pConnectionString
                );

            }
            catch (Exception ex)
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".PermenentlyRemove():", ex);
                return false;
            }
        }

        /// <summary>
        /// Used to run the UPDATE SQL command and sets the Deleted flag on any object
        /// </summary>
        /// <param name="pWhereClause"></param>
        /// <param name="pConnectionString"></param>
        /// <param name="pException"></param>
        /// <returns></returns>
        public static bool Remove(ODataCondition[] pWhereClause, string pConnectionString, out ODataException pException)
        {

            pException = null;

            try
            {
                return gd.Query(
                    new ODataUpdateQuery()
                    {
                        From = typeof(I),
                        Fields = new ODataFieldSet[] {
                            new ODataFieldSet(){ Column = typeof(I).GetProperty("Updated"), UpdateOperator = ODataUpdateOperator.EQUAL,         NewValue = DateTime.Now },
                            new ODataFieldSet(){ Column = typeof(I).GetProperty("Flags"),   UpdateOperator = ODataUpdateOperator.PLUS_EQUAL,    NewValue = (long)ODataFlags.Deleted }
                        },
                        Where = pWhereClause
                    }.ToString(),
                    pConnectionString
                );
            }
            catch (Exception ex)
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".Update():", ex);
                return false;
            }
        }

        /// <summary>
        /// Used to run the UPDATE SQL command and sets the DBDelete flag on any object
        /// </summary>
        /// <param name="pWhereClause"></param>
        /// <param name="pConnectionString"></param>
        /// <param name="pException"></param>
        /// <returns></returns>
        public static bool PermenentlyRemove(ODataCondition[] pWhereClause, string pConnectionString, out ODataException pException)
        {

            pException = null;

            try
            {
                return gd.Query(
                    new ODataUpdateQuery()
                    {
                        From = typeof(I),
                        Fields = new ODataFieldSet[] {
                            new ODataFieldSet(){ Column = typeof(I).GetProperty("Updated"), UpdateOperator = ODataUpdateOperator.EQUAL, NewValue = DateTime.Now },
                            new ODataFieldSet(){ Column = typeof(I).GetProperty("Flags"),   UpdateOperator = ODataUpdateOperator.EQUAL, NewValue = (long)ODataFlags.DBDelete }
                        },
                        Where = pWhereClause
                    }.ToString(),
                    pConnectionString
                );
            }
            catch (Exception ex)
            {
                pException = new ODataException("K2host.Data." + typeof(I).Name + ".Update():", ex);
                return false;
            }
        }

        /// <summary>
        /// Used to return a single item based on the Uid of the IDataObject, with an exception message
        /// </summary>
        /// <param name="pUid">The Uid of the IDataObject.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <param name="ex">Any Error will be pushed out as a default null is returned</param>
        /// <returns>The exception message.</returns>
        public static I Retrieve(long pUid, string pConnectionString, out ODataException ex)
        {

            ex = null;

            DataSet dts = gd.Get(
                new ODataSelectQuery() {
                    Fields = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                    From = typeof(I),
                    Where = new ODataCondition[] {
                        new ODataCondition() {
                            Column      = typeof(I).GetProperty("Uid"),
                            Operator    = ODataOperator.EQUAL,
                            Values      = new object[] { pUid }
                        }
                    }
                }.ToString(),
                pConnectionString
            );

            if (dts == null)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".Retrieve(): No Dataset(s) Returned.");
                return default;
            }

            if (dts.Tables.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".Retrieve(): No Dataset Table(s) Returned.");
                return default;
            }

            if (dts.Tables[0].Rows.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".Retrieve(): No Dataset Table Row(s) Returned.");
                return default;
            }

            I output = Retrieve(dts, pConnectionString);

            gd.Clear(dts);

            return output;

        }

        /// <summary>
        /// Used to return a single item based on where a field is equal to a value.
        /// </summary>
        /// <param name="pConditions">The where clause that will filter the record.</param>
        /// <param name="pSqlOrder">The array of field(s) order.</param>
        /// <param name="pTakeSkip">The paging object for this query, can only be used with an order object.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <param name="ex">Any Error will be pushed out as a default null is returned</param>
        /// <returns></returns>
        public static I Retrieve(ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, string pConnectionString, out ODataException ex)
        {

            ex = null;

            DataSet dts = gd.Get(
                new ODataSelectQuery() {
                    Fields = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                    From = typeof(I),
                    Where = pConditions,
                    Order = pSqlOrder,
                    TakeSkip = pTakeSkip
                }.ToString(),
                pConnectionString
            );

            if (dts == null)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".Retrieve(): No Dataset(s) Returned.");
                return default;
            }

            if (dts.Tables.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".Retrieve(): No Dataset Table(s) Returned.");
                return default;
            }

            if (dts.Tables[0].Rows.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".Retrieve(): No Dataset Table Row(s) Returned.");
                return default;
            }

            I output = Retrieve(dts, pConnectionString);

            gd.Clear(dts);

            return output;

        }

        /// <summary>
        /// Used to return a single item based on where a clause built based on where field name equals the value. and adds any extended columns to the objects extended columns list.
        /// </summary>
        /// <param name="pExtra">The field set that would return extended columns from an object selection.</param>
        /// <param name="pConditions">The array of condition(s) where clause</param>
        /// <param name="pSqlOrder">The array of field(s) order.</param>
        /// <param name="pTakeSkip">The paging object for this query, can only be used with an order object.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <param name="error"></param>
        /// <returns></returns>
        public static I Retrieve(ODataFieldSet[] pExtra, ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, string pConnectionString, out ODataException ex)
        {

            ex = null;

            DataSet dts = gd.Get(
                new ODataSelectQuery() {
                    Fields = typeof(I).GetFieldSets(ODataFieldSetType.SELECT).Concat(pExtra).ToArray(),
                    From = typeof(I),
                    Where = pConditions,
                    Order = pSqlOrder,
                    TakeSkip = pTakeSkip
                }.ToString(),
                pConnectionString
            );

            if (dts == null)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".Retrieve(): No Dataset(s) Returned.");
                return default;
            }

            if (dts.Tables.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".Retrieve(): No Dataset Table(s) Returned.");
                return default;
            }

            if (dts.Tables[0].Rows.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".Retrieve(): No Dataset Table Row(s) Returned.");
                return default;
            }

            I output = Retrieve(dts, pConnectionString);

            gd.Clear(dts);

            return output;

        }

        /// <summary>
        /// Used to return a list of type I based on limiting and / or an order.
        /// </summary>
        /// <param name="pTop">Limits the return of objects based on the top count (X). 0 equals max</param>
        /// <param name="pSqlOrder">The array of field(s) order.</param>
        /// <param name="pTakeSkip">The paging object for this query, can only be used with an order object.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static List<I> List(int pTop, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, string pConnectionString, out ODataException ex)
        {

            ex = null;

            DataSet dts = gd.Get(new ODataSelectQuery()
            {
                Top = pTop,
                Fields = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                From = typeof(I),
                Order = pSqlOrder,
                TakeSkip = pTakeSkip
            }.ToString(),
                pConnectionString
            );

            if (dts == null)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".List(): No Dataset(s) Returned.");
                return default;
            }

            if (dts.Tables.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".List(): No Dataset Table(s) Returned.");
                return default;
            }

            List<I> result = List(dts, pConnectionString);

            gd.Clear(dts);

            return result;

        }

        /// <summary>
        /// Used to return a list of type I based on limiting, where a clause built based on where field name equals the value and / or an order.
        /// </summary>
        /// <param name="pTop">Limits the return of objects based on the top count (X). 0 equals max</param>
        /// <param name="pConditions">The array of condition(s) where clause.</param>
        /// <param name="pSqlOrder">The array of field(s) order.</param>
        /// <param name="pTakeSkip">The paging object for this query, can only be used with an order object.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static List<I> List(int pTop, ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, string pConnectionString, out ODataException ex)
        {

            ex = null;

            DataSet dts = gd.Get(new ODataSelectQuery()
            {
                Top = pTop,
                Fields = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                From = typeof(I),
                Where = pConditions,
                Order = pSqlOrder,
                TakeSkip = pTakeSkip
            }.ToString(),
                pConnectionString
            );

            if (dts == null)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".List(): No Dataset(s) Returned.");
                return default;
            }

            if (dts.Tables.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".List(): No Dataset Table(s) Returned.");
                return default;
            }

            List<I> result = List(dts, pConnectionString);

            gd.Clear(dts);

            return result;

        }

        /// <summary>
        /// Used to return a list of type I based on a list of <see cref="ODataJoinSet"/> to other referenced IDataObject's with a list of <see cref="ODataCondition"/> and / or an order.
        /// </summary>
        /// <param name="pTop">Limits the return of objects based on the top count (X). 0 equals max</param>
        /// <param name="pJoins">The array of joins(s) that link referenced objects</param>
        /// <param name="pConditions">The array of condition(s) where clause</param>
        /// <param name="pSqlOrder">The array of field(s) order.</param>
        /// <param name="pTakeSkip">The paging object for this query, can only be used with an order object.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static List<I> Join(int pTop, ODataJoinSet[] pJoins, ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, string pConnectionString, out ODataException ex)
        {

            ex = null;

            DataSet dts = gd.Get(
                new ODataSelectQuery() {
                    Top = pTop,
                    Fields = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                    From = typeof(I),
                    Joins = pJoins,
                    Where = pConditions,
                    Order = pSqlOrder,
                    TakeSkip = pTakeSkip
                }.ToString(),
                pConnectionString
            );

            if (dts == null)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".Join(): No Dataset(s) Returned.");
                return default;
            }

            if (dts.Tables.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".Join(): No Dataset Table(s) Returned.");
                return default;
            }

            List<I> result = List(dts, pConnectionString);

            gd.Clear(dts);

            return result;

        }

        /// <summary>
        /// Used to run the SELECT SQL command and builds the query based on the ODataSelectQuery object.
        /// </summary>
        /// <param name="Query"></param>
        /// <param name="pConnectionString"></param>
        /// <param name="ex"></param>
        /// <returns></returns>
        public static List<I> Retrieve(ODataSelectQuery Query, string pConnectionString, out ODataException ex)
        {

            ex = null;

            DataSet dts = gd.Get(Query.ToString(), pConnectionString);

            if (dts == null)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".Join(): No Dataset(s) Returned.");
                return default;
            }

            if (dts.Tables.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".Join(): No Dataset Table(s) Returned.");
                return default;
            }

            List<I> result = List(dts, pConnectionString);

            gd.Clear(dts);

            return result;

        }

        /// <summary>
        /// Used to return a single item JSON string based on the Uid of the IDataObject
        /// </summary>
        /// <param name="pUid">The Uid of the IDataObject in the database.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static string RetrieveJSON(long pUid, string pConnectionString, out ODataException ex)
        {

            ex = null;

            var Query = new ODataSelectQuery()
            {
                Fields  = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                From    = typeof(I),
                Where   = new ODataCondition[] {
                    new ODataCondition() {
                        Column      = typeof(I).GetProperty("Uid"),
                        Operator    = ODataOperator.EQUAL,
                        Values      = new object[] { pUid }
                    }
                }
            };

            DataSet dts = gd.Get(
                Query.ToString() + " FOR JSON PATH, ROOT('" + Query.From.GetMappedName() + "')",
                pConnectionString
            );

            if (dts == null)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".RetrieveJSON(): No Dataset(s) Returned.");
                return default;
            }

            if (dts.Tables.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".RetrieveJSON(): No Dataset Table(s) Returned.");
                return default;
            }

            if (dts.Tables[0].Rows.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".RetrieveJSON(): No Dataset Table Row(s) Returned.");
                return default;
            }

            string output = string.Empty;

            foreach (DataRow r in dts.Tables[0].Rows)
                output += r[0].ToString();

            gd.Clear(dts);

            return output;

        }

        /// <summary>
        /// Used to return a single item based on where a field is equal to a value.
        /// </summary>
        /// <param name="pConditions">The where clause that will filter the record.</param>
        /// <param name="pSqlOrder">The array of field(s) order.</param>
        /// <param name="pTakeSkip">The paging object for this query, can only be used with an order object.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <param name="ex">Any Error will be pushed out as a default null is returned</param>
        /// <returns></returns>
        public static string RetrieveJSON(ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, string pConnectionString, out ODataException ex)
        {

            ex = null;

            var Query = new ODataSelectQuery()
            {
                Fields      = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                From        = typeof(I),
                Where       = pConditions,
                Order       = pSqlOrder,
                TakeSkip    = pTakeSkip
            };

            DataSet dts = gd.Get(
                Query.ToString() + " FOR JSON PATH, ROOT('" + Query.From.GetMappedName() + "')",
                pConnectionString
            );

            if (dts == null)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".RetrieveJSON(): No Dataset(s) Returned.");
                return default;
            }

            if (dts.Tables.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".RetrieveJSON(): No Dataset Table(s) Returned.");
                return default;
            }

            if (dts.Tables[0].Rows.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".RetrieveJSON(): No Dataset Table Row(s) Returned.");
                return default;
            }

            string output = string.Empty;

            foreach (DataRow r in dts.Tables[0].Rows)
                output += r[0].ToString();

            gd.Clear(dts);

            return output;

        }

        /// <summary>
        /// Used to return a single item based on where a clause built based on where field name equals the value. and adds any extended columns to the objects extended columns list.
        /// </summary>
        /// <param name="pExtra">The field set that would return extended columns from an object selection.</param>
        /// <param name="pConditions">The array of condition(s) where clause</param>
        /// <param name="pSqlOrder">The array of field(s) order.</param>
        /// <param name="pTakeSkip">The paging object for this query, can only be used with an order object.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <param name="error"></param>
        /// <returns></returns>
        public static string RetrieveJSON(ODataFieldSet[] pExtra, ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, string pConnectionString, out ODataException ex)
        {

            ex = null;

            var Query = new ODataSelectQuery()
            {
                Fields      = typeof(I).GetFieldSets(ODataFieldSetType.SELECT).Concat(pExtra).ToArray(),
                From        = typeof(I),
                Where       = pConditions,
                Order       = pSqlOrder,
                TakeSkip    = pTakeSkip
            };

            DataSet dts = gd.Get(
                Query.ToString() + " FOR JSON PATH, ROOT('" + Query.From.GetMappedName() + "')",
                pConnectionString
            );

            if (dts == null)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".RetrieveJSON(): No Dataset(s) Returned.");
                return default;
            }

            if (dts.Tables.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".RetrieveJSON(): No Dataset Table(s) Returned.");
                return default;
            }

            if (dts.Tables[0].Rows.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".RetrieveJSON(): No Dataset Table Row(s) Returned.");
                return default;
            }

            string output = string.Empty;

            foreach (DataRow r in dts.Tables[0].Rows)
                output += r[0].ToString();

            gd.Clear(dts);

            return output;

        }

        /// <summary>
        /// Used to return a list of type I based on limiting and / or an order.
        /// </summary>
        /// <param name="pTop">Limits the return of objects based on the top count (X). 0 equals max</param>
        /// <param name="pSqlOrder">The array of field(s) order.</param>
        /// <param name="pTakeSkip">The paging object for this query, can only be used with an order object.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static string ListJSON(int pTop, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, string pConnectionString, out ODataException ex)
        {

            ex = null;
           
            var Query = new ODataSelectQuery()
            {
                Top         = pTop,
                Fields      = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                From        = typeof(I),
                Order       = pSqlOrder,
                TakeSkip    = pTakeSkip
            };

            DataSet dts = gd.Get(
                Query.ToString() + " FOR JSON PATH, ROOT('" + Query.From.GetMappedName() + "')",
                pConnectionString
            );

            if (dts == null)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".ListJSON(): No Dataset(s) Returned.");
                return default;
            }

            if (dts.Tables.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".ListJSON(): No Dataset Table(s) Returned.");
                return default;
            }

            string output = string.Empty;

            foreach (DataRow r in dts.Tables[0].Rows)
                output += r[0].ToString();


            gd.Clear(dts);

            return output;

        }

        /// <summary>
        /// Used to return a list of type I based on limiting, where a clause built based on where field name equals the value and / or an order.
        /// </summary>
        /// <param name="pTop">Limits the return of objects based on the top count (X). 0 equals max</param>
        /// <param name="pConditions">The array of condition(s) where clause.</param>
        /// <param name="pSqlOrder">The array of field(s) order.</param>
        /// <param name="pTakeSkip">The paging object for this query, can only be used with an order object.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static string ListJSON(int pTop, ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, string pConnectionString, out ODataException ex)
        {

            ex = null;

            var Query = new ODataSelectQuery()
            {
                Top         = pTop,
                Fields      = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                From        = typeof(I),
                Where       = pConditions,
                Order       = pSqlOrder,
                TakeSkip    = pTakeSkip
            };

            DataSet dts = gd.Get(
                Query.ToString() + " FOR JSON PATH, ROOT('" + Query.From.GetMappedName() + "')",
                pConnectionString
            );

            if (dts == null)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".ListJSON(): No Dataset(s) Returned.");
                return default;
            }

            if (dts.Tables.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".ListJSON(): No Dataset Table(s) Returned.");
                return default;
            }

            string output = string.Empty;

            foreach (DataRow r in dts.Tables[0].Rows)
                output += r[0].ToString();

            gd.Clear(dts);

            return output;

        }

        /// <summary>
        /// Used to return a list of type I based on a list of <see cref="ODataJoinSet"/> to other referenced IDataObject's with a list of <see cref="ODataCondition"/> and / or an order.
        /// </summary>
        /// <param name="pTop">Limits the return of objects based on the top count (X). 0 equals max</param>
        /// <param name="pJoins">The array of joins(s) that link referenced objects</param>
        /// <param name="pConditions">The array of condition(s) where clause</param>
        /// <param name="pSqlOrder">The array of field(s) order.</param>
        /// <param name="pTakeSkip">The paging object for this query, can only be used with an order object.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static string JoinJSON(int pTop, ODataJoinSet[] pJoins, ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, string pConnectionString, out ODataException ex)
        {

            ex = null;

            var Query = new ODataSelectQuery() {
                Top         = pTop,
                Fields      = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                From        = typeof(I),
                Joins       = pJoins,
                Where       = pConditions,
                Order       = pSqlOrder,
                TakeSkip    = pTakeSkip
            };

            DataSet dts = gd.Get(
                Query.ToString() + " FOR JSON PATH, ROOT('" + Query.From.GetMappedName() + "')",
                pConnectionString
            );

            if (dts == null)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".JoinJSON(): No Dataset(s) Returned.");
                return default;
            }

            if (dts.Tables.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".JoinJSON(): No Dataset Table(s) Returned.");
                return default;
            }

            string output = string.Empty;

            foreach (DataRow r in dts.Tables[0].Rows)
                output += r[0].ToString();

            gd.Clear(dts);

            return output;

        }

        /// <summary>
        /// Used to run the SELECT SQL command and builds the query based on the ODataSelectQuery object.
        /// </summary>
        /// <param name="Query"></param>
        /// <param name="pConnectionString"></param>
        /// <param name="ex"></param>
        /// <returns></returns>
        public static string RetrieveForPath(ODataSelectQuery Query, string pConnectionString, bool enableTotalCount, out long totalCount, out ODataException ex)
        {

            ex = null;
            totalCount = 0;

            string suffix = string.Empty;

            if (enableTotalCount)
                suffix += "; SELECT COUNT(*) FROM tbl_" + Query.From.GetMappedName() + ";";

            DataSet dts = gd.Get(Query.ToString() + suffix, pConnectionString);

            if (dts == null)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".RetrieveJSON(): No Dataset(s) Returned.");
                return default;
            }

            if (dts.Tables.Count <= 0)
            {
                ex = new ODataException("K2host.Data." + typeof(I).Name + ".RetrieveJSON(): No Dataset Table(s) Returned.");
                return default;
            }

            if (enableTotalCount)
                totalCount = Convert.ToInt64(dts.Tables[1].Rows[0][0]);

            string output = string.Empty;

            foreach (DataRow r in dts.Tables[0].Rows)
                output += r[0].ToString();

            gd.Clear(dts);

            return output;

        }

        /// <summary>
        /// This is used to completly remove and the recreate the table and stored procedure of type I in the database.
        /// </summary>
        /// <param name="pDatabaseConnection">The connection object instance that will create the connectionstring.</param>
        public static void ReBuildMigration(OConnection pDatabaseConnection)
        {

            RemoveMigration(pDatabaseConnection);

            BuildMigration(pDatabaseConnection);

        }

        /// <summary>
        /// Used to create the table and the stored procedure of type I in the database.  
        /// </summary>
        /// <param name="pDatabaseConnection">The connection object instance that will create the connectionstring.</param>
        public static void BuildMigration(OConnection pDatabaseConnection)
        {

            gd.Query(
                CreateDatabaseTable(typeof(I), pDatabaseConnection.Database),
                pDatabaseConnection.ToString(
                    OConnectionType.SQLStandardSecurity
                )
            );

            gd.Query(
                CreateDatabaseStoredProc(typeof(I)),
                pDatabaseConnection.ToString(
                    OConnectionType.SQLStandardSecurity
                )
            );

        }

        /// <summary>
        /// Used to alter table columns based on the object type reference.  
        /// </summary>
        /// <param name="pDatabaseConnection">The connection object instance that will create the connectionstring.</param>
        public static void MergeMigration(OConnection pDatabaseConnection)
        {

            List<string> ignore = new() { "Uid", "ParentId", "ParentType", "Flags", "Updated", "Datestamp" };

            Type obj = typeof(I);
            StringBuilder output = new();
            
            var tnm = obj.GetTypeInfo().GetMappedName();

            output.Append("IF (OBJECT_ID (N'tbl_" + tnm + "', N'U') IS NOT NULL)" + Environment.NewLine);
            output.Append("BEGIN" + Environment.NewLine);
            output.Append("    IF ((SELECT COUNT([Uid]) FROM [dbo].[tbl_" + tnm + "]) > 0)" + Environment.NewLine);
            output.Append("    BEGIN" + Environment.NewLine);
            output.Append("        DECLARE @a VARCHAR(255)" + Environment.NewLine);
            output.Append("        DECLARE cc CURSOR" + Environment.NewLine);
            output.Append("        FOR" + Environment.NewLine);
            output.Append("        SELECT [name] FROM sys.objects WHERE [type_desc] = 'DEFAULT_CONSTRAINT' AND OBJECT_NAME(parent_object_id) = 'tbl_" + tnm + "'" + Environment.NewLine);
            output.Append("        OPEN cc" + Environment.NewLine);
            output.Append("        FETCH NEXT FROM cc INTO @a" + Environment.NewLine);
            output.Append("        WHILE @@FETCH_STATUS = 0" + Environment.NewLine);
            output.Append("            BEGIN" + Environment.NewLine);
            output.Append("                DECLARE @b VARCHAR(255) = 'sp_rename N''' + @a + ''', N''' + @a + '_OLD'', N''OBJECT'''" + Environment.NewLine);

            //Used to revert but not used here.
            //output.Append("                DECLARE @b VARCHAR(255) = 'sp_rename N''' + @a + ''', N''' + REPLACE(@a, '_OLD', '') + ''', N''OBJECT'''" + Environment.NewLine);

            output.Append("                EXEC(@b)" + Environment.NewLine);
            output.Append("                FETCH NEXT FROM cc INTO @a" + Environment.NewLine);
            output.Append("            END" + Environment.NewLine);
            output.Append("        CLOSE cc" + Environment.NewLine);
            output.Append("        DEALLOCATE cc" + Environment.NewLine);
            output.Append("        EXEC sp_rename N'PK_" + tnm + "', N'PK_" + tnm + "_OLD', N'OBJECT';" + Environment.NewLine);
            output.Append("        EXEC sp_rename 'tbl_" + tnm + "', 'old_" + tnm + "'; " + Environment.NewLine);
            output.Append("    END" + Environment.NewLine);
            output.Append("    ELSE" + Environment.NewLine);
            output.Append("    BEGIN" + Environment.NewLine);
            output.Append("        DROP TABLE IF EXISTS tbl_" + tnm + Environment.NewLine);
            output.Append("    END" + Environment.NewLine);
            output.Append("    DROP PROCEDURE IF EXISTS spr_" + tnm + Environment.NewLine);
            output.Append("END" + Environment.NewLine);

            gd.Query(
                output.ToString(),
                pDatabaseConnection.ToString(
                    OConnectionType.SQLStandardSecurity
                )
            );

            BuildMigration(pDatabaseConnection);

            output = new();

            output.Append("IF (OBJECT_ID (N'old_" + tnm + "', N'U') IS NOT NULL)" + Environment.NewLine);
            output.Append("BEGIN" + Environment.NewLine);
            output.Append("    SET IDENTITY_INSERT [tbl_" + tnm + "] ON;" + Environment.NewLine);

            output.Append("    DECLARE @query NVARCHAR(MAX) ='" + Environment.NewLine);

            output.Append("    SELECT" + Environment.NewLine);
            output.Append("    [Uid]," + Environment.NewLine);
            output.Append("    [ParentId]," + Environment.NewLine);
            output.Append("    [ParentType],' + " + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .Each(p => {
                    SqlDbType dbt = gd.GetSqlDbType(p.PropertyType);
                    output.Append("    (CASE WHEN EXISTS (SELECT 1 FROM syscolumns WHERE name = '" + p.Name + "' AND id = OBJECT_ID('old_" + tnm + "')) THEN '[" + p.Name + "],' ELSE '" + gd.GetSqlDefaultValueRepresentation(dbt, true) + " AS [" + p.Name + "],' END) + " + Environment.NewLine);
                    return true;
                });

            output.Append("   '[Flags]," + Environment.NewLine);
            output.Append("    [Updated]," + Environment.NewLine);
            output.Append("    [Datestamp]" + Environment.NewLine);
            output.Append("    FROM [old_" + tnm + "]'" + Environment.NewLine);

            output.Append("    INSERT INTO [tbl_" + tnm + "] (" + Environment.NewLine);
            output.Append("    [Uid]," + Environment.NewLine);
            output.Append("    [ParentId]," + Environment.NewLine);
            output.Append("    [ParentType]," + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .Each(p => {
                    output.Append("    [" + p.Name + "]," + Environment.NewLine);
                    return true;
                });

            output.Append("    [Flags]," + Environment.NewLine);
            output.Append("    [Updated]," + Environment.NewLine);
            output.Append("    [Datestamp]" + Environment.NewLine);

            output.Append("    ) EXEC sp_executesql @query;" + Environment.NewLine);

            output.Append("    SET IDENTITY_INSERT [tbl_" + tnm + "] OFF;" + Environment.NewLine);
            output.Append("END" + Environment.NewLine);
            output.Append("DROP TABLE IF EXISTS [old_" + tnm + "];" + Environment.NewLine);

            gd.Query(
                output.ToString(),
                pDatabaseConnection.ToString(
                    OConnectionType.SQLStandardSecurity
                )
            );

        }

        /// <summary>
        /// This is used to completly remove the table and stored procedure of type I in the database.
        /// </summary>
        /// <param name="pDatabaseConnection">The connection object instance that will create the connectionstring.</param>
        public static void RemoveMigration(OConnection pDatabaseConnection)
        {

            gd.Query(
                DropDatabaseTable(typeof(I)),
                pDatabaseConnection.ToString(
                    OConnectionType.SQLStandardSecurity
                )
            );

            gd.Query(
                DropDatabaseStoredProc(typeof(I)),
                pDatabaseConnection.ToString(
                    OConnectionType.SQLStandardSecurity
                )
            );

        }

        /// <summary>
        /// This is used to completly remove all data from the table of type I in the database..
        /// </summary>
        /// <param name="pDatabaseConnection">The connection object instance that will create the connectionstring.</param>
        public static void EmptyMigrationData(OConnection pDatabaseConnection)
        {

            gd.Query(
                TruncateDatabaseTable(typeof(I)),
                pDatabaseConnection.ToString(
                    OConnectionType.SQLStandardSecurity
                )
            );

        }

        /// <summary>
        /// Returns and array of ODataFieldSet based on this type with no values.
        /// </summary>
        /// <returns></returns>
        public static ODataFieldSet[] GetFields(ODataFieldSetType e)
        {
            List<ODataFieldSet> output = new();

            ODataExceptionType NonInteract = e switch
            {
                ODataFieldSetType.SELECT => ODataExceptionType.NON_SELECT,
                ODataFieldSetType.INSERT => ODataExceptionType.NON_INSERT,
                ODataFieldSetType.UPDATE => ODataExceptionType.NON_UPDATE,
                _ => ODataExceptionType.NON_SELECT,
            };

            typeof(I).GetProperties().ForEach(p => {
                if ((p.GetCustomAttributes(typeof(ODataExceptionAttribute), true).Length == 0) || (p.GetCustomAttributes(typeof(ODataExceptionAttribute), true).Length > 0 && !((ODataExceptionAttribute)p.GetCustomAttributes(typeof(ODataExceptionAttribute), true)[0]).ODataExceptionType.HasFlag(NonInteract)))
                    output.Add(
                        new ODataFieldSet()
                        {
                            Column      = p,
                            DataType    = gd.GetSqlDbType(p.PropertyType)
                        }
                    );
            });

            return output.ToArray();
        }

        #endregion

        #region Data Object Mapping Converters and Tool
        
        /// <summary>
        /// This is used to create the instance of type I and returns it.
        /// </summary>
        /// <param name="e">The data set to which to create the instance of type I.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static I Retrieve(DataSet e, string pConnectionString)
        {
            try
            {

                I result = (I)Activator.CreateInstance(typeof(I), new object[] { pConnectionString });

                e.Tables[0].Columns.Each(c => {

                    try
                    {

                        string  cname       = c.Caption;
                        bool    isPrefixed  = cname.Contains(".");

                        if (isPrefixed)
                            cname = cname.Remove(0, cname.IndexOf(".") + 1);

                        PropertyInfo p = result.GetType()
                            .GetProperty(cname).DeclaringType
                            .GetProperty(cname, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);


                        //if there is any ODataProcessorAttribute loop though and read value base on attr.
                        object value = e.Tables[0].Rows[0][c.Caption];
                        p.GetCustomAttributes<ODataPropertyAttribute>()?.OrderBy(a => a.Order).ForEach(a => { value = a.OnReadValue(value); });

                        //Loop though converters that can convert on this property and set the value.
                        ODataContext.PropertyConverters.Where(c => c.CanConvert(p)).ForEach(c => { value = c.OnConvertFrom(p, value, (IDataObject)result); });

                        //Setup the prop value based on the output of the converters and attributes.
                        p.SetValue(result, value, null);

                    }
                    catch (Exception)
                    {
                        //If the column from the query is not in the DMM then we add it to the models extended columns list.
                        ((IDataObject)result).ExtendedColumns.Add(c.Caption, e.Tables[0].Rows[0][c.Caption]);
                    }

                });

                e.Clear();
                e.Tables.Clear();

                return result;

            }
            catch (Exception ex)
            {
                throw new ODataException("K2host.Data." + typeof(I).Name + ".Retrieve():", ex);
            }
        }

        /// <summary>
        /// This is used to create the instance of type I and returns it.
        /// Includes building the joined objects if from a join sql structure.
        /// </summary>
        /// <param name="r">The data table row to which to create the instance of type I.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static I Retrieve(DataRow r, string pConnectionString)
        {

            try
            {

                I result = (I)Activator.CreateInstance(typeof(I), new object[] { pConnectionString });

                bool isPrefixed = r.Table.Columns[0].Caption.Contains(".");

                if (!isPrefixed)
                {

                    r.Table.Columns.Each(c =>
                    {

                        IEnumerable<PropertyInfo> lookup = result.GetType().GetProperties().Where(p => p.Name == c.Caption);

                        if (lookup.Any())
                        {
                            PropertyInfo p = result.GetType()
                                .GetProperty(c.Caption).DeclaringType
                                .GetProperty(c.Caption, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);

                            //if there is any ODataProcessorAttribute loop though and read value base on attr.
                            object value = r[c.Caption];
                            p.GetCustomAttributes<ODataPropertyAttribute>()?.OrderBy(a => a.Order).ForEach(a => { value = a.OnReadValue(value); });

                            //Loop though converters that can convert on this property and set the value.
                            ODataContext.PropertyConverters.Where(c => c.CanConvert(p)).ForEach(c => { value = c.OnConvertFrom(p, value, (IDataObject)result); });

                            p.SetValue(result, value, null);

                        }
                        else
                        {
                            ((IDataObject)result).ExtendedColumns.Add(c.Caption, r[c.Caption]);
                        }

                    });

                }
                else
                {

                    IEnumerable<DataColumn> tColumns = r.Table.Columns.Cast<DataColumn>();

                    tColumns
                        .Where(c => c.Caption.Remove(c.Caption.IndexOf(".")) == typeof(I).Name)
                        .ForEach(c => {

                            string columnName = c.Caption.Remove(0, c.Caption.IndexOf(".") + 1);

                            PropertyInfo p = result.GetType()
                            .GetProperty(columnName).DeclaringType
                            .GetProperty(columnName, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
                           
                            //if there is any ODataProcessorAttribute loop though and read value base on attr.
                            object value = r[c.Caption];
                            p.GetCustomAttributes<ODataPropertyAttribute>()?.OrderBy(a => a.Order).ForEach(a => { value = a.OnReadValue(value); });

                            //Loop though converters that can convert on this property and set the value.
                            ODataContext.PropertyConverters.Where(c => c.CanConvert(p)).ForEach(c => { value = c.OnConvertFrom(p, value, (IDataObject)result); });

                            p.SetValue(result, value, null);

                        });

                    tColumns
                        .Select(c => c.Caption.Remove(c.Caption.IndexOf(".")))
                        .Distinct()
                        .Where(n => n != typeof(I).Name)
                        .ToArray()
                        .ForEach(type => {

                            IDataObject instance = (IDataObject)Activator.CreateInstance(gl.GetTypeFromDomain(type).FirstOrDefault(), new object[] { pConnectionString });

                            try
                            {

                                tColumns
                                    .Where(c => c.Caption.Remove(c.Caption.IndexOf(".")) == type)
                                    .ForEach(c =>
                                    {
                                        string cname = c.Caption.Remove(0, c.Caption.IndexOf(".") + 1);

                                        PropertyInfo p = instance.GetType()
                                        .GetProperty(cname).DeclaringType
                                        .GetProperty(cname, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
        
                                        //if there is any ODataProcessorAttribute loop though and read value base on attr.
                                        object value = r[c.Caption];
                                        p.GetCustomAttributes<ODataPropertyAttribute>()?.OrderBy(a => a.Order).ForEach(a => { value = a.OnReadValue(value); });

                                        //Loop though converters that can convert on this property and set the value.
                                        ODataContext.PropertyConverters.Where(c => c.CanConvert(p)).ForEach(c => { value = c.OnConvertFrom(p, value, (IDataObject)result); });

                                        p.SetValue(instance, value, null);

                                    });

                                ((IDataObject)result).Joins.Add(instance);

                            }
                            catch (Exception)
                            {
                                instance.Dispose();
                            }

                        });

                    tColumns
                        .Where(c => !c.Caption.Contains("."))
                        .ForEach(c => {
                            ((IDataObject)result).ExtendedColumns.Add(c.Caption, r[c.Caption]);
                        });

                }

                return result;

            }
            catch (Exception ex)
            {
                throw new ODataException("K2host.Data." + typeof(I).Name + ".Retrieve():", ex);
            }

        }

        /// <summary>
        /// This is used to create the instance(s) of type I and returns it.
        /// </summary>
        /// <param name="e">The data set to which to create the instance(s) of type I.</param>
        /// <param name="pConnectionString">The connection string used to connect to the sql instance.</param>
        /// <returns></returns>
        public static List<I> List(DataSet e, string pConnectionString)
        {
            try
            {

                List<I> ret = new();

                e.Tables[0].Rows.Each(r => {
                    ret.Add(Retrieve(r, pConnectionString));
                });

                return ret;
            }
            catch (Exception ex)
            {
                throw new ODataException("K2host.Data." + typeof(I).Name + ".List():", ex);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="DatabaseName"></param>
        /// <returns></returns>
        public static string CreateMigrationVersionTable(string DatabaseName)
        {

            StringBuilder output = new();

            output.Append("USE [" + DatabaseName + "]" + Environment.NewLine);
            output.Append(";SET ANSI_NULLS ON" + Environment.NewLine);
            output.Append(";SET QUOTED_IDENTIFIER ON" + Environment.NewLine);
            output.Append(";CREATE TABLE [dbo].[tbl_MigrationVersion] ([Version] NVARCHAR(255) NULL)");
            output.Append(";INSERT INTO [dbo].[tbl_MigrationVersion] ([Version]) VALUES ('0');");

            return output.ToString();

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="DatabaseName"></param>
        /// <returns></returns>
        public static string CreateDatabaseTable(Type obj, string DatabaseName)
        {

            List<string> ignore = new() { "Uid", "ParentId", "ParentType", "Flags", "Updated", "Datestamp" };

            StringBuilder output = new();

            var tnm = obj.GetTypeInfo().GetMappedName();

            output.Append("USE [" + DatabaseName + "]" + Environment.NewLine);
            output.Append(";SET ANSI_NULLS ON" + Environment.NewLine);
            output.Append(";SET QUOTED_IDENTIFIER ON" + Environment.NewLine);
            output.Append(";CREATE TABLE [dbo].[tbl_" + tnm + "](" + Environment.NewLine);
            output.Append("    [Uid] BIGINT IDENTITY(1,1) NOT NULL," + Environment.NewLine);
            output.Append("    [ParentId] BIGINT NULL," + Environment.NewLine);
            output.Append("    [ParentType] NVARCHAR(255) NULL," + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .Each(p => {
                    output.Append("    [" + p.Name + "] " + ((ODataTypeAttribute)p.GetCustomAttributes(true)[0]).ToString() + " NULL," + Environment.NewLine);
                    return true;
                });

            output.Append("    [Flags] BIGINT NULL," + Environment.NewLine);
            output.Append("    [Updated] DATETIME NULL," + Environment.NewLine);
            output.Append("    [Datestamp] DATETIME NULL," + Environment.NewLine);
            output.Append(" CONSTRAINT [PK_" + tnm + "] PRIMARY KEY CLUSTERED " + Environment.NewLine);
            output.Append("(" + Environment.NewLine);
            output.Append("	[Uid] ASC" + Environment.NewLine);
            output.Append(")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]" + Environment.NewLine);
            output.Append(") ON [PRIMARY] ");

            if (output.ToString().Contains("NVARCHAR(MAX)") || output.ToString().Contains("VARCHAR(MAX)") || output.ToString().Contains("NTEXT") || output.ToString().Contains("TEXT") || output.ToString().Contains("IMAGE") || output.ToString().Contains("VARBINARY(MAX)") || output.ToString().Contains("XML"))
                output.Append("TEXTIMAGE_ON [PRIMARY]");

            output.Append(Environment.NewLine);
            output.Append(";ALTER TABLE [dbo].[tbl_" + tnm + "] ADD  CONSTRAINT [DF_" + tnm + "_ParentId]  DEFAULT ((0)) FOR [ParentId]" + Environment.NewLine);
            output.Append(";ALTER TABLE [dbo].[tbl_" + tnm + "] ADD  CONSTRAINT [DF_" + tnm + "_ParentType]  DEFAULT ('') FOR [ParentType]" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .Each(p => {
                    output.Append(";ALTER TABLE [dbo].[tbl_" + tnm + "] ADD  CONSTRAINT [DF_" + obj.GetTypeInfo().Name + "_" + p.Name + "]  DEFAULT ((" + gd.GetSqlDefaultValueRepresentation(gd.GetSqlDbType(p.PropertyType)) + ")) FOR [" + p.Name + "]" + Environment.NewLine);
                    return true;
                });

            output.Append(";ALTER TABLE [dbo].[tbl_" + tnm + "] ADD  CONSTRAINT [DF_" + tnm + "_Flags]  DEFAULT ((0)) FOR [Flags]" + Environment.NewLine);
            output.Append(";ALTER TABLE [dbo].[tbl_" + tnm + "] ADD  CONSTRAINT [DF_" + tnm + "_Updated]  DEFAULT (getdate()) FOR [Updated]" + Environment.NewLine);
            output.Append(";ALTER TABLE [dbo].[tbl_" + tnm + "] ADD  CONSTRAINT [DF_" + tnm + "_Datestamp]  DEFAULT (getdate()) FOR [Datestamp]" + Environment.NewLine);

            return output.ToString();

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string CreateDatabaseStoredProc(Type obj)
        {

            List<string> ignore = new() { "Uid", "ParentId", "ParentType", "Flags", "Updated", "Datestamp" };

            StringBuilder output = new();

            var tnm = obj.GetTypeInfo().GetMappedName();

            output.Append("CREATE PROCEDURE [dbo].[spr_" + tnm + "]" + Environment.NewLine);
            output.Append(" @Uid BIGINT = 0" + Environment.NewLine);
            output.Append(",@ParentId BIGINT = 0" + Environment.NewLine);
            output.Append(",@ParentType NVARCHAR(255) = ''" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .Each(p => {
                    output.Append(",@" + p.Name + " " + ((ODataTypeAttribute)p.GetCustomAttributes(true)[0]).ToString() + Environment.NewLine);
                    return true;
                });

            output.Append(",@Flags BIGINT = 0" + Environment.NewLine);
            output.Append(",@Updated DATETIME = getdate" + Environment.NewLine);
            output.Append(",@Datestamp DATETIME = getdate" + Environment.NewLine);
            output.Append("AS" + Environment.NewLine);
            output.Append("BEGIN" + Environment.NewLine);
            output.Append("    SET NOCOUNT ON;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("    IF (@Uid > 0) BEGIN" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        IF (@Flags = -2) BEGIN -- delete" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("            DELETE FROM tbl_" + tnm + " WHERE [Uid] = @Uid;" + Environment.NewLine);
            output.Append("            SELECT @Uid;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        END ELSE IF (@Flags = -1) BEGIN -- select" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("            SELECT " + Environment.NewLine);
            output.Append("            [Uid]" + Environment.NewLine);
            output.Append("            ,[ParentId]" + Environment.NewLine);
            output.Append("            ,[ParentType]" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => p.GetCustomAttribute(typeof(ODataTypeAttribute), true) != null && !ignore.Contains(p.Name))
                .ToArray()
                .Each(p => {
                    output.Append("            ,[" + p.Name + "]" + Environment.NewLine);
                    return true;
                });

            output.Append("            ,[Flags]" + Environment.NewLine);
            output.Append("            ,[Updated]" + Environment.NewLine);
            output.Append("            ,[Datestamp]" + Environment.NewLine);
            output.Append("            FROM tbl_" + tnm + " WHERE [Uid] = @Uid;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        END ELSE BEGIN -- update" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("            UPDATE tbl_" + tnm + " SET " + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => (p.GetCustomAttribute<ODataTypeAttribute>(true) != null && !ignore.Contains(p.Name)) && (p.GetCustomAttribute<ODataExceptionAttribute>(true) == null || p.GetCustomAttribute<ODataExceptionAttribute>(true) != null && !p.GetCustomAttribute<ODataExceptionAttribute>(true).ODataExceptionType.HasFlag(ODataExceptionType.NON_UPDATE)))
                .ToArray()
                .Each(p => {
                    output.Append("            [" + p.Name + "] = @" + p.Name + "," + Environment.NewLine);
                    return true;
                });

            output.Append("            [ParentId] = @ParentId," + Environment.NewLine);
            output.Append("            [ParentType] = @ParentType," + Environment.NewLine);
            output.Append("            [Flags] = @Flags," + Environment.NewLine);
            output.Append("            [Updated] = @Updated, " + Environment.NewLine);
            output.Append("            [Datestamp] = @Datestamp " + Environment.NewLine);
            output.Append("            WHERE [Uid] = @Uid;" + Environment.NewLine);
            output.Append("            SELECT @Uid;" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        END" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("    END ELSE BEGIN -- insert" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("        INSERT INTO tbl_" + tnm + " (" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => (p.GetCustomAttribute<ODataTypeAttribute>(true) != null && !ignore.Contains(p.Name)) && (p.GetCustomAttribute<ODataExceptionAttribute>(true) == null || p.GetCustomAttribute<ODataExceptionAttribute>(true) != null && !p.GetCustomAttribute<ODataExceptionAttribute>(true).ODataExceptionType.HasFlag(ODataExceptionType.NON_INSERT)))
                .ToArray()
                .Each(p => {
                    output.Append("            [" + p.Name + "]," + Environment.NewLine);
                    return true;
                });

            output.Append("            [ParentId]," + Environment.NewLine);
            output.Append("            [ParentType]," + Environment.NewLine);
            output.Append("            [Flags]," + Environment.NewLine);
            output.Append("            [Updated]," + Environment.NewLine);
            output.Append("            [Datestamp]" + Environment.NewLine);
            output.Append("        ) VALUES (" + Environment.NewLine);

            obj.GetTypeInfo()
                .GetProperties()
                .Where(p => (p.GetCustomAttribute<ODataTypeAttribute>(true) != null && !ignore.Contains(p.Name)) && (p.GetCustomAttribute<ODataExceptionAttribute>(true) == null || p.GetCustomAttribute<ODataExceptionAttribute>(true) != null && !p.GetCustomAttribute<ODataExceptionAttribute>(true).ODataExceptionType.HasFlag(ODataExceptionType.NON_INSERT)))
                .ToArray()
                .Each(p => {
                    output.Append("            @" + p.Name + "," + Environment.NewLine);
                    return true;
                });

            output.Append("            @ParentId," + Environment.NewLine);
            output.Append("            @ParentType," + Environment.NewLine);
            output.Append("            @Flags," + Environment.NewLine);
            output.Append("            @Updated," + Environment.NewLine);
            output.Append("            @Datestamp" + Environment.NewLine);
            output.Append("        )" + Environment.NewLine);
            output.Append("        SELECT [Uid] FROM tbl_" + tnm + " WHERE [Uid] = SCOPE_IDENTITY();" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("    END" + Environment.NewLine);
            output.Append(Environment.NewLine);
            output.Append("END" + Environment.NewLine);

            return output.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string DropDatabaseTable(Type obj)
        {
            return "DROP TABLE IF EXISTS tbl_" + obj.GetTypeInfo().GetMappedName();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string DropDatabaseStoredProc(Type obj)
        {
            return "DROP PROCEDURE IF EXISTS spr_" + obj.GetTypeInfo().GetMappedName();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string DropDatabaseTable(string typeName)
        {
            return "DROP TABLE IF EXISTS tbl_" + typeName;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string DropDatabaseStoredProc(string typeName)
        {
            return "DROP PROCEDURE IF EXISTS spr_" + typeName;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string TruncateDatabaseTable(Type obj)
        {
            return "TRUNCATE TABLE tbl_" + obj.GetTypeInfo().GetMappedName();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string GetMigrationVersion()
        {
            return "SELECT [Version] FROM [tbl_MigrationVersion];";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string SetMigrationVersion(string version)
        {
            return "UPDATE [tbl_MigrationVersion] SET [Version] = '" + version + "';";
        }

        #endregion

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




                }
            IsDisposed = true;
        }

        #endregion

    }

}