/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Data.Common;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using MySql.Data.MySqlClient;

using K2host.Core;
using K2host.Data.Enums;
using K2host.Data.Interfaces;
using K2host.Data.Attributes;
using K2host.Data.Extentions.ODataConnection;

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
        where I : class, IDataObject
    {

        #region Properties

        /// <summary>
        /// The record auto id of this type in its own database table.
        /// </summary>
        [Key]
        [ODataType(SqlDbType.BigInt, MySqlDbType.Int64, OracleDbType.Int64)]
        [ODataException(ODataExceptionType.NON_INSERT | ODataExceptionType.NON_UPDATE)]
        public long Uid { get; set; } = -1;

        /// <summary>
        /// The parent Id of the given type maps to a record in a table in the database. 
        /// </summary>
        [ODataType(SqlDbType.BigInt, MySqlDbType.Int64, OracleDbType.Int64)]
        public long ParentId { get; set; } = 0;

        /// <summary>
        /// The type name as a string which maps to a table in the database.
        /// </summary>
        [ODataType(SqlDbType.NVarChar, MySqlDbType.VarString, OracleDbType.Varchar2, 255)]
        public string ParentType { get; set; } = string.Empty;

        /// <summary>
        /// The record flags if any.
        /// </summary>
        [ODataType(SqlDbType.BigInt, MySqlDbType.Int64, OracleDbType.Int64)]
        public long Flags { get; set; } = 0;

        /// <summary>
        /// The date and time of the recored being updated.
        /// </summary>
        [ODataType(SqlDbType.DateTime, MySqlDbType.DateTime, OracleDbType.TimeStamp)]
        public DateTime? Updated { get; set; } = DateTime.Now;

        /// <summary>
        /// The date and time of the recored was inserted.
        /// </summary>
        [ODataType(SqlDbType.DateTime, MySqlDbType.DateTime, OracleDbType.TimeStamp)]
        [ODataException(ODataExceptionType.NON_UPDATE)]
        public DateTime? Datestamp { get; set; } = DateTime.Now;

        /// <summary>
        /// This holding space allows to hold information / object data while in use.
        /// </summary>
        [JsonIgnore]
        [NotMapped]
        [ODataException(ODataExceptionType.NON_INSERT | ODataExceptionType.NON_UPDATE | ODataExceptionType.NON_SELECT | ODataExceptionType.NON_DELETE | ODataExceptionType.NON_CREATE)]
        public object Tag { get; set; } = null;

        /// <summary>
        /// This holds the joined objects when using <see cref="ODataJoinSet"/> from sql.
        /// </summary>
        [NotMapped]
        [ODataException(ODataExceptionType.NON_INSERT | ODataExceptionType.NON_UPDATE | ODataExceptionType.NON_SELECT | ODataExceptionType.NON_DELETE | ODataExceptionType.NON_CREATE)]
        public List<IDataObject> Joins { get; set; } = new List<IDataObject>();

        /// <summary>
        /// This holds the extended field values forn cases, static and other manual fields.
        /// </summary>
        [NotMapped]
        [ODataException(ODataExceptionType.NON_INSERT | ODataExceptionType.NON_UPDATE | ODataExceptionType.NON_SELECT | ODataExceptionType.NON_DELETE | ODataExceptionType.NON_CREATE)]
        public Dictionary<string, object> ExtendedColumns { get; set; } = new Dictionary<string, object>();

        #endregion

        #region Constructor

        /// <summary>
        /// This creates the instance of the generic type
        /// </summary>
        public ODataObject()
        {

        }

        #endregion

        #region Methods

        /// <summary>
        /// This saves the object to the database.
        /// </summary>
        public IDataObject Save(bool sproc = true, string mappedName = "")
        {

            if (string.IsNullOrEmpty(mappedName))
                mappedName = this.GetType().GetMappedName();

            string error = "K2host.Data." + mappedName + ": Error getting this object from the database. ";

            DataSet dts;

            //Lets set the date stamp of the save action
            Updated = DateTime.Now;

            if (!sproc)
                dts = ODataContext.Connection().Get(CommandType.Text, Translate(this, out IEnumerable<DbParameter> pta, false), pta.ToArray());
            else
            {
                // The last item will be the connection string for connecting to the database
                List<DbParameter> parms = new();
                this.GetType()
                    .GetProperties()
                    .ForEach(p => {
                        if ((p.GetCustomAttributes(typeof(ODataExceptionAttribute), true).Length == 0) || (p.GetCustomAttributes(typeof(ODataExceptionAttribute), true).Length > 0 && !((ODataExceptionAttribute)p.GetCustomAttributes(typeof(ODataExceptionAttribute), true)[0]).ODataExceptionType.HasFlag(ODataExceptionType.NON_CREATE)))
                        {

                            //if there is any ODataProcessorAttribute loop though and read value base on attr.
                            object value = p.GetValue(this, null);

                            p.GetCustomAttributes<ODataPropertyAttribute>()?
                                .OrderBy(a => a.Order)
                                .ForEach(a => { 
                                    value = a.OnWriteValue(value); 
                                });

                            ODataContext
                                .PropertyConverters()
                                .Where(c => c.CanConvert(p))
                                .ForEach(c => { 
                                    value = c.OnConvertTo(p, value, null); 
                                });

                            parms.Add(ODataContext.Connection().CreateParam(ODataContext.Connection().GetDbDataType(p.PropertyType), value, "@" + p.Name));

                        }
                    });

                dts = ODataContext.Connection().Get(CommandType.StoredProcedure, "spr_" + mappedName, parms.ToArray());
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

                DataSet dts = ODataContext.Connection().Get(CommandType.Text, Select(this, out IEnumerable<DbParameter> pta, pUid), pta.ToArray());

                string error = "K2host.Data." + this.GetType().Name + ": Error getting this object from the database. ";

                if (dts == null)
                    throw new ODataException(error + "No Dataset(s) Returned.");

                if (dts.Tables.Count <= 0)
                    throw new ODataException(error + "No Dataset Table(s) Returned.");

                if (dts.Tables[0].Rows.Count <= 0)
                    throw new ODataException(error + "No Dataset Table Row(s) Returned.");

                dts.Tables[0].Columns.ForEach(c => {
                    
                    PropertyInfo p = this.GetType()
                        .GetProperty(c.Caption)
                        .DeclaringType
                        .GetProperty(c.Caption, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
                    
                    //if there is any ODataProcessorAttribute loop though and read value base on attr.
                    object value = dts.Tables[0].Rows[0][c.Caption];
                    p.GetCustomAttributes<ODataPropertyAttribute>()?
                        .OrderBy(a => a.Order)
                        .ForEach(a => { 
                            value = a.OnReadValue(value); 
                        });

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
        public ODataFieldSet[] GetFieldSets(ODataFieldSetType e, out IEnumerable<DbParameter> parameters)
        {

            parameters = Array.Empty<DbParameter>();

            List<ODataFieldSet> output = new();

            ODataExceptionType NonInteract = e switch
            {
                ODataFieldSetType.SELECT => ODataExceptionType.NON_SELECT,
                ODataFieldSetType.INSERT => ODataExceptionType.NON_INSERT,
                ODataFieldSetType.UPDATE => ODataExceptionType.NON_UPDATE,
                _ => ODataExceptionType.NON_SELECT,
            };

            foreach (var p in this.GetType().GetProperties()) { 
                var attr = p.GetCustomAttribute<ODataExceptionAttribute>();
                if ((attr == null) || (attr != null && !attr.ODataExceptionType.HasFlag(NonInteract))) {
                    output.Add(new ODataFieldSet() {
                        Column      = p,
                        NewValue    = ODataContext.Connection().GetSqlRepresentation(p, p.GetValue(this), out IEnumerable<DbParameter> pta),
                        DataType    = ODataContext.Connection().GetDbDataType(p.PropertyType)
                    });
                    parameters = parameters.Concat(pta);
                }            
            }

            return output.ToArray();

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="parameters"></param>
        /// <param name="identityInsert"></param>
        /// <returns></returns>
        public string Insert(ODataObject<I> obj, out IEnumerable<DbParameter> parameters, bool identityInsert = false)
        {

            parameters = Array.Empty<DbParameter>();

            var query = new ODataInsertQuery()
            {
                UseIdentityInsert   = identityInsert,
                To                  = obj.GetType(),
                Fields              = obj.GetFieldSets(ODataFieldSetType.INSERT, out IEnumerable<DbParameter> pta)
            };

            //Lets build first which will get all the params for the query.
            string output = query.ToString();

            switch(ODataContext.Connection().GetDbEnumType())
            {
                case OConnectionDbType.SqlDbType:
                    output += "; SELECT Uid FROM tbl_" + obj.GetType().Name + " WHERE Uid = SCOPE_IDENTITY();";
                    break;
                case OConnectionDbType.MySqlDbType:
                    output += "; SELECT LAST_INSERT_ID();";
                    break;
                case OConnectionDbType.OracleDbType:
                    output += "; SELECT IDENT_CURRENT('dbo.tbl_" + obj.GetType().Name + "') AS Uid;";
                    break;
            };

            //Lets concat the params from all sources.
            parameters = parameters.Concat(query.Parameters).Concat(pta);

            return output;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public string Update(ODataObject<I> obj, out IEnumerable<DbParameter> parameters)
        {
            parameters = Array.Empty<DbParameter>();

            var query = new ODataUpdateQuery()
            {
                From    = obj.GetType(),
                Fields  = obj.GetFieldSets(ODataFieldSetType.UPDATE, out IEnumerable<DbParameter> pta),
                Where   = new ODataCondition[] {
                    new ODataCondition() {
                        Column      = obj.GetType().GetProperty("Uid"),
                        Operator    = ODataOperator.EQUAL,
                        Values      = new object[] { obj.Uid }
                    }
                }
            };

            //Lets build first which will get all the params for the query.
            string output = query.ToString() + "; SELECT " + obj.Uid.ToString() + "; ";

            //Lets concat the params from all sources.
            parameters = parameters.Concat(query.Parameters).Concat(pta);

            return output;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="parameters"></param>
        /// <param name="uid"></param>
        /// <returns></returns>
        public string Delete(ODataObject<I> obj, out IEnumerable<DbParameter> parameters, long uid)
        {
            parameters = Array.Empty<DbParameter>();

            var query = new ODataDeleteQuery()
            {
                From    = obj.GetType(),
                Where   = new ODataCondition[] {
                    new ODataCondition() {
                        Column      = obj.GetType().GetProperty("Uid"),
                        Operator    = ODataOperator.EQUAL,
                        Values      = new object[] { uid }
                    }
                }
            };

            //Lets build first which will get all the params for the query.
            string output = query.ToString() + "; SELECT " + uid.ToString() + ";";

            //Lets concat the params from all sources.
            parameters = parameters.Concat(query.Parameters);

            return output;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="parameters"></param>
        /// <param name="conditions"></param>
        /// <returns></returns>
        public string Delete(ODataObject<I> obj, out IEnumerable<DbParameter> parameters, ODataCondition[] conditions)
        {

            parameters = Array.Empty<DbParameter>();

            var query = new ODataDeleteQuery()
            {
                From    = obj.GetType(),
                Where   = conditions
            };

            //Lets build first which will get all the params for the query.
            string output = query.ToString();

            //Lets concat the params from all sources.
            parameters = parameters.Concat(query.Parameters);

            return output;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="parameters"></param>
        /// <param name="uid"></param>
        /// <returns></returns>
        public string Select(ODataObject<I> obj, out IEnumerable<DbParameter> parameters, long uid)
        {

            parameters = Array.Empty<DbParameter>();

            var query = new ODataSelectQuery()
            {
                Fields  = obj.GetFieldSets(ODataFieldSetType.SELECT, out IEnumerable<DbParameter> pta),
                From    = obj.GetType(),
                Where   = new ODataCondition[] {
                    new ODataCondition() {
                        Column      = obj.GetType().GetProperty("Uid"),
                        Operator    = ODataOperator.EQUAL,
                        Values      = new object[] { uid }
                    }
                }
            };

            //Lets build first which will get all the params for the query.
            string output = query.ToString();

            //Lets concat the params from all sources.
            parameters = parameters.Concat(query.Parameters).Concat(pta);

            return output;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="parameters"></param>
        /// <param name="top"></param>
        /// <param name="order"></param>
        /// <param name="takeskip"></param>
        /// <returns></returns>
        public string Select(ODataObject<I> obj, out IEnumerable<DbParameter> parameters, int top, ODataOrder[] order, ODataTakeSkip takeskip)
        {
            parameters = Array.Empty<DbParameter>();

            var query = new ODataSelectQuery()
            {
                Top         = top,
                Fields      = obj.GetFieldSets(ODataFieldSetType.SELECT, out IEnumerable<DbParameter> pta),
                From        = obj.GetType(),
                Order       = order,
                TakeSkip    = takeskip
            };

            //Lets build first which will get all the params for the query.
            string output = query.ToString();

            //Lets concat the params from all sources.
            parameters = parameters.Concat(query.Parameters).Concat(pta);

            return output;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="parameters"></param>
        /// <param name="top"></param>
        /// <param name="conditions"></param>
        /// <param name="order"></param>
        /// <param name="takeskip"></param>
        /// <returns></returns>
        public string Select(ODataObject<I> obj, out IEnumerable<DbParameter> parameters, int top, ODataCondition[] conditions, ODataOrder[] order, ODataTakeSkip takeskip)
        {
            parameters = Array.Empty<DbParameter>();

            var query = new ODataSelectQuery()
            {
                Top         = top,
                Fields      = obj.GetFieldSets(ODataFieldSetType.SELECT, out IEnumerable<DbParameter> pta),
                From        = obj.GetType(),
                Where       = conditions,
                Order       = order,
                TakeSkip    = takeskip
            };

            //Lets build first which will get all the params for the query.
            string output = query.ToString();

            //Lets concat the params from all sources.
            parameters = parameters.Concat(query.Parameters).Concat(pta);

            return output;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="parameters"></param>
        /// <param name="top"></param>
        /// <param name="joins"></param>
        /// <param name="order"></param>
        /// <param name="takeskip"></param>
        /// <returns></returns>
        public string Select(ODataObject<I> obj, out IEnumerable<DbParameter> parameters, int top, ODataJoinSet[] joins, ODataOrder[] order, ODataTakeSkip takeskip)
        {
            parameters = Array.Empty<DbParameter>();

            var query = new ODataSelectQuery()
            {
                Top         = top,
                Fields      = obj.GetFieldSets(ODataFieldSetType.SELECT, out IEnumerable<DbParameter> pta),
                From        = obj.GetType(),
                Joins       = joins,
                Order       = order,
                TakeSkip    = takeskip
            };

            //Lets build first which will get all the params for the query.
            string output = query.ToString();

            //Lets concat the params from all sources.
            parameters = parameters.Concat(query.Parameters).Concat(pta);

            return output;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="parameters"></param>
        /// <param name="top"></param>
        /// <param name="joins"></param>
        /// <param name="conditions"></param>
        /// <param name="takeskip"></param>
        /// <param name="order"></param>
        /// <returns></returns>
        public string Select(ODataObject<I> obj, out IEnumerable<DbParameter> parameters, int top, ODataJoinSet[] joins, ODataCondition[] conditions, ODataOrder[] order, ODataTakeSkip takeskip)
        {
            parameters = Array.Empty<DbParameter>();

            var query = new ODataSelectQuery()
            {
                Top         = top,
                Fields      = obj.GetFieldSets(ODataFieldSetType.SELECT, out IEnumerable<DbParameter> pta),
                From        = obj.GetType(),
                Joins       = joins,
                Where       = conditions,
                Order       = order,
                TakeSkip    = takeskip
            };

            //Lets build first which will get all the params for the query.
            string output = query.ToString();

            //Lets concat the params from all sources.
            parameters = parameters.Concat(query.Parameters).Concat(pta);

            return output;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="identityInsert"></param>
        /// <returns></returns>
        public string Translate(ODataObject<I> obj, out IEnumerable<DbParameter> parameters, bool identityInsert = false)
        {

            string result;

            if (obj.Uid > 0)
            {
                if (obj.Flags == (long)ODataFlags.DBDelete)
                {
                    result = Delete(obj, out IEnumerable<DbParameter> pta, obj.Uid);
                    parameters = pta;
                }
                else if (obj.Flags == (long)ODataFlags.DBSelect)
                {
                    result = Select(obj, out IEnumerable<DbParameter> pta, obj.Uid);
                    parameters = pta;
                }
                else
                {
                    result = Update(obj, out IEnumerable<DbParameter> pta);
                    parameters = pta;
                }
            }
            else
            {
                result = Insert(obj, out IEnumerable<DbParameter> pta, identityInsert);
                parameters = pta;
            }

            return result;
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
        /// <returns></returns>
        public static bool Insert(ODataFieldSet[] pInto, Type pSelect, ODataFieldSet[] pColumns, ODataCondition[] pClause, out ODataException pException)
        {

            pException = null;

            try
            {

                var Query = new ODataInsertQuery(){
                    To      = typeof(I),
                    Fields  = pInto,
                    Select  = new ODataSelectQuery()
                    {
                        From    = pSelect,
                        Fields  = pColumns,
                        Where   = pClause
                    }
                };

                return ODataContext.Connection().Query(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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
        /// <returns></returns>
        public static bool Insert(ODataFieldSet[] pInto, out ODataException pException)
        {

            pException = null;

            try
            {

                var Query = new ODataInsertQuery() {
                    To      = typeof(I),
                    Fields  = pInto
                };

                return ODataContext.Connection().Query(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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
        /// <returns></returns>
        public static bool Insert(ODataFieldSet[] pInto, List<ODataFieldSet[]> pValues, IEnumerable<DbParameter> pParameters, out ODataException pException)
        {
            pException = null;

            try
            {
                
                var Query = new ODataInsertQuery() {
                    To          = typeof(I),
                    Fields      = pInto,
                    ValueSets   = pValues, 
                    Parameters  = pParameters
                };

                return ODataContext.Connection().Query(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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
        /// <param name="pException"></param>
        /// <returns></returns>
        public static bool Insert(ODataInsertQuery pQuery, out ODataException pException)
        {
            pException = null;

            try
            {

                return ODataContext.Connection().Query(CommandType.Text, pQuery.ToString(), pQuery.Parameters.ToArray());

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
        /// <returns></returns>
        public static bool Update(ODataFieldSet[] pColumns, ODataCondition[] pWhereClause, out ODataException pException)
        {

            pException = null;

            try
            {


                var Query = new ODataUpdateQuery()
                {
                    From    = typeof(I),
                    Fields  = pColumns,
                    Where   = pWhereClause
                };

                return ODataContext.Connection().Query(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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
        /// <param name="pException"></param>
        /// <returns></returns>
        public static bool Update(ODataUpdateQuery pQuery, out ODataException pException)
        {

            pException = null;

            try
            {
                return ODataContext.Connection().Query(CommandType.Text, pQuery.ToString(), pQuery.Parameters.ToArray());
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
        /// <returns></returns>
        public static bool Delete(long pUid, out ODataException pException)
        {

            pException = null;

            try
            {

                var Query = new ODataDeleteQuery()
                {
                    From    = typeof(I),
                    Where   = new ODataCondition[] {
                        new ODataCondition() {
                            Column      = typeof(I).GetProperty("Uid"),
                            Operator    = ODataOperator.EQUAL,
                            Values      = new object[] { pUid }
                        }
                    }
                };

                DataSet dts = ODataContext.Connection().Get(
                    CommandType.Text,
                    Query.ToString() + "; SELECT " + pUid.ToString() + ";",
                    Query.Parameters.ToArray()
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
        /// <returns></returns>
        public static bool Delete(ODataCondition[] pWhereClause, out ODataException pException)
        {

            pException = null;

            try
            {
               
                var Query = new ODataDeleteQuery()
                {
                    From    = typeof(I),
                    Where   = pWhereClause
                };

                return ODataContext.Connection().Query(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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
        /// <param name="pException"></param>
        /// <returns></returns>
        public static bool Delete(ODataDeleteQuery pQuery, out ODataException pException)
        {

            pException = null;

            try
            {

                return ODataContext.Connection().Query(CommandType.Text, pQuery.ToString(), pQuery.Parameters.ToArray());

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
        /// <returns></returns>
        public static bool Remove(IDataObject[] items, out ODataException pException)
        {
            pException = null;

            if (items[0].GetType() != typeof(I)) {
                if (items[0].GetType().ReflectedType != typeof(I)) {
                    pException = new ODataException("K2host.Data." + typeof(I).Name + ".Remove(): The types do not match.");
                    return false;
                }
            }

            try
            {

                var Query = new ODataUpdateQuery()
                {
                    From    = typeof(I),
                    Fields  = new ODataFieldSet[] {
                        new ODataFieldSet(){ Column = typeof(I).GetProperty("Updated"), UpdateOperator = ODataUpdateOperator.EQUAL,         NewValue = DateTime.Now },
                        new ODataFieldSet(){ Column = typeof(I).GetProperty("Flags"),   UpdateOperator = ODataUpdateOperator.PLUS_EQUAL,    NewValue = (long)ODataFlags.Deleted }
                    },
                    Where   = new ODataCondition[] {
                        new ODataCondition() {
                            Column      = typeof(I).GetProperty("Uid"),
                            Operator    = ODataOperator.IN,
                            Values      = items.Select(i => i.Uid.ToString()).ToArray()
                        }
                    }
                };

                return ODataContext.Connection().Query(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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
        /// <returns></returns>
        public static bool PermenentlyRemove(IDataObject[] items, out ODataException pException)
        {
            pException = null;

            if (items[0].GetType() != typeof(I))
            {
                if (items[0].GetType().ReflectedType != typeof(I))
                {
                    pException = new ODataException("K2host.Data." + typeof(I).Name + ".Remove(): The types do not match.");
                    return false;
                }
            }

            try
            {

                var Query = new ODataUpdateQuery()
                {
                    From    = typeof(I),
                    Fields  = new ODataFieldSet[] {
                        new ODataFieldSet(){ Column = typeof(I).GetProperty("Updated"), UpdateOperator = ODataUpdateOperator.EQUAL, NewValue = DateTime.Now },
                        new ODataFieldSet(){ Column = typeof(I).GetProperty("Flags"),   UpdateOperator = ODataUpdateOperator.EQUAL, NewValue = (long)ODataFlags.DBDelete }
                    },
                    Where   = new ODataCondition[] {
                        new ODataCondition() {
                            Column      = typeof(I).GetProperty("Uid"),
                            Operator    = ODataOperator.IN,
                            Values      = items.Select(i => i.Uid.ToString()).ToArray()
                        }
                    }
                };

                return ODataContext.Connection().Query(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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
        /// <param name="pException"></param>
        /// <returns></returns>
        public static bool Remove(ODataCondition[] pWhereClause, out ODataException pException)
        {

            pException = null;

            try
            {

                var Query = new ODataUpdateQuery()
                {
                    From    = typeof(I),
                    Fields  = new ODataFieldSet[] {
                        new ODataFieldSet(){ Column = typeof(I).GetProperty("Updated"), UpdateOperator = ODataUpdateOperator.EQUAL,         NewValue = DateTime.Now },
                        new ODataFieldSet(){ Column = typeof(I).GetProperty("Flags"),   UpdateOperator = ODataUpdateOperator.PLUS_EQUAL,    NewValue = (long)ODataFlags.Deleted }
                    },
                    Where = pWhereClause
                };

                return ODataContext.Connection().Query(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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
        /// <param name="pException"></param>
        /// <returns></returns>
        public static bool PermenentlyRemove(ODataCondition[] pWhereClause, out ODataException pException)
        {

            pException = null;

            try
            {

                var Query = new ODataUpdateQuery()
                {
                    From    = typeof(I),
                    Fields  = new ODataFieldSet[] {
                        new ODataFieldSet(){ Column = typeof(I).GetProperty("Updated"), UpdateOperator = ODataUpdateOperator.EQUAL, NewValue = DateTime.Now },
                        new ODataFieldSet(){ Column = typeof(I).GetProperty("Flags"),   UpdateOperator = ODataUpdateOperator.EQUAL, NewValue = (long)ODataFlags.DBDelete }
                    },
                    Where = pWhereClause
                };

                return ODataContext.Connection().Query(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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
        /// <param name="ex">Any Error will be pushed out as a default null is returned</param>
        /// <returns>The exception message.</returns>
        public static I Retrieve(long pUid, out ODataException ex)
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

            DataSet dts = ODataContext
                .Connection()
                .Get(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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

            I output = Retrieve(dts);

            gd.Clear(dts);

            return output;

        }

        /// <summary>
        /// Used to return a single item based on where a field is equal to a value.
        /// </summary>
        /// <param name="pConditions">The where clause that will filter the record.</param>
        /// <param name="pSqlOrder">The array of field(s) order.</param>
        /// <param name="pTakeSkip">The paging object for this query, can only be used with an order object.</param>
        /// <param name="ex">Any Error will be pushed out as a default null is returned</param>
        /// <returns></returns>
        public static I Retrieve(ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, out ODataException ex)
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

            DataSet dts = ODataContext
                .Connection()
                .Get(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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

            I output = Retrieve(dts);

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
        /// <param name="error"></param>
        /// <returns></returns>
        public static I Retrieve(ODataFieldSet[] pExtra, ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, out ODataException ex)
        {

            ex = null;

            var Query = new ODataSelectQuery() 
            {
                Fields  = typeof(I).GetFieldSets(ODataFieldSetType.SELECT).Concat(pExtra).ToArray(),
                From    = typeof(I),
                Where   = pConditions,
                Order   = pSqlOrder,
                TakeSkip = pTakeSkip
            };

            DataSet dts = ODataContext
                .Connection()
                .Get(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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

            I output = Retrieve(dts);

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
        public static List<I> List(int pTop, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, out ODataException ex)
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

            DataSet dts = ODataContext
                .Connection()
                .Get(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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

            List<I> result = List(dts);

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
        /// <returns></returns>
        public static List<I> List(int pTop, ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, out ODataException ex)
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

            DataSet dts = ODataContext
                .Connection()
                .Get(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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

            List<I> result = List(dts);

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
        /// <returns></returns>
        public static List<I> Join(int pTop, ODataJoinSet[] pJoins, ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, out ODataException ex)
        {

            ex = null;

            var Query = new ODataSelectQuery() 
            {
                Top         = pTop,
                Fields      = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                From        = typeof(I),
                Joins       = pJoins,
                Where       = pConditions,
                Order       = pSqlOrder,
                TakeSkip    = pTakeSkip
            };

            DataSet dts = ODataContext
                .Connection()
                .Get(
                    CommandType.Text,
                    Query.ToString(),
                    Query.Parameters.ToArray()
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

            List<I> result = List(dts);

            gd.Clear(dts);

            return result;

        }

        /// <summary>
        /// Used to run the SELECT SQL command and builds the query based on the ODataSelectQuery object.
        /// </summary>
        /// <param name="Query"></param>
        /// <param name="ex"></param>
        /// <returns></returns>
        public static List<I> Retrieve(ODataSelectQuery Query, out ODataException ex)
        {

            ex = null;

            DataSet dts = ODataContext.Connection().Get(CommandType.Text, Query.ToString(), Query.Parameters.ToArray());

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

            List<I> result = List(dts);

            gd.Clear(dts);

            return result;

        }

        /// <summary>
        /// Used to return a single item JSON string based on the Uid of the IDataObject
        /// </summary>
        /// <param name="pUid">The Uid of the IDataObject in the database.</param>
        /// <returns></returns>
        public static string RetrieveJSON(long pUid, out ODataException ex)
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
                },
                ForPath = new ODataForPath() { 
                    PathType    = ODataForPathType.JSON,
                    Root        = typeof(I).GetMappedName()
                }
            };

            DataSet dts = ODataContext.Connection().Get(
                CommandType.Text,
                Query.ToString(),
                Query.Parameters.ToArray()
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
        /// <param name="ex">Any Error will be pushed out as a default null is returned</param>
        /// <returns></returns>
        public static string RetrieveJSON(ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, out ODataException ex)
        {

            ex = null;

            var Query = new ODataSelectQuery()
            {
                Fields      = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                From        = typeof(I),
                Where       = pConditions,
                Order       = pSqlOrder,
                TakeSkip    = pTakeSkip,
                ForPath     = new ODataForPath()
                {
                    PathType    = ODataForPathType.JSON,
                    Root        = typeof(I).GetMappedName()
                }
            };

            DataSet dts = ODataContext.Connection().Get(
                CommandType.Text,
                Query.ToString(),
                Query.Parameters.ToArray()
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
        /// <param name="ex"></param>
        /// <returns></returns>
        public static string RetrieveJSON(ODataFieldSet[] pExtra, ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, out ODataException ex)
        {

            ex = null;

            var Query = new ODataSelectQuery()
            {
                Fields      = typeof(I).GetFieldSets(ODataFieldSetType.SELECT).Concat(pExtra).ToArray(),
                From        = typeof(I),
                Where       = pConditions,
                Order       = pSqlOrder,
                TakeSkip    = pTakeSkip,
                ForPath     = new ODataForPath()
                {
                    PathType    = ODataForPathType.JSON,
                    Root        = typeof(I).GetMappedName()
                }
            };

            DataSet dts = ODataContext.Connection().Get(
                CommandType.Text,
                Query.ToString(),
                Query.Parameters.ToArray()
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
        /// <returns></returns>
        public static string ListJSON(int pTop, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, out ODataException ex)
        {

            ex = null;
           
            var Query = new ODataSelectQuery()
            {
                Top         = pTop,
                Fields      = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                From        = typeof(I),
                Order       = pSqlOrder,
                TakeSkip    = pTakeSkip,
                ForPath     = new ODataForPath()
                {
                    PathType    = ODataForPathType.JSON,
                    Root        = typeof(I).GetMappedName()
                }
            };

            DataSet dts = ODataContext.Connection().Get(
                CommandType.Text,
                Query.ToString(),
                Query.Parameters.ToArray()
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
        /// <returns></returns>
        public static string ListJSON(int pTop, ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, out ODataException ex)
        {

            ex = null;

            var Query = new ODataSelectQuery()
            {
                Top         = pTop,
                Fields      = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                From        = typeof(I),
                Where       = pConditions,
                Order       = pSqlOrder,
                TakeSkip    = pTakeSkip,
                ForPath     = new ODataForPath()
                {
                    PathType    = ODataForPathType.JSON,
                    Root        = typeof(I).GetMappedName()
                }
            };

            DataSet dts = ODataContext.Connection().Get(
                CommandType.Text,
                Query.ToString(),
                Query.Parameters.ToArray()
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
        /// <returns></returns>
        public static string JoinJSON(int pTop, ODataJoinSet[] pJoins, ODataCondition[] pConditions, ODataOrder[] pSqlOrder, ODataTakeSkip pTakeSkip, out ODataException ex)
        {

            ex = null;

            var Query = new ODataSelectQuery() {
                Top         = pTop,
                Fields      = typeof(I).GetFieldSets(ODataFieldSetType.SELECT),
                From        = typeof(I),
                Joins       = pJoins,
                Where       = pConditions,
                Order       = pSqlOrder,
                TakeSkip    = pTakeSkip,
                ForPath     = new ODataForPath()
                {
                    PathType    = ODataForPathType.JSON,
                    Root        = typeof(I).GetMappedName()
                }
            };

            DataSet dts = ODataContext.Connection().Get(
                CommandType.Text,
                Query.ToString(),
                Query.Parameters.ToArray()
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
        /// <param name="ex"></param>
        /// <returns></returns>
        public static string RetrieveForPath(ODataSelectQuery Query, bool enableTotalCount, out long totalCount, out ODataException ex)
        {

            ex = null;
            totalCount = 0;

            string suffix = string.Empty;

            if (enableTotalCount)
            {
                ODataSelectQuery countQuery = new() {
                    IncludeApplyFields      = false, 
                    IncludeJoinFields       = false, 
                    UseFieldDefaultAlias    = false, 
                    UseFieldPrefixing       = false,
                    Fields = new ODataFieldSet[] { 
                        new  ODataFieldSet(){ 
                            Function    = ODataFunction.COUNT,
                            Column      = Query.From.GetProperty("Uid")
                        }
                    },
                    From = Query.From
                };
                suffix += "; " + countQuery.ToString() + ";";
                //suffix += "; SELECT COUNT(*) FROM tbl_" + Query.From.GetMappedName() + ";";
            }

            DataSet dts = ODataContext.Connection().Get(CommandType.Text, Query.ToString() + suffix, Query.Parameters.ToArray());

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
        /// Used to run the SELECT SQL command and builds the query based on the ODataSelectQuery object.
        /// </summary>
        /// <param name="Query"></param>
        /// <param name="ex"></param>
        /// <returns></returns>
        public static List<I> RetrieveForTable(ODataSelectQuery Query, bool enableTotalCount, out long totalCount, out ODataException ex)
        {

            ex = null;
            totalCount = 0;

            string suffix = string.Empty;

            if (enableTotalCount)
            {
                ODataSelectQuery countQuery = new()
                {
                    IncludeApplyFields = false,
                    IncludeJoinFields = false,
                    UseFieldDefaultAlias = false,
                    UseFieldPrefixing = false,
                    Fields = new ODataFieldSet[] {
                        new  ODataFieldSet(){
                            Function    = ODataFunction.COUNT,
                            Column      = Query.From.GetProperty("Uid")
                        }
                    },
                    From = Query.From
                };
                suffix += "; " + countQuery.ToString() + ";";
                //suffix += "; SELECT COUNT(*) FROM tbl_" + Query.From.GetMappedName() + ";";
            }

            DataSet dts = ODataContext.Connection().Get(CommandType.Text, Query.ToString() + suffix, Query.Parameters.ToArray());

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

            List<I> result = List(dts);

            gd.Clear(dts);

            return result;

        }

        /// <summary>
        /// This is used to completly remove and the recreate the table and stored procedure of type I in the database.
        /// </summary>
        /// <param name="Connection">The connection object instance that will create the connectionstring.</param>
        public static void ReBuildMigration(IDataConnection Connection)
        {

            RemoveMigration(Connection);

            BuildMigration(Connection);

        }

        /// <summary>
        /// Used to create the table and the stored procedure of type I in the database.  
        /// </summary>
        /// <param name="Connection">The connection object instance that will create the connectionstring.</param>
        public static void BuildMigration(IDataConnection Connection)
        {

            Connection.Query(
                CommandType.Text,
                CreateDatabaseTable(typeof(I), Connection), 
                null
            );

            Connection.Query(
                CommandType.Text,
                CreateDatabaseStoredProc(typeof(I)),
                null
            );

        }

        /// <summary>
        /// Used to alter table columns based on the object type reference.  
        /// </summary>
        /// <param name="Connection">The connection object instance that will create the connectionstring.</param>
        public static void MergeMigration(IDataConnection Connection)
        {
            
            //Lets merge migration backup and / or remove
            ODataContext
                .Connection()
                .GetFactory()
                .MergeMigrationBackupAndRemove(typeof(I), Connection);

            //Lets now build the migration
            BuildMigration(Connection);

            //Lets merge migration restore and / or remove
            ODataContext
                .Connection()
                .GetFactory()
                .MergeMigrationRestoreAndRemove(typeof(I), Connection);
           
        }

        /// <summary>
        /// This is used to completly remove the table and stored procedure of type I in the database.
        /// </summary>
        /// <param name="Connection">The connection object instance that will create the connectionstring.</param>
        public static void RemoveMigration(IDataConnection Connection)
        {

            Connection.Query(
                CommandType.Text,
                DropDatabaseTable(typeof(I)),
                null
            );

            Connection.Query(
                CommandType.Text,
                DropDatabaseStoredProc(typeof(I)),
                null
            );

        }

        /// <summary>
        /// This is used to completly remove all data from the table of type I in the database..
        /// </summary>
        /// <param name="Connection">The connection object instance that will create the connectionstring.</param>
        public static void EmptyMigrationData(IDataConnection Connection)
        {

            Connection.Query(
                CommandType.Text,
                TruncateDatabaseTable(typeof(I)),
                null
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

            typeof(I)
                .GetProperties()
                .ForEach(p => {
                    if ((p.GetCustomAttributes(typeof(ODataExceptionAttribute), true).Length == 0) || (p.GetCustomAttributes(typeof(ODataExceptionAttribute), true).Length > 0 && !((ODataExceptionAttribute)p.GetCustomAttributes(typeof(ODataExceptionAttribute), true)[0]).ODataExceptionType.HasFlag(NonInteract)))
                        output.Add(
                            new ODataFieldSet()
                            {
                                Column      = p,
                                DataType    = ODataContext.Connection().GetDbDataType(p.PropertyType)
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
        public static I Retrieve(DataSet e)
        {
            try
            {

                I result = Activator.CreateInstance<I>();

                e.Tables[0].Columns.ForEach(c => {

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
                        ODataContext.PropertyConverters().Where(c => c.CanConvert(p)).ForEach(c => { value = c.OnConvertFrom(p, value, (IDataObject)result); });

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
        public static I Retrieve(DataRow r)
        {

            try
            {

                I result = Activator.CreateInstance<I>();

                bool isPrefixed = r.Table.Columns.Cast<DataColumn>().Any(c => c.Caption.Contains("."));

                if (!isPrefixed)
                {

                    r.Table.Columns.ForEach(c =>
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
                            ODataContext.PropertyConverters().Where(c => c.CanConvert(p)).ForEach(c => { value = c.OnConvertFrom(p, value, (IDataObject)result); });

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
                        .Where(c => !c.Caption.Contains(".") || c.Caption.Remove(c.Caption.IndexOf(".")) == typeof(I).Name)
                        .ForEach(c => {

                            string columnName = c.Caption.Contains(".") ? c.Caption.Remove(0, c.Caption.IndexOf(".") + 1) : c.Caption;

                            PropertyInfo p = result.GetType()
                            .GetProperty(columnName).DeclaringType
                            .GetProperty(columnName, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
                           
                            //if there is any ODataProcessorAttribute loop though and read value base on attr.
                            object value = r[c.Caption];
                            p.GetCustomAttributes<ODataPropertyAttribute>()?.OrderBy(a => a.Order).ForEach(a => { value = a.OnReadValue(value); });

                            //Loop though converters that can convert on this property and set the value.
                            ODataContext.PropertyConverters().Where(c => c.CanConvert(p)).ForEach(c => { value = c.OnConvertFrom(p, value, (IDataObject)result); });

                            p.SetValue(result, value, null);

                        });

                    tColumns
                        .Where(c => c.Caption.Contains("."))
                        .Select(c => c.Caption.Remove(c.Caption.IndexOf(".")))
                        .Distinct()
                        .Where(n => n != typeof(I).Name)
                        .ToArray()
                        .ForEach(type => {

                            IDataObject instance = (IDataObject)Activator.CreateInstance(gl.GetTypeFromDomain(type).FirstOrDefault());

                            try
                            {

                                tColumns
                                    .Where(c => c.Caption.Contains(".") && c.Caption.Remove(c.Caption.IndexOf(".")) == type)
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
                                        ODataContext.PropertyConverters().Where(c => c.CanConvert(p)).ForEach(c => { value = c.OnConvertFrom(p, value, (IDataObject)result); });

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
                        .Where(c => !c.Caption.Contains(".") && !typeof(I).GetProperties().ToArray().Any(p => p.Name == c.Caption))
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
        public static List<I> List(DataSet e)
        {
            try
            {

                List<I> ret = new();

                e.Tables[0].Rows.ForEach(r => {
                    ret.Add(Retrieve(r));
                });

                return ret;
            }
            catch (Exception ex)
            {
                throw new ODataException("K2host.Data." + typeof(I).Name + ".List():", ex);
            }
        }
       
        /// <summary>
        /// Returns a dbset for running IQueryable expressions on a type.
        /// </summary>
        /// <typeparam name="I"></typeparam>
        /// <returns></returns>
        public static IQueryable<I> Select()
        {
            return ODataContext.Instance.Set<I>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="DatabaseName"></param>
        /// <returns></returns>
        public static string CreateMigrationVersionTable(string DatabaseName)
        {
            return ODataContext
                .Connection()
                .GetFactory()
                .CreateMigrationVersionTable(DatabaseName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="Connection"></param>
        /// <returns></returns>
        public static string CreateDatabaseTable(Type obj, IDataConnection Connection)
        {
            return ODataContext
                .Connection()
                .GetFactory()
                .CreateDatabaseTable(obj, Connection);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string CreateDatabaseStoredProc(Type obj)
        {
            return ODataContext
                .Connection()
                .GetFactory()
                .CreateDatabaseStoredProc(obj);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string DropDatabaseTable(Type obj)
        {
            return ODataContext
                .Connection()
                .GetFactory()
                .DropDatabaseTable(obj);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string DropDatabaseStoredProc(Type obj)
        {
            return ODataContext
                .Connection()
                .GetFactory()
                .DropDatabaseStoredProc(obj);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string DropDatabaseTable(string typeName)
        {
            return ODataContext
                .Connection()
                .GetFactory()
                .DropDatabaseTable(typeName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string DropDatabaseStoredProc(string typeName)
        {
            return ODataContext
                .Connection()
                .GetFactory()
                .DropDatabaseStoredProc(typeName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string TruncateDatabaseTable(Type obj)
        {
            return ODataContext
                .Connection()
                .GetFactory()
                .TruncateDatabaseTable(obj);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string GetMigrationVersion()
        {
            return ODataContext
                .Connection()
                .GetFactory()
                .GetMigrationVersion();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static string SetMigrationVersion(string version)
        {
            return ODataContext
                .Connection()
                .GetFactory()
                .SetMigrationVersion(version);
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