/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Data;

using K2host.Data.Enums;
using K2host.Data.Classes;
using K2host.Data.Interfaces;

using gl = K2host.Core.OHelpers;
using gd = K2host.Data.OHelpers;

namespace K2host.Data.Extentions
{

    public static class ODataTriggerRequestExtention
    {

        /// <summary>
        /// This function returns the type based on the string representation of the action on the trigger.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static OTriggerRequestAction ConvertTriggerRequestAction(string type)
        {
            var result = (type.ToLower()) switch
            {
                "i" => OTriggerRequestAction.INSERT,
                "u" => OTriggerRequestAction.UPDATE,
                "d" => OTriggerRequestAction.DELETE,
                _ => OTriggerRequestAction.INSERT,
            };
            return result;
        }

        /// <summary>
        /// This will function turns this object into a data table for saving to a database if reqired.
        /// </summary>
        /// <returns></returns>
        public static DataTable TriggerRequestToTable(this IDataTriggerRequest e)
        {

            DataTable dt = new("TriggerRequest");

            dt.Columns.AddRange(new DataColumn[] {
                new DataColumn("TriggerId",         typeof(long)),
                new DataColumn("Action",            typeof(int)),
                new DataColumn("Updated",           typeof(string)),
                new DataColumn("DatabaseName",      typeof(string)),
                new DataColumn("Tablename",         typeof(string)),
                new DataColumn("PkColumnName",      typeof(string)),
                new DataColumn("RecordId",          typeof(long)),
                new DataColumn("PluginService",     typeof(string)),
                new DataColumn("PluginAddress",     typeof(string)),
                new DataColumn("AuthenticationKey", typeof(string))
            });

            DataRow dr = dt.NewRow();

            dr.ItemArray = new object[]
            {
                e.TriggerId,
                e.Action,
                gl.ArrayToString(e.Updated),
                e.DatabaseName,
                e.Tablename,
                e.PkColumnName,
                e.RecordId,
                e.ServiceName,
                e.AddressName,
                e.AuthenticationKey
            };

            dt.Rows.Add(dr);

            return dt;

        }

        /// <summary>
        /// This is used to build a request based on the dataset that came from the system trigger on the sql server.
        /// </summary>
        /// <param name="e"></param>
        /// <returns></returns>
        public static IDataTriggerRequest BuildTriggerRequest(DataSet e)
        {

            if (e.Tables[0].Rows.Count <= 0)
            {
                gd.Clear(e);
                return null;
            }

            ODataTriggerRequest r = new()
            {
                TriggerId           = Convert.ToInt64(e.Tables[0].Rows[0]["RecordId"]),
                Action              = ConvertTriggerRequestAction(e.Tables[0].Rows[0]["Action"].ToString()),
                Updated             = gl.SplitInt(e.Tables[0].Rows[0]["Updated"].ToString(), char.Parse(",")),
                DatabaseName        = e.Tables[0].Rows[0]["DatabaseName"].ToString(),
                Tablename           = e.Tables[0].Rows[0]["Tablename"].ToString(),
                PkColumnName        = e.Tables[0].Rows[0]["PkColumnName"].ToString(),
                RecordId            = Convert.ToInt64(e.Tables[0].Rows[0]["Uid"].ToString()),
                ServiceName         = e.Tables[0].Rows[0]["ServiceName"].ToString(),
                AddressName         = e.Tables[0].Rows[0]["AddressName"].ToString(),
                AuthenticationKey   = e.Tables[0].Rows[0]["AuthenticationKey"].ToString(),
            };

            if (r.Data == null)
                r.Data = new DataSet(e.DataSetName);
            else
                r.Data.DataSetName = e.DataSetName;

            r.Data.Tables.Add(e.Tables[1].Copy());

            gd.Clear(e);

            return r;

        }

    }

}
