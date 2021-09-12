/*
' /====================================================\
'| Developed Tony N. Hyde (www.k2host.co.uk)            |
'| Projected Started: 2019-03-26                        | 
'| Use: General                                         |
' \====================================================/
*/
using System;
using System.Collections.Generic;
using K2host.Data.Classes;
using K2host.Data.Enums;

namespace K2host.Data.Interfaces
{
    /// <summary>
    /// This interface allows the generic object folderable.
    /// </summary>
    public interface IDataObject : IDisposable
    {

        /// <summary>
        /// The record auto id of this type in its own database table.
        /// </summary>
        long Uid { get; }

        /// <summary>
        /// The parent Id of the given type maps to a record in a table in the database. 
        /// </summary>
        long ParentId { get; set; }

        /// <summary>
        /// The type name as a string which maps to a table in the database.
        /// </summary>
        string ParentType { get; set; }

        /// <summary>
        /// The record flags if any.
        /// </summary>
        long Flags { get; set; }

        /// <summary>
        /// The date and time of the recored being updated.
        /// </summary>
        DateTime Updated { get; set; }

        /// <summary>
        /// The date and time of the recored was inserted.
        /// </summary>
        DateTime Datestamp { get; set; }

        /// <summary>
        /// The objects database connection string.
        /// </summary>
        string ConnectionString { get; set; }

        /// <summary>
        /// This holding space allows to hold information / object data while in use.
        /// </summary>
        object Tag { get; set; }

        /// <summary>
        /// This holds the joined objects when using joins from sql.
        /// </summary>
        List<IDataObject> Joins { get; set; }

        /// <summary>
        /// This holds the extended field values forn cases, static and other manual fields.
        /// </summary>
        Dictionary<string, object> ExtendedColumns { get; set; }

        /// <summary>
        /// This saves the object to the database.
        /// </summary>
        IDataObject Save(bool sproc = true, string typeName = "");

        /// <summary>
        /// This removes the object from the database.
        /// </summary>
        IDataObject Remove(bool sproc = true);

        /// <summary>
        /// This removes the object from the database.
        /// </summary>
        IDataObject PermenentlyRemove(bool sproc = true);

        /// <summary>
        /// This returns the object from the database.
        /// </summary>
        IDataObject Retrieve(long pUid);

        /// <summary>
        /// Returns and array of ODataFieldSet based on this instance properties and values.
        /// </summary>
        /// <returns></returns>
        ODataFieldSet[] GetFieldSets(ODataFieldSetType e);

    }

}
