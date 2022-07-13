using System;
using System.Collections.Generic;

namespace DMSModelConfigDbUpdater
{
    internal class PageFamilyColumnInfo
    {
        /// <summary>
        /// Dictionary tracking column names from the source database objects
        /// </summary>
        /// <remarks>
        /// <para>
        /// Keys are table, view, or stored procedure names used to obtain data for the web page
        /// Values are the column names for the data returned by the table, view, or stored procedure
        /// </para>
        /// <para>
        /// For detail report pages, the column names are the user friendly detail report view column names
        /// </para>
        /// <para>
        /// For param report pages backed by a stored procedure, the column names are those of the data table returned by the procedure
        /// </para>
        /// </remarks>
        public Dictionary<string, SortedSet<string>> DatabaseColumnNames { get; }

        /// <summary>
        /// Page family name
        /// </summary>
        public string PageFamily { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="pageFamilyName"></param>
        public PageFamilyColumnInfo(string pageFamilyName)
        {
            PageFamily = pageFamilyName;

            DatabaseColumnNames = new Dictionary<string, SortedSet<string>>(StringComparer.OrdinalIgnoreCase);
        }
    }
}
