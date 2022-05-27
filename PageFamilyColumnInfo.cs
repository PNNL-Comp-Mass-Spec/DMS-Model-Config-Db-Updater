using System;
using System.Collections.Generic;

namespace DMSModelConfigDbUpdater
{
    internal class PageFamilyColumnInfo
    {
        /// <summary>
        /// Dictionary tracking column names from the source database objects (typically the user friendly detail report names)
        /// </summary>
        /// <remarks>
        /// Keys are table, view, or stored procedure names
        /// Values are the column names in the object
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
