using System;
using System.Collections.Generic;
using System.Linq;

namespace DMSModelConfigDbUpdater
{
    internal class ExternalSourceColumnInfo
    {
        /// <summary>
        /// Dictionary tracking external data sources (other page families)
        /// </summary>
        /// <remarks>
        /// Keys are external page family names
        /// Values are the columns referenced in the external page family (typically detail report columns)
        /// </remarks>
        public Dictionary<string, SortedSet<string>> ExternalSources { get; }

        /// <summary>
        /// Page family name
        /// </summary>
        public string PageFamily { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="pageFamilyName"></param>
        public ExternalSourceColumnInfo(string pageFamilyName)
        {
            PageFamily = pageFamilyName;

            ExternalSources = new Dictionary<string, SortedSet<string>>();
        }

        /// <summary>
        /// Add an externally referenced page family and its columns
        /// </summary>
        /// <param name="sourcePageName"></param>
        /// <param name="sourcePageColumns"></param>
        public void AddExternalSourceColumns(string sourcePageName, SortedSet<string> sourcePageColumns)
        {
            SortedSet<string> cachedColumnNames;

            if (ExternalSources.TryGetValue(sourcePageName, out var existingColumnNames))
            {
                cachedColumnNames = existingColumnNames;
            }
            else
            {
                cachedColumnNames = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
                ExternalSources.Add(sourcePageName, cachedColumnNames);
            }

            foreach (var column in sourcePageColumns.Where(column => !cachedColumnNames.Contains(column)))
            {
                cachedColumnNames.Add(column);
            }
        }
    }
}
