using System;
using System.Collections.Generic;

namespace DMSModelConfigDbUpdater
{
    internal class CachedNameContainer
    {
        /// <summary>
        /// Dictionary tracking the database columns referenced by each page family
        /// </summary>
        /// <remarks>
        /// Keys are page family names
        /// Values are the source data objects (table, view, or stored procedure name), and the columns in each data object
        /// </remarks>
        public Dictionary<string, PageFamilyColumnInfo> DatabaseColumnsByPageFamily { get; }

        /// <summary>
        /// Dictionary tracking references to other page families, as defined table external_sources
        /// </summary>
        /// <remarks>
        /// Keys are page family names
        /// Values are an instance of ExternalSourceColumnInfo, which tracks the externally referenced page families and column names
        /// </remarks>
        public Dictionary<string, ExternalSourceColumnInfo> ExternalSourceReferences { get; }

        /// <summary>
        /// Keys are list report helper names, values are usage counts
        /// </summary>
        /// <remarks>
        /// List report helpers are referenced by form field choosers of type list-report.helper
        /// </remarks>
        internal Dictionary<string, int> ListReportHelperUsage { get; }

        /// <summary>
        /// Keys are pick list names, values are usage counts
        /// </summary>
        /// <remarks>
        /// Pick lists are referenced by form field choosers of type picker.append, picker.list, etc.
        /// </remarks>
        internal Dictionary<string, int> PickListChooserUsage { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public CachedNameContainer()
        {
            DatabaseColumnsByPageFamily = new Dictionary<string, PageFamilyColumnInfo>(StringComparer.OrdinalIgnoreCase);

            ExternalSourceReferences = new Dictionary<string, ExternalSourceColumnInfo>(StringComparer.OrdinalIgnoreCase);

            ListReportHelperUsage = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            PickListChooserUsage = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        }
    }
}
