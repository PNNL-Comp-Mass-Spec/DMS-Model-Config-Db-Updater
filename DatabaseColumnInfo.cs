﻿using System;
using System.Collections.Generic;
using PRISM;

namespace DMSModelConfigDbUpdater
{
    internal class DatabaseColumnInfo : EventNotifier
    {
        public string DatabaseName { get; }

        /// <summary>
        /// Keys in this dictionary are schema names
        /// Values are a dictionary where keys are table or view name and values are the column names in the table or view
        /// </summary>
        public Dictionary<string, Dictionary<string, SortedSet<string>>> TableAndViewsBySchema { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="databaseName"></param>
        public DatabaseColumnInfo(string databaseName)
        {
            DatabaseName = databaseName;

            TableAndViewsBySchema = new Dictionary<string, Dictionary<string, SortedSet<string>>>(StringComparer.OrdinalIgnoreCase);
        }

        public void AddTableOrView(string schemaName, string tableOrViewName, SortedSet<string> columnNames)
        {
            if (TableAndViewsBySchema.TryGetValue(schemaName, out var tablesAndViews))
            {
                tablesAndViews.Add(tableOrViewName, columnNames);
                return;
            }

            TableAndViewsBySchema.Add(schemaName, new Dictionary<string, SortedSet<string>>(StringComparer.OrdinalIgnoreCase)
            {
                { tableOrViewName, columnNames }
            });
        }

        public SortedSet<string> GetColumnsForTableOrView(string schemaName, string tableOrViewName)
        {
            // First try to match by schema name and table or view name
            if (TableAndViewsBySchema.TryGetValue(schemaName, out var tablesAndViewsForSchema) &&
                tablesAndViewsForSchema.TryGetValue(tableOrViewName, out var columnNamesExactMatch))
            {
                return columnNamesExactMatch;
            }

            foreach (var schemaItem in TableAndViewsBySchema)
            {
                if (!schemaItem.Value.TryGetValue(tableOrViewName, out var columnNames))
                    continue;

                OnWarningEvent("{0} was not found in schema {1}, but was found in schema {2}",
                    ModelConfigDbValidator.GetTableOrViewDescription(tableOrViewName), schemaName, schemaItem.Key);

                return columnNames;
            }

            // Table or view not found in any schema
            return new SortedSet<string>();
        }
    }
}