﻿using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PRISM;
using TableColumnNameMapContainer;

namespace DMSModelConfigDbUpdater
{
    public class ModelConfigDbUpdater : EventNotifier
    {
        // Ignore Spelling: dms

        /// <summary>
        /// Match any character that is not a letter, number, or underscore
        /// </summary>
        private readonly Regex mColumnCharNonStandardMatcher = new Regex("[^a-z0-9_]", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

        private string mCurrentConfigDB;

        private SQLiteConnection mDbConnection;

        private readonly ModelConfigDbUpdaterOptions mOptions;

        /// <summary>
        /// Keys in this dictionary are view names, as read from the tab-delimited text file; names should include the schema and may be quoted
        /// Values are dictionaries tracking renamed columns (keys are the original column name, values are information on the new name)
        /// </summary>
        private readonly Dictionary<string, Dictionary<string, ColumnNameInfo>> mViewColumnNameMap;

        /// <summary>
        /// Keys in this dictionary are view names, without schema and without any quotes
        /// Values are the full name for the view, as tracked by mViewColumnNameMap
        /// </summary>
        private readonly Dictionary<string, string> mViewNameMap;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public ModelConfigDbUpdater(ModelConfigDbUpdaterOptions options)
        {
            mOptions = options;

            mCurrentConfigDB = string.Empty;

            mViewColumnNameMap = new Dictionary<string, Dictionary<string, ColumnNameInfo>> (StringComparer.OrdinalIgnoreCase);

            mViewNameMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Convert the object name to snake_case
        /// </summary>
        /// <param name="objectName"></param>
        private string ConvertNameToSnakeCase(string objectName)
        {
            if (!mAnyLowerMatcher.IsMatch(objectName))
            {
                // objectName contains no lowercase letters; simply change to lowercase and return
                return objectName.ToLower();
            }

            var match = mCamelCaseMatcher.Match(objectName);

            var updatedName = match.Success
                ? mCamelCaseMatcher.Replace(objectName, "${LowerLetter}_${UpperLetter}")
                : objectName;

            return updatedName.ToLower();
        }

        /// <summary>
        /// Get the object name, without the schema
        /// </summary>
        /// <remarks>
        /// Simply looks for the first period and assumes the schema name is before the period and the object name is after it
        /// </remarks>
        /// <param name="objectName"></param>
        private static string GetNameWithoutSchema(string objectName)
        {
            if (string.IsNullOrWhiteSpace(objectName))
                return string.Empty;

            var periodIndex = objectName.IndexOf('.');
            if (periodIndex > 0 && periodIndex < objectName.Length - 1)
                return objectName.Substring(periodIndex + 1);

            return objectName;
        }

        private bool LoadNameMapFiles()
        {
            mViewColumnNameMap.Clear();
            mViewNameMap.Clear();

            var viewColumnMapFile = new FileInfo(mOptions.ViewColumnMapFile);

            if (!viewColumnMapFile.Exists)
            {
                OnErrorEvent("View column map file not found: " + viewColumnMapFile.FullName);
                return false;
            }

            var mapReader = new NameMapReader();
            RegisterEvents(mapReader);

            // In dictionary tableNameMap, keys are the original (source) table names
            // and values are WordReplacer classes that track the new table names and new column names in PostgreSQL

            // In dictionary viewColumnNameMap, keys are new table names
            // and values are a Dictionary of mappings of original column names to new column names in PostgreSQL;
            // names should not have double quotes around them

            // Dictionary tableNameMapSynonyms has original table names to new table names

            var columnMapFileLoaded = LoadViewColumnMapFile(viewColumnMapFile);

            if (!columnMapFileLoaded)
                return false;

            var tableNameMapSynonyms = new Dictionary<string, string>();

            if (string.IsNullOrWhiteSpace(mOptions.TableNameMapFile))
                return true;

            var tableNameMapFile = new FileInfo(mOptions.TableNameMapFile);
            if (!tableNameMapFile.Exists)
            {
                OnErrorEvent("Table name map file not found: " + tableNameMapFile.FullName);
                return false;
            }

            var tableNameMapReader = new TableNameMapContainer.NameMapReader();
            RegisterEvents(tableNameMapReader);

            var tableNameInfo = tableNameMapReader.LoadTableNameMapFile(tableNameMapFile.FullName, true, out var abortProcessing);

            if (abortProcessing)
            {
                return false;
            }

            // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
            foreach (var item in tableNameInfo)
            {
                if (tableNameMapSynonyms.ContainsKey(item.SourceTableName) || string.IsNullOrWhiteSpace(item.TargetTableName))
                    continue;

                tableNameMapSynonyms.Add(item.SourceTableName, item.TargetTableName);

                // Look for known renamed views and add new entries to viewColumnNameMap for any matches
                if (mViewColumnNameMap.TryGetValue(item.TargetTableName, out var renamedColumns))
                {
                    mViewColumnNameMap.Add(item.SourceTableName, renamedColumns);
                }
            }

            return true;
        }

        private bool LoadViewColumnMapFile(FileSystemInfo columnMapFile)
        {
            var linesRead = 0;

            try
            {
                using var reader = new StreamReader(new FileStream(columnMapFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));

                while (!reader.EndOfStream)
                {
                    var dataLine = reader.ReadLine();
                    linesRead++;

                    if (string.IsNullOrWhiteSpace(dataLine))
                        continue;

                    var lineParts = dataLine.Split('\t');

                    if (lineParts.Length < 3)
                        continue;

                    if (linesRead == 1 &&
                        (lineParts[0].Equals("View", StringComparison.OrdinalIgnoreCase) ||
                         lineParts[1].Equals("SourceColumnName", StringComparison.OrdinalIgnoreCase)))
                    {
                        // Header line; skip it
                        continue;
                    }

                    var viewName = lineParts[0];

                    var sourceColumnName = lineParts[1];

                    var newColumnName = lineParts[2];

                    var isColumnAlias = lineParts.Length > 3 && bool.TryParse(lineParts[3], out var isAlias) && isAlias;

                    if (!mViewColumnNameMap.ContainsKey(viewName))
                    {
                        mViewColumnNameMap.Add(viewName, new Dictionary<string, ColumnNameInfo>(StringComparer.OrdinalIgnoreCase));

                        var nameWithoutSchema = PossiblyUnquote(GetNameWithoutSchema(viewName));

                        if (mViewNameMap.ContainsKey(nameWithoutSchema))
                        {
                            OnWarningEvent("The view column map file has views with the same name but different schema; this is not supported");
                            return false;
                        }

                        mViewNameMap.Add(nameWithoutSchema, viewName);
                    }

                    var renamedColumns = mViewColumnNameMap[viewName];

                    if (renamedColumns.ContainsKey(sourceColumnName))
                    {
                        // The view column name map file has duplicate rows; only keep the first occurrence of each column
                        continue;
                    }

                    renamedColumns.Add(sourceColumnName, newColumnName);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Error in LoadViewColumnMapFile, reading line {0}", linesRead), ex);
                return false;
            }
        }


        /// <summary>
        /// If objectName only has letters, numbers, or underscores, remove the double quotes surrounding it
        /// </summary>
        /// <param name="objectName"></param>
        private string PossiblyUnquote(string objectName)
        {
            return mColumnCharNonStandardMatcher.IsMatch(objectName)
                ? objectName
                : objectName.Trim('"');
        }

        /// <summary>
        /// Process the SQLite files in the input directory
        /// </summary>
        /// <returns>True if successful, false if an error</returns>
        public bool ProcessInputDirectory()
        {
            try
            {
                var inputDirectory = new DirectoryInfo(mOptions.InputDirectory);
                if (!inputDirectory.Exists)
                {
                    OnErrorEvent("Input directory not found: " + inputDirectory.FullName);
                    return false;
                }

                // Load the various name map files
                if (!LoadNameMapFiles())
                    return false;

                string searchPattern;
                if (string.IsNullOrWhiteSpace(mOptions.FilenameFilter))
                {
                    searchPattern = "*.db";
                }
                else if (mOptions.FilenameFilter.EndsWith(".db", StringComparison.OrdinalIgnoreCase))
                {
                    searchPattern = mOptions.FilenameFilter;
                }
                else
                {
                    searchPattern = mOptions.FilenameFilter + ".db";
                }

                var filesToProcess = inputDirectory.GetFiles(searchPattern).ToList();

                if (filesToProcess.Count == 0)
                {
                    OnWarningEvent(
                        "Did not find any files matching '{0}' in {1}",
                        searchPattern, PathUtils.CompactPathString(inputDirectory.FullName, 80));
                    return true;
                }

                OnStatusEvent(
                    "Found {0} file{1} matching '{2}' in {3}",
                    filesToProcess.Count,
                    filesToProcess.Count > 1 ? "s" : string.Empty,
                    searchPattern,
                    PathUtils.CompactPathString(inputDirectory.FullName, 80));

                // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
                foreach (var modelConfigDb in filesToProcess)
                {
                    var success = ProcessFile(modelConfigDb);

                    if (!success)
                        return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ProcessInputDirectory", ex);
                return false;
            }
        }

        private bool ProcessFile(FileSystemInfo modelConfigDb)
        {
            try
            {
                var connectionString = "Data Source=" + modelConfigDb.FullName + "; Version=3; DateTimeFormat=Ticks; Read Only=False;";

                // When calling the constructor, optionally set parseViaFramework to true if reading SqLite files located on a network share or in read-only folders
                mDbConnection = new SQLiteConnection(connectionString, true);

                mCurrentConfigDB = modelConfigDb.Name;

                try
                {
                    mDbConnection.Open();
                }
                catch (Exception ex)
                {
                    OnErrorEvent("Error opening SQLite database " + modelConfigDb.Name, ex);
                    return false;
                }

                var generalParamsLoaded = ReadGeneralParams(out var generalParams);

                if (!generalParamsLoaded)
                    return false;

                var formFieldsLoaded = ReadFormFields(out var formFields);

                if (!formFieldsLoaded)
                    return false;
                mCurrentConfigDB = string.Empty;
                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ProcessFile", ex);
                mCurrentConfigDB = string.Empty;
                return false;
            }
        }

        private bool ReadFormFields(out List<FormFieldInfo> formFields)
        {
            formFields = new List<FormFieldInfo>();

            try
            {
                using var dbCommand = mDbConnection.CreateCommand();

                dbCommand.CommandText = "SELECT id, name, label FROM form_fields";

                using var reader = dbCommand.ExecuteReader();

                while (reader.Read())
                {
                    var id = SQLiteUtilities.GetInt32(reader, "id");
                    var name = SQLiteUtilities.GetString(reader, "name");
                    var label = SQLiteUtilities.GetString(reader, "label");

                    formFields.Add(new FormFieldInfo(id, name, label));
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReadFormFields", ex);
                return false;
            }
        }

        private bool ReadGeneralParams(out GeneralParameters generalParams)
        {
            generalParams = new GeneralParameters();

            try
            {
                using var dbCommand = mDbConnection.CreateCommand();

                dbCommand.CommandText = "SELECT name, value FROM general_params";

                using var reader = dbCommand.ExecuteReader();

                while (reader.Read())
                {
                    var paramName = SQLiteUtilities.GetString(reader, "name");
                    var paramValue = SQLiteUtilities.GetString(reader, "value");

                    switch (paramName.ToLower())
                    {
                        case "list_report_data_table":
                            generalParams.ListReportView = paramValue;
                            break;

                        case "list_report_data_cols":
                            generalParams.ListReportDataColumns = paramValue;
                            break;

                        case "list_report_data_sort_col":
                            generalParams.ListReportSortColumn = paramValue;
                            break;

                        case "list_report_sproc":
                            generalParams.ListReportStoredProcedure = paramValue;
                            break;

                        case "detail_report_data_table":
                            generalParams.DetailReportView = paramValue;
                            break;

                        case "detail_report_data_id_col":
                            generalParams.DetailReportDataIdColumn = paramValue;
                            break;

                        case "detail_report_sproc":
                            generalParams.DetailReportStoredProcedure = paramValue;
                            break;

                        case "entry_page_data_table":
                            generalParams.EntryPageView = paramValue;
                            break;

                        case "entry_page_data_id_col":
                            generalParams.EntryPageDataIdColumn = paramValue;
                            break;

                        case "entry_sproc":
                            generalParams.EntryStoredProcedure = paramValue;
                            break;

                        case "my_db_group":
                            generalParams.DatabaseGroup = paramValue;
                            break;

                        case "operations_sproc":
                            generalParams.OperationsStoredProcedure = paramValue;
                            break;

                        case "post_submission_detail_id":
                            generalParams.PostSubmissionDetailId = paramValue;
                            break;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReadGeneralParams", ex);
                return false;
            }
        }

        /// <summary>
        /// If the object name begins and ends with double quotes, remove them
        /// </summary>
        /// <param name="objectName"></param>
        private static string TrimQuotes(string objectName)
        {
            if (objectName.StartsWith("\"") && objectName.EndsWith("\""))
            {
                return objectName.Substring(1, objectName.Length - 2);
            }

            return objectName;
        }
    }
}
