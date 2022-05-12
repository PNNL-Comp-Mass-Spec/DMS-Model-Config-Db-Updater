using System;
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
        // Ignore Spelling: dms, dpkg, mc, ont, sw

        /// <summary>
        /// Match any character that is not a letter, number, or underscore
        /// </summary>
        private readonly Regex mColumnCharNonStandardMatcher = new("[^a-z0-9_]", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

        private string mCurrentConfigDB;

        private SQLiteConnection mDbConnection;

        /// <summary>
        /// This is used to assure that only a single warning is shown for each missing view
        /// </summary>
        private readonly SortedSet<string> mMissingViews = new();

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
        /// Keys in this dictionary are view names, with schema, but without any quotes
        /// Values are the full name for the view, as tracked by mViewColumnNameMap
        /// </summary>
        private readonly Dictionary<string, string> mViewNameMapWithSchema;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public ModelConfigDbUpdater(ModelConfigDbUpdaterOptions options)
        {
            mOptions = options;

            mCurrentConfigDB = string.Empty;

            mViewColumnNameMap = new Dictionary<string, Dictionary<string, ColumnNameInfo>>(StringComparer.OrdinalIgnoreCase);

            mViewNameMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            mViewNameMapWithSchema = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        private bool ColumnRenamed(string viewName, string currentColumnName, out string columnNameToUse)
        {
            if (string.IsNullOrWhiteSpace(currentColumnName))
            {
                columnNameToUse = string.Empty;
                return false;
            }

            if (!TryGetColumnMap(viewName, out var columnMap))
            {
                columnNameToUse = currentColumnName;
                return false;
            }

            if (!columnMap.TryGetValue(currentColumnName, out var columnInfo))
            {
                columnNameToUse = currentColumnName;
                return false;
            }

            columnNameToUse = columnInfo.NewColumnName;

            return !currentColumnName.Equals(columnNameToUse);
        }

        private bool FormFieldRenamed(IReadOnlyDictionary<string, FormFieldInfo> renamedFormFields, string formFieldName, out string newFormFieldName)
        {
            if (!renamedFormFields.TryGetValue(formFieldName, out var formFieldInfo))
            {
                newFormFieldName = string.Empty;
                return false;
            }

            newFormFieldName = formFieldInfo.NewName;
            return true;
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
            mViewNameMapWithSchema.Clear();

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

                        var nameWithoutSchema = ValidateQuotedName(GetNameWithoutSchema(viewName));

                        if (!mViewNameMap.ContainsKey(nameWithoutSchema))
                        {
                            mViewNameMap.Add(nameWithoutSchema, viewName);
                        }

                        var nameWithSchema = viewName.Replace("\"", string.Empty);
                        if (!mViewNameMapWithSchema.ContainsKey(nameWithSchema))
                        {
                            mViewNameMapWithSchema.Add(nameWithSchema, viewName);
                        }
                    }

                    var renamedColumns = mViewColumnNameMap[viewName];

                    if (renamedColumns.ContainsKey(sourceColumnName))
                    {
                        // The view column name map file has duplicate rows; only keep the first occurrence of each column
                        continue;
                    }

                    renamedColumns.Add(sourceColumnName, new ColumnNameInfo(sourceColumnName, newColumnName, isColumnAlias));
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent(string.Format("Error in LoadViewColumnMapFile, reading line {0}", linesRead), ex);
                return false;
            }
        }

        private string PossiblyAddSchema(GeneralParameters generalParams, string objectName)
        {
            if (!mOptions.UsePostgresSchema || objectName.Contains("."))
                return objectName;

            return generalParams.Parameters[GeneralParameters.ParameterType.DatabaseGroup].ToLower() switch
            {
                "package" => "dpkg." + objectName,
                "ontology" => "ont." + objectName,
                "broker" => "sw." + objectName,
                "capture" => "cap." + objectName,
                "manager_control" => "mc." + objectName,
                _ => objectName
            };
        }

        /// <summary>
        /// If objectName contains characters other than A-Z, a-z, 0-9, or an underscore, surround the name with square brackets or double quotes
        /// </summary>
        /// <param name="objectName"></param>
        /// <param name="quoteWithSquareBrackets"></param>
        /// <param name="alwaysQuoteNames"></param>
        protected string PossiblyQuoteName(string objectName, bool quoteWithSquareBrackets = false, bool alwaysQuoteNames = false)
        {
            if (!alwaysQuoteNames &&
                !mColumnCharNonStandardMatcher.Match(objectName).Success)
            {
                return objectName;
            }

            if (quoteWithSquareBrackets)
            {
                // SQL Server quotes names with square brackets
                return '[' + objectName + ']';
            }

            // PostgreSQL quotes names with double quotes
            return '"' + objectName + '"';
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
                    if (mOptions.FilenameFilter.Contains("*"))
                        searchPattern = mOptions.FilenameFilter + ".db";
                    else
                        searchPattern = mOptions.FilenameFilter + "*.db";
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

                if (mOptions.RenameEntryPageViewAndColumns)
                {
                    var entryPageView = RenameEntryPageView(generalParams);

                    // Update form_fields, form_field_choosers, form_field_options, and external_sources
                    UpdateFormFields(formFields, entryPageView);
                }

                if (mOptions.RenameListReportViewAndColumns)
                {
                    var listReportView = RenameListReportView(generalParams);
                    UpdateListReportHotlinks(formFields, listReportView);

                    // Update list_report_primary_filter and primary_filter_choosers
                    UpdateListReportPrimaryFilter(formFields, listReportView);
                }

                if (mOptions.RenameDetailReportViewAndColumns)
                {
                    var detailReportView = RenameDetailReportView(generalParams);
                    UpdateDetailReportHotlinks(formFields, detailReportView);
                }

                if (mOptions.RenameStoredProcedures)
                {
                    RenameStoredProcedures(generalParams);
                }

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

                    // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                    foreach (var item in generalParams.FieldNames)
                    {
                        if (paramName.Equals(item.Value))
                        {
                            generalParams.Parameters[item.Key] = paramValue;
                        }
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

        private string RenameEntryPageView(GeneralParameters generalParams)
        {
            try
            {
                var viewNameToUse = RenameViewOrProcedure(generalParams, GeneralParameters.ParameterType.EntryPageView);
                if (string.IsNullOrWhiteSpace(viewNameToUse))
                    return string.Empty;

                if (!ColumnRenamed(viewNameToUse, generalParams.Parameters[GeneralParameters.ParameterType.EntryPageDataIdColumn], out var columnNameToUse))
                {
                    UpdateGeneralParameter(generalParams, GeneralParameters.ParameterType.EntryPageDataIdColumn, columnNameToUse);
                }

                return viewNameToUse;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in RenameEntryPageView", ex);
                return string.Empty;
            }
        }

        private string RenameDetailReportView(GeneralParameters generalParams)
        {
            try
            {
                var viewNameToUse = RenameViewOrProcedure(generalParams, GeneralParameters.ParameterType.DetailReportView);
                if (string.IsNullOrWhiteSpace(viewNameToUse))
                    return string.Empty;

                if (!ColumnRenamed(viewNameToUse, generalParams.Parameters[GeneralParameters.ParameterType.DetailReportDataIdColumn], out var columnNameToUse))
                {
                    UpdateGeneralParameter(generalParams, GeneralParameters.ParameterType.DetailReportDataIdColumn, columnNameToUse);
                }

                return viewNameToUse;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in RenameDetailReportView", ex);
                return string.Empty;
            }
        }

        private string RenameListReportView(GeneralParameters generalParams)
        {
            try
            {
                var viewNameToUse = RenameViewOrProcedure(generalParams, GeneralParameters.ParameterType.ListReportView);
                if (string.IsNullOrWhiteSpace(viewNameToUse))
                    return string.Empty;

                if (!ColumnRenamed(viewNameToUse, generalParams.Parameters[GeneralParameters.ParameterType.ListReportSortColumn], out var columnNameToUse))
                {
                    UpdateGeneralParameter(generalParams, GeneralParameters.ParameterType.ListReportSortColumn, columnNameToUse);
                }

                if (!string.IsNullOrWhiteSpace(generalParams.Parameters[GeneralParameters.ParameterType.ListReportDataColumns]))
                {
                    UpdateListReportDataColumns(generalParams);
                }

                return viewNameToUse;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in RenameListReportView", ex);
                return string.Empty;
            }
        }

        /// <summary>
        /// Convert stored procedure names to snake case
        /// </summary>
        private void RenameStoredProcedures(GeneralParameters generalParams)
        {
            try
            {
                RenameViewOrProcedure(generalParams, GeneralParameters.ParameterType.ListReportSP);
                RenameViewOrProcedure(generalParams, GeneralParameters.ParameterType.DetailReportSP);
                RenameViewOrProcedure(generalParams, GeneralParameters.ParameterType.EntryPageSP);
                RenameViewOrProcedure(generalParams, GeneralParameters.ParameterType.OperationsSP);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in RenameStoredProcedures", ex);
            }
        }

        /// <summary>
        /// Change a view or stored procedure name to snake case
        /// </summary>
        /// <remarks>Do not put an exception handler in this method</remarks>
        /// <param name="generalParams"></param>
        /// <param name="parameterType"></param>
        /// <returns>The object name to use (either the original if already in the correct format, or the new name)</returns>
        private string RenameViewOrProcedure(GeneralParameters generalParams, GeneralParameters.ParameterType parameterType)
        {
            var currentName = generalParams.Parameters[parameterType];
            var objectDescription = generalParams.FieldDescriptions[parameterType];

            if (string.IsNullOrWhiteSpace(generalParams.Parameters[parameterType]))
                return string.Empty;

            var updatedName = NameUpdater.ConvertNameToSnakeCase(currentName);

            var nameToUse = PossiblyAddSchema(generalParams, updatedName);

            if (currentName.Equals(nameToUse))
            {
                OnStatusEvent("In {0}, {1} is already {2}", mCurrentConfigDB, objectDescription, nameToUse);
                return nameToUse;
            }

            UpdateGeneralParameter(generalParams, parameterType, nameToUse, false);

            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (mOptions.PreviewUpdates)
                OnStatusEvent("In {0}, would change {1} to {2}", mCurrentConfigDB, objectDescription, nameToUse);
            else
                OnStatusEvent("In {0}, changed {1} to {2}", mCurrentConfigDB, objectDescription, nameToUse);

            return nameToUse;
        }

        /// <summary>
        /// If the object name begins and ends with square brackets or double quotes, remove them
        /// </summary>
        /// <param name="objectName"></param>
        private static string TrimQuotes(string objectName)
        {
            if (objectName.StartsWith("[") && objectName.EndsWith("]"))
            {
                return objectName.Substring(1, objectName.Length - 2);
            }

            if (objectName.StartsWith("\"") && objectName.EndsWith("\""))
            {
                return objectName.Substring(1, objectName.Length - 2);
            }

            return objectName;
        }

        private bool TryGetColumnMap(string viewName, out Dictionary<string, ColumnNameInfo> columnMap)
        {
            // ReSharper disable once InvertIf
            if (mViewNameMap.TryGetValue(viewName, out var nameWithSchema))
            {
                columnMap = mViewColumnNameMap[nameWithSchema];
                return true;
            }

            return mViewColumnNameMap.TryGetValue(viewName, out columnMap);
        }

        private void UpdateDetailReportHotlinks(List<FormFieldInfo> formFields, string detailReportView)
        {
            try
            {

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdateDetailReportHotlinks", ex);
            }
        }

        private void UpdateFormFields(List<FormFieldInfo> formFields, string entryPageView)
        {
            try
            {

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdateFormFields", ex);
            }
        }

        private void UpdateGeneralParameter(GeneralParameters generalParams, GeneralParameters.ParameterType parameterType, string newValue, bool reportUpdate = true)
        {
            var currentValue = generalParams.Parameters[parameterType];
            var generalParamsKeyName = generalParams.FieldNames[parameterType];

            if (!string.IsNullOrWhiteSpace(currentValue) && currentValue.Equals(newValue))
            {
                if (reportUpdate)
                {
                    OnStatusEvent("In {0}, {1} is already {2}", mCurrentConfigDB, generalParamsKeyName, newValue);
                }

                return;
            }

            if (mOptions.PreviewUpdates)
            {
                if (reportUpdate)
                {
                    OnStatusEvent("In {0}, would change {1} from {2} to {3}", mCurrentConfigDB, generalParamsKeyName, currentValue ?? "an empty string", newValue);
                }

                return;
            }

            // Update the database
            using var dbCommand = mDbConnection.CreateCommand();

            dbCommand.CommandText = string.Format("UPDATE general_params set value = '{0}' WHERE name = '{1}'", newValue, generalParamsKeyName);

            dbCommand.ExecuteNonQuery();

            // Update the cached value
            generalParams.Parameters[parameterType] = newValue;

            if (reportUpdate)
            {
                OnStatusEvent("In {0}, changed {1} from {2} to {3}", mCurrentConfigDB, generalParamsKeyName, currentValue ?? "an empty string", newValue);
            }
        }

        private void UpdateListReportDataColumns(GeneralParameters generalParams)
        {
            try
            {
                var columnList = generalParams.Parameters[GeneralParameters.ParameterType.ListReportDataColumns].Split(',');

                var updatedColumns = new List<string>();

                foreach (var currentColumn in columnList)
                {
                    var asIndex = currentColumn.IndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
                    string columnNameToFind;
                    string aliasName;

                    if (asIndex > 0)
                    {
                        columnNameToFind = TrimQuotes(currentColumn.Substring(0, asIndex));
                        aliasName = currentColumn.Substring(asIndex);
                    }
                    else
                    {
                        columnNameToFind = TrimQuotes(currentColumn);
                        aliasName = string.Empty;
                    }

                    if (ColumnRenamed(generalParams.Parameters[GeneralParameters.ParameterType.ListReportView], columnNameToFind, out var newColumnName))
                    {
                        if (string.IsNullOrWhiteSpace(aliasName))
                        {
                            updatedColumns.Add(PossiblyQuoteName(newColumnName));
                        }
                        else
                        {
                            var nameWithAlias = PossiblyQuoteName(newColumnName) + aliasName;
                            updatedColumns.Add(nameWithAlias);
                        }
                    }
                    else
                    {
                        updatedColumns.Add(currentColumn);
                    }
                }

                var updatedColumnList = string.Join(", ", updatedColumns);

                UpdateGeneralParameter(generalParams, GeneralParameters.ParameterType.ListReportDataColumns, updatedColumnList);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdateListReportDataColumns", ex);
            }
        }

        private void UpdateListReportHotlinks(List<FormFieldInfo> formFields, string listReportView)
        {
            try
            {

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdateListReportHotlinks", ex);
            }
        }

        private void UpdateListReportPrimaryFilter(List<FormFieldInfo> formFields, string listReportView)
        {
            try
            {

            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdateListReportPrimaryFilter", ex);
            }
        }

        /// <summary>
        /// If objectName only has letters, numbers, or underscores, remove any double quotes
        /// Otherwise, assure that the name is surrounded by double quotes
        /// </summary>
        /// <param name="objectName"></param>
        private string ValidateQuotedName(string objectName)
        {
            var cleanName = objectName.Trim().Trim('"');

            if (!mColumnCharNonStandardMatcher.IsMatch(cleanName))
                return cleanName;

            var startsWithQuote = objectName.Trim().StartsWith("\"");
            var endsWithQuote = objectName.Trim().EndsWith("\"");

            // ReSharper disable once ConvertIfStatementToSwitchStatement
            if (startsWithQuote && endsWithQuote)
                return objectName;

            if (startsWithQuote)
                return objectName + "\"";

            if (endsWithQuote)
                return "\"" + objectName;

            return "\"" + objectName + "\"";
        }
    }
}
