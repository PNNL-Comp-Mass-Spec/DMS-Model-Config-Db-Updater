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
        // Ignore Spelling: dms, dpkg, mc, ont, sw

        /// <summary>
        /// Match any character that is not a letter, number, or underscore
        /// </summary>
        private readonly Regex mColumnCharNonStandardMatcher = new("[^a-z0-9_]", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

        private string mCurrentConfigDB;

        private SQLiteConnection mDbConnectionReader;

        private SQLiteConnection mDbConnectionWriter;

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

        private bool ColumnRenamed(string viewName, string currentColumnName, out string columnNameToUse, bool snakeCaseName = false)
        {
            if (string.IsNullOrWhiteSpace(currentColumnName))
            {
                columnNameToUse = string.Empty;
                return false;
            }

            if (currentColumnName.Trim().StartsWith("'") && currentColumnName.Trim().EndsWith("'"))
            {
                // String literal; leave as-is
                columnNameToUse = currentColumnName;
                return false;
            }

            if (!TryGetColumnMap(viewName, out var columnMap))
            {
                columnNameToUse = snakeCaseName
                    ? ConvertToSnakeCaseAndUpdatePrefix(currentColumnName)
                    : currentColumnName;

                return !currentColumnName.Equals(columnNameToUse);
            }

            if (!columnMap.TryGetValue(currentColumnName, out var columnInfo))
            {
                columnNameToUse = snakeCaseName
                    ? ConvertToSnakeCaseAndUpdatePrefix(currentColumnName)
                    : currentColumnName;

                return !currentColumnName.Equals(columnNameToUse);
            }

            columnNameToUse = snakeCaseName
                ? ConvertToSnakeCaseAndUpdatePrefix(columnInfo.NewColumnName)
                : columnInfo.NewColumnName;

            return !currentColumnName.Equals(columnNameToUse);
        }

        private string ConvertToSnakeCaseAndUpdatePrefix(string currentName)
        {
            if (currentName.StartsWith("EUS", StringComparison.OrdinalIgnoreCase) && !currentName.StartsWith("EUS_", StringComparison.OrdinalIgnoreCase))
            {
                currentName = "EUS_" + currentName.Substring(3);
            }

            var updatedName = NameUpdater.ConvertNameToSnakeCase(currentName);

            if (updatedName.StartsWith("aj_") ||
                updatedName.StartsWith("ds_") ||
                updatedName.StartsWith("ap_") ||
                updatedName.StartsWith("sc_") ||
                updatedName.StartsWith("rr_") ||
                updatedName.StartsWith("sp_"))
            {
                return updatedName.Substring(3);
            }

            if (updatedName.StartsWith("ajr_") ||
                updatedName.StartsWith("org_"))
            {
                return updatedName.Substring(4);
            }

            return updatedName;
        }

        private bool FormFieldRenamed(IReadOnlyDictionary<string, FormFieldInfo> renamedFormFields, string formFieldName, out string newFormFieldName)
        {
            if (string.IsNullOrWhiteSpace(formFieldName))
            {
                newFormFieldName = string.Empty;
                return false;
            }

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

        private bool InitializeWriter(FileInfo modelConfigDb)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(mOptions.OutputDirectory))
                {
                    // Update files in-place
                    mDbConnectionWriter = mDbConnectionReader;
                    return true;
                }

                if (modelConfigDb.Directory == null)
                {
                    OnErrorEvent("Unable to determine the parent directory of SQLite DB " + PathUtils.CompactPathString(modelConfigDb.FullName, 80));
                    return false;
                }

                var targetDirectoryPath = Path.IsPathRooted(mOptions.OutputDirectory)
                    ? mOptions.OutputDirectory
                    : Path.Combine(modelConfigDb.Directory.FullName, mOptions.OutputDirectory);

                if (targetDirectoryPath.Equals(modelConfigDb.Directory.FullName, StringComparison.OrdinalIgnoreCase))
                {
                    OnErrorEvent(
                        "The output directory is the same as the input directory; " +
                        "either set the output directory to an empty string to update files in-place " +
                        "or specify a different output directory");

                    return false;
                }

                var targetFilePath = Path.Combine(targetDirectoryPath, modelConfigDb.Name);

                // Create the target directory if missing
                var targetDirectory = new DirectoryInfo(targetDirectoryPath);

                if (!targetDirectory.Exists)
                    targetDirectory.Create();

                // Copy the source file to the target file
                modelConfigDb.CopyTo(targetFilePath, true);

                var connectionString = "Data Source=" + targetFilePath + "; Version=3; DateTimeFormat=Ticks; Read Only=False;";

                // When calling the constructor, optionally set parseViaFramework to true if reading SqLite files located on a network share or in read-only folders
                mDbConnectionWriter = new SQLiteConnection(connectionString, true);

                try
                {
                    mDbConnectionWriter.Open();
                }
                catch (Exception ex)
                {
                    OnErrorEvent("Error opening SQLite database " + modelConfigDb.Name, ex);
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in InitializeWriter", ex);
                mCurrentConfigDB = string.Empty;
                return false;
            }
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
                        // The view column map file has duplicate rows; only keep the first occurrence of each column
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
            if (!mOptions.UsePostgresSchema)
            {
                if (objectName.StartsWith("\"public\""))
                {
                    return objectName.Substring(9).Trim('"');
                }

                // ReSharper disable once ConvertIfStatementToReturnStatement
                if (objectName.StartsWith("public."))
                {
                    return objectName.Substring(8);
                }

                return objectName;
            }

            if (objectName.Contains("."))
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
            if (objectName.StartsWith("'") && objectName.EndsWith("'"))
            {
                // Literal string; leave as-is
                return objectName;
            }

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

                Console.WriteLine();

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

        private bool ProcessFile(FileInfo modelConfigDb)
        {
            try
            {
                Console.WriteLine();
                Console.WriteLine("Processing " + PathUtils.CompactPathString(modelConfigDb.FullName, 80));

                var connectionString = "Data Source=" + modelConfigDb.FullName + "; Version=3; DateTimeFormat=Ticks; Read Only=False;";

                // When calling the constructor, optionally set parseViaFramework to true if reading SqLite files located on a network share or in read-only folders
                mDbConnectionReader = new SQLiteConnection(connectionString, true);

                mCurrentConfigDB = modelConfigDb.Name;

                try
                {
                    mDbConnectionReader.Open();
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

                if (!mOptions.PreviewUpdates && !InitializeWriter(modelConfigDb))
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

        private bool ReadExternalSources(out List<BasicFormField> externalSources)
        {
            externalSources = new List<BasicFormField>();

            try
            {
                if (!SQLiteUtilities.TableExists(mDbConnectionReader, "external_sources"))
                    return true;

                using var dbCommand = mDbConnectionReader.CreateCommand();

                dbCommand.CommandText = "SELECT id, field FROM external_sources";

                using var reader = dbCommand.ExecuteReader();

                while (reader.Read())
                {
                    var id = SQLiteUtilities.GetInt32(reader, "id");
                    var formField = SQLiteUtilities.GetString(reader, "field");

                    externalSources.Add(new BasicFormField(id, formField));
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReadExternalSources", ex);
                return false;
            }
        }

        private bool ReadFormFields(out List<FormFieldInfo> formFields)
        {
            formFields = new List<FormFieldInfo>();

            try
            {
                if (!SQLiteUtilities.TableExists(mDbConnectionReader, "form_fields"))
                    return true;

                using var dbCommand = mDbConnectionReader.CreateCommand();

                dbCommand.CommandText = "SELECT id, name, label FROM form_fields";

                using var reader = dbCommand.ExecuteReader();

                while (reader.Read())
                {
                    var id = SQLiteUtilities.GetInt32(reader, "id");
                    var formField = SQLiteUtilities.GetString(reader, "name");
                    var label = SQLiteUtilities.GetString(reader, "label");

                    formFields.Add(new FormFieldInfo(id, formField, label));
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReadFormFields", ex);
                return false;
            }
        }

        private bool ReadFormFieldOptions(out List<BasicFormField> formFieldOptions)
        {
            formFieldOptions = new List<BasicFormField>();

            try
            {
                if (!SQLiteUtilities.TableExists(mDbConnectionReader, "form_field_options"))
                    return true;

                using var dbCommand = mDbConnectionReader.CreateCommand();

                dbCommand.CommandText = "SELECT id, field FROM form_field_options";

                using var reader = dbCommand.ExecuteReader();

                while (reader.Read())
                {
                    var id = SQLiteUtilities.GetInt32(reader, "id");
                    var formField = SQLiteUtilities.GetString(reader, "field");

                    formFieldOptions.Add(new BasicFormField(id, formField));
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReadFormFieldOptions", ex);
                return false;
            }
        }

        private bool ReadFormFieldChoosers(out List<FormFieldChooserInfo> formFieldChoosers)
        {
            formFieldChoosers = new List<FormFieldChooserInfo>();

            try
            {
                if (!SQLiteUtilities.TableExists(mDbConnectionReader, "form_field_choosers"))
                    return true;

                using var dbCommand = mDbConnectionReader.CreateCommand();

                dbCommand.CommandText = "SELECT id, field, XRef FROM form_field_choosers";

                using var reader = dbCommand.ExecuteReader();

                while (reader.Read())
                {
                    var id = SQLiteUtilities.GetInt32(reader, "id");
                    var formField = SQLiteUtilities.GetString(reader, "field");
                    var crossReference = SQLiteUtilities.GetString(reader, "XRef");

                    formFieldChoosers.Add(new FormFieldChooserInfo(id, formField, crossReference));
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReadFormFieldChoosers", ex);
                return false;
            }
        }

        private bool ReadGeneralParams(out GeneralParameters generalParams)
        {
            generalParams = new GeneralParameters();

            try
            {
                using var dbCommand = mDbConnectionReader.CreateCommand();

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

        private bool ReadStoredProcedureArguments(out List<StoredProcArgumentInfo> storedProcedureArguments)
        {
            storedProcedureArguments = new List<StoredProcArgumentInfo>();

            try
            {
                if (!SQLiteUtilities.TableExists(mDbConnectionReader, "sproc_args"))
                    return true;

                using var dbCommand = mDbConnectionReader.CreateCommand();

                dbCommand.CommandText = "SELECT id, field, name FROM sproc_args";

                using var reader = dbCommand.ExecuteReader();

                while (reader.Read())
                {
                    var id = SQLiteUtilities.GetInt32(reader, "id");
                    var formField = SQLiteUtilities.GetString(reader, "field");
                    var argumentName = SQLiteUtilities.GetString(reader, "name");

                    storedProcedureArguments.Add(new StoredProcArgumentInfo(id, formField, argumentName));
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReadStoredProcedureArguments", ex);
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

                if (ColumnRenamed(viewNameToUse, generalParams.Parameters[GeneralParameters.ParameterType.EntryPageDataIdColumn], out var dataIdNameToUse, true))
                {
                    UpdateGeneralParameter(generalParams, GeneralParameters.ParameterType.EntryPageDataIdColumn, dataIdNameToUse);
                }

                if (!string.IsNullOrWhiteSpace(generalParams.Parameters[GeneralParameters.ParameterType.EntryPageDataColumns]))
                {
                    UpdateEntryPageDataColumns(generalParams);
                }

                if (ColumnRenamed(viewNameToUse, generalParams.Parameters[GeneralParameters.ParameterType.PostSubmissionDetailId], out var postSubmissionNameToUse, true))
                {
                    UpdateGeneralParameter(generalParams, GeneralParameters.ParameterType.PostSubmissionDetailId, postSubmissionNameToUse);
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

            string updatedName;

            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (mViewNameMap.TryGetValue(currentName, out var nameFromNameMap))
            {
                // Renamed view found in the loaded name map data
                updatedName = nameFromNameMap;
            }
            else
            {
                // Snake case the name since Perl script sqlserver2pgsql.pl renames all views to snake case
                updatedName = ConvertToSnakeCaseAndUpdatePrefix(currentName);
            }

            var nameToUse = PossiblyAddSchema(generalParams, updatedName);

            if (currentName.Equals(nameToUse))
            {
                OnStatusEvent("{0,-25} {1} is already {2}", mCurrentConfigDB + ":", objectDescription, nameToUse);
                return nameToUse;
            }

            UpdateGeneralParameter(generalParams, parameterType, nameToUse);

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
            if (mViewNameMap.TryGetValue(viewName, out var nameWithSchema))
            {
                columnMap = mViewColumnNameMap[nameWithSchema];
                return true;
            }

            var viewFound = mViewColumnNameMap.TryGetValue(viewName, out columnMap);

            if (viewFound || mMissingViews.Contains(viewName))
                return viewFound;

            OnWarningEvent(
                "Cannot check for explicit column renames since view not found in mViewColumnNameMap: {0}\n" +
                "Form fields will be auto-converted to lowercase snake case", viewName);

            Console.WriteLine();

            mMissingViews.Add(viewName);

            return false;
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

        private void UpdateEntryPageDataColumns(GeneralParameters generalParams)
        {
            UpdateListOfDataColumns(generalParams, GeneralParameters.ParameterType.EntryPageDataColumns, GeneralParameters.ParameterType.EntryPageView, true);
        }

        private void UpdateFormFields(List<FormFieldInfo> formFields, string entryPageView)
        {
            try
            {
                var storedProcedureArgsLoaded = ReadStoredProcedureArguments(out var storedProcedureArguments);
                if (!storedProcedureArgsLoaded)
                    return;

                var formFieldsLoaded = ReadFormFieldChoosers(out var formFieldChoosers);
                if (!formFieldsLoaded)
                    return;

                var formFieldOptionsLoaded = ReadFormFieldOptions(out var formFieldOptions);
                if (!formFieldOptionsLoaded)
                    return;

                var externalSourcesLoaded = ReadExternalSources(out var externalSources);
                if (!externalSourcesLoaded)
                    return;

                // Keys are the original form field names, values include the updated info
                var renamedFormFields = new Dictionary<string, FormFieldInfo>(StringComparer.OrdinalIgnoreCase);

                foreach (var formField in formFields)
                {
                    if (!ColumnRenamed(entryPageView, formField.FormFieldName, out var columnNameToUse, true))
                        continue;

                    formField.NewName = columnNameToUse;
                    renamedFormFields.Add(formField.FormFieldName, formField);
                }

                if (mOptions.PreviewUpdates)
                {
                    OnStatusEvent(
                        "{0,-25} Would rename {1} form field{2}", mCurrentConfigDB + ":",
                        renamedFormFields.Count, renamedFormFields.Count == 1 ? string.Empty : "s");

                    return;
                }

                using var dbCommand = mDbConnectionWriter.CreateCommand();

                foreach (var formField in renamedFormFields.Values)
                {
                    dbCommand.CommandText = string.Format(
                        "UPDATE form_fields SET Name = '{0}' WHERE id = {1}",
                        formField.NewName, formField.ID);

                    dbCommand.ExecuteNonQuery();
                }

                OnStatusEvent(
                    "{0,-25} Renamed {1} form field{2} in 'form_fields'", mCurrentConfigDB + ":",
                    renamedFormFields.Count, renamedFormFields.Count == 1 ? string.Empty : "s");

                var updatedItems = 0;

                foreach (var procedureArgument in storedProcedureArguments)
                {
                    if (!FormFieldRenamed(renamedFormFields, procedureArgument.FormFieldName, out var newFormFieldName))
                        continue;

                    dbCommand.CommandText = string.Format(
                        "UPDATE sproc_args SET field = '{0}' WHERE id = {1}",
                        newFormFieldName, procedureArgument.ID);

                    dbCommand.ExecuteNonQuery();
                    updatedItems++;
                }

                OnStatusEvent(
                    "{0,-25} Renamed {1} form field{2} in 'sproc_args'", mCurrentConfigDB + ":",
                    updatedItems, updatedItems == 1 ? string.Empty : "s");

                updatedItems = 0;

                foreach (var formFieldChooser in formFieldChoosers)
                {
                    var fieldRenamed = FormFieldRenamed(renamedFormFields, formFieldChooser.FormFieldName, out var newFormFieldName);

                    var crossReferenceRenamed = FormFieldRenamed(renamedFormFields, formFieldChooser.CrossReference, out var newCrossReferenceFieldName);

                    // ReSharper disable once ConvertIfStatementToSwitchStatement
                    if (!fieldRenamed && !crossReferenceRenamed)
                        continue;

                    if (fieldRenamed)
                    {
                        dbCommand.CommandText = string.Format(
                            "UPDATE form_field_choosers SET field = '{0}' WHERE id = {1}",
                            newFormFieldName, formFieldChooser.ID);

                        dbCommand.ExecuteNonQuery();
                    }

                    if (crossReferenceRenamed)
                    {
                        dbCommand.CommandText = string.Format(
                            "UPDATE form_field_choosers SET XRef = '{0}' WHERE id = {1}",
                            newCrossReferenceFieldName, formFieldChooser.ID);

                        dbCommand.ExecuteNonQuery();
                    }

                    updatedItems++;
                }

                OnStatusEvent(
                    "{0,-25} Renamed {1} form field{2} in 'form_field_choosers'", mCurrentConfigDB + ":",
                    updatedItems, updatedItems == 1 ? string.Empty : "s");

                updatedItems = 0;

                foreach (var formFieldOption in formFieldOptions)
                {
                    if (!FormFieldRenamed(renamedFormFields, formFieldOption.FormFieldName, out var newFormFieldName))
                        continue;

                    dbCommand.CommandText = string.Format(
                        "UPDATE form_field_options SET field = '{0}' WHERE id = {1}",
                        newFormFieldName, formFieldOption.ID);

                    dbCommand.ExecuteNonQuery();
                    updatedItems++;
                }

                OnStatusEvent(
                    "{0,-25} Renamed {1} form field{2} in 'form_field_options'", mCurrentConfigDB + ":",
                    updatedItems, updatedItems == 1 ? string.Empty : "s");

                updatedItems = 0;

                foreach (var externalSource in externalSources)
                {
                    if (!FormFieldRenamed(renamedFormFields, externalSource.FormFieldName, out var newFormFieldName))
                        continue;

                    dbCommand.CommandText = string.Format(
                        "UPDATE external_sources SET field = '{0}' WHERE id = {1}",
                        newFormFieldName, externalSource.ID);

                    dbCommand.ExecuteNonQuery();
                    updatedItems++;
                }

                OnStatusEvent(
                    "{0,-25} Renamed {1} form field{2} in 'external_sources'", mCurrentConfigDB + ":",
                    updatedItems, updatedItems == 1 ? string.Empty : "s");
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
                    OnStatusEvent("{0,-25} {1} is already {2}", mCurrentConfigDB + ":", generalParamsKeyName, newValue);
                }

                return;
            }

            if (mOptions.PreviewUpdates)
            {
                if (reportUpdate)
                {
                    OnStatusEvent("{0,-25} Would change {1} from {2} to {3}", mCurrentConfigDB + ":", generalParamsKeyName, currentValue ?? "an empty string", newValue);
                }

                return;
            }

            // Update the database
            using var dbCommand = mDbConnectionWriter.CreateCommand();

            // Note: escape single quotes using ''

            dbCommand.CommandText = string.Format("UPDATE general_params set value = '{0}' WHERE name = '{1}'", newValue.Replace("'", "''"), generalParamsKeyName);

            dbCommand.ExecuteNonQuery();

            // Update the cached value
            generalParams.Parameters[parameterType] = newValue;

            if (reportUpdate)
            {
                OnStatusEvent("{0,-25} Changed {1} from {2} to {3}", mCurrentConfigDB + ":", generalParamsKeyName, currentValue ?? "an empty string", newValue);
            }
        }

        private void UpdateListOfDataColumns(
            GeneralParameters generalParams,
            GeneralParameters.ParameterType parameterType,
            GeneralParameters.ParameterType viewType,
            bool snakeCaseNames)
        {
            try
            {
                var columnList = generalParams.Parameters[parameterType].Split(',');

                var updatedColumns = new List<string>();

                foreach (var currentColumn in columnList)
                {
                    var asIndex = currentColumn.IndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
                    string columnNameToFind;
                    string aliasName;

                    if (asIndex > 0)
                    {
                        columnNameToFind = TrimQuotes(currentColumn.Substring(0, asIndex)).Trim();
                        aliasName = currentColumn.Substring(asIndex + 4).Trim();
                    }
                    else
                    {
                        columnNameToFind = TrimQuotes(currentColumn).Trim();
                        aliasName = string.Empty;
                    }

                    string aliasNameToUse;

                    if (viewType == GeneralParameters.ParameterType.EntryPageView)
                    {
                        // See if the column alias needs to be updated
                        if (ColumnRenamed(generalParams.Parameters[viewType], aliasName, out var newAliasName, snakeCaseNames))
                        {
                            aliasNameToUse = newAliasName;
                        }
                        else
                        {
                            aliasNameToUse = aliasName;
                        }
                    }
                    else
                    {
                        aliasNameToUse = string.Empty;
                    }

                    string columnNameToUse;

                    if (ColumnRenamed(generalParams.Parameters[viewType], columnNameToFind, out var newColumnName, snakeCaseNames))
                    {
                        columnNameToUse = newColumnName;
                    }
                    else
                    {
                        columnNameToUse = columnNameToFind;
                    }

                    if (string.IsNullOrWhiteSpace(aliasNameToUse))
                    {
                        updatedColumns.Add(PossiblyQuoteName(columnNameToUse));
                    }
                    else
                    {
                        var nameWithAlias = PossiblyQuoteName(columnNameToUse) + " AS " + aliasNameToUse;
                        updatedColumns.Add(nameWithAlias);
                    }
                }

                var updatedColumnList = string.Join(", ", updatedColumns);

                UpdateGeneralParameter(generalParams, parameterType, updatedColumnList);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdateListReportDataColumns", ex);
            }
        }

        private void UpdateListReportDataColumns(GeneralParameters generalParams)
        {
            UpdateListOfDataColumns(generalParams, GeneralParameters.ParameterType.ListReportDataColumns, GeneralParameters.ParameterType.ListReportView, false);
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
