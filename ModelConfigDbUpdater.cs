using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using JetBrains.Annotations;
using PRISM;
using TableColumnNameMapContainer;

namespace DMSModelConfigDbUpdater
{
    public class ModelConfigDbUpdater : EventNotifier
    {
        // Ignore Spelling: dms, dpkg, hotlink, hotlinks, idx, mc, ont, Postgres, sw

        internal const string DB_TABLE_DETAIL_REPORT_HOTLINKS = "detail_report_hotlinks";

        internal const string DB_TABLE_GENERAL_PARAMS = "general_params";

        internal const string DB_TABLE_LIST_REPORT_HOTLINKS = "list_report_hotlinks";

        internal const string DB_TABLE_LIST_REPORT_PRIMARY_FILTER = "list_report_primary_filter";

        /// <summary>
        /// Match any character that is not a letter, number, or underscore
        /// </summary>
        private readonly Regex mColumnCharNonStandardMatcher = new("[^a-z0-9_]", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

        private SQLiteConnection mDbConnectionReader;

        private SQLiteConnection mDbConnectionWriter;

        /// <summary>
        /// This is used to assure that only a single warning is shown for each missing view
        /// </summary>
        private readonly SortedSet<string> mMissingViews = new();

        /// <summary>
        /// This is used to look for one or more plus signs at the start of a column name
        /// </summary>
        private readonly Regex mPlusSignMatcher = new(@" *(?<Prefix>\++)(?<ColumnName>.+)", RegexOptions.Compiled);

        /// <summary>
        /// Dictionary tracking validation results
        /// Keys are model config DB file names
        /// Values are the number of errors found
        /// </summary>
        private readonly Dictionary<string, int> mValidationResults = new();

        /// <summary>
        /// Validation results file writer
        /// </summary>
        private StreamWriter mValidationResultsWriter;

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
        /// Filename of the current model config DB
        /// </summary>
        public string CurrentConfigDB { get; set; }

        /// <summary>
        /// Processing options
        /// </summary>
        public ModelConfigDbUpdaterOptions Options { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public ModelConfigDbUpdater(ModelConfigDbUpdaterOptions options)
        {
            Options = options;

            CurrentConfigDB = string.Empty;

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

        private bool CreateValidationResultsFile(FileSystemInfo inputDirectory)
        {
            try
            {
                var resultsFilePath = GetValidateResultsFilePath(inputDirectory.FullName, Options.ValidateResultsFileName);

                mValidationResultsWriter = new StreamWriter(new FileStream(resultsFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite));
                mValidationResultsWriter.AutoFlush = true;

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error creating the validation results file", ex);
                return false;
            }
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

            newFormFieldName = formFieldInfo.NewFieldName;
            return true;
        }

        /// <summary>
        /// Look for plus signs before the field name
        /// If found, return them in the prefix argument
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="prefix"></param>
        /// <returns>Name without any initial plus signs</returns>
        private string GetCleanFieldName(string fieldName, out string prefix)
        {
            var match = mPlusSignMatcher.Match(fieldName);

            if (match.Success)
            {
                prefix = match.Groups["Prefix"].Value;
                return match.Groups["ColumnName"].Value;
            }

            prefix = string.Empty;
            return fieldName;
        }

        /// <summary>
        /// Get the name of the ID column for the given table in the SQLite database
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns>ID column name</returns>
        private string GetIdFieldName(string tableName)
        {
            return tableName switch
            {
                DB_TABLE_DETAIL_REPORT_HOTLINKS => "idx",
                _ => "id"
            };
        }

        /// <summary>
        /// Get the object name, without the schema
        /// </summary>
        /// <remarks>
        /// Simply looks for the first period and assumes the schema name is before the period and the object name is after it
        /// </remarks>
        /// <param name="objectName"></param>
        internal static string GetNameWithoutSchema(string objectName)
        {
            if (string.IsNullOrWhiteSpace(objectName))
                return string.Empty;

            var periodIndex = objectName.IndexOf('.');
            if (periodIndex > 0 && periodIndex < objectName.Length - 1)
                return objectName.Substring(periodIndex + 1);

            return objectName;
        }

        /// <summary>
        /// Get the schema name, or "public" if no schema
        /// </summary>
        /// <param name="objectName"></param>
        internal static string GetSchemaName(string objectName)
        {
            if (string.IsNullOrWhiteSpace(objectName))
                return string.Empty;

            var periodIndex = objectName.IndexOf('.');

            return periodIndex > 0 ? objectName.Substring(0, periodIndex) : "public";
        }

        /// <summary>
        /// Get the validation results file path
        /// </summary>
        /// <param name="inputDirectoryPath"></param>
        /// <param name="validationResultsFileNameOrPath"></param>
        /// <returns>The full path to the validation results file</returns>
        public static string GetValidateResultsFilePath(string inputDirectoryPath, string validationResultsFileNameOrPath)
        {
            if (string.IsNullOrWhiteSpace(inputDirectoryPath))
                inputDirectoryPath = ".";

            if (string.IsNullOrWhiteSpace(validationResultsFileNameOrPath))
            {
                return Path.Combine(inputDirectoryPath, "ValidationResults.txt");
            }

            if (validationResultsFileNameOrPath.StartsWith(@".\"))
            {
                // Save in the working directory
                return validationResultsFileNameOrPath;
            }

            return Path.IsPathRooted(validationResultsFileNameOrPath)
                ? validationResultsFileNameOrPath
                : Path.Combine(inputDirectoryPath, validationResultsFileNameOrPath);
        }

        private bool InitializeWriter(FileInfo modelConfigDb)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(Options.OutputDirectory))
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

                var targetDirectoryPath = Path.IsPathRooted(Options.OutputDirectory)
                    ? Options.OutputDirectory
                    : Path.Combine(modelConfigDb.Directory.FullName, Options.OutputDirectory);

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
                CurrentConfigDB = string.Empty;
                return false;
            }
        }

        private bool LoadNameMapFiles()
        {
            mViewColumnNameMap.Clear();
            mViewNameMap.Clear();
            mViewNameMapWithSchema.Clear();

            var viewColumnMapFile = new FileInfo(Options.ViewColumnMapFile);

            if (!viewColumnMapFile.Exists)
            {
                OnErrorEvent("View column map file not found: " + viewColumnMapFile.FullName);
                return false;
            }

            var mapReader = new NameMapReader();
            RegisterEvents(mapReader);

            // In dictionary tableNameMap, keys are the original (source) table names
            // and values are WordReplacer classes that track the new table names and new column names in Postgres

            // In dictionary viewColumnNameMap, keys are new table names
            // and values are a Dictionary of mappings of original column names to new column names in Postgres;
            // names should not have double quotes around them

            // Dictionary tableNameMapSynonyms has original table names to new table names

            var columnMapFileLoaded = LoadViewColumnMapFile(viewColumnMapFile);

            if (!columnMapFileLoaded)
                return false;

            var tableNameMapSynonyms = new Dictionary<string, string>();

            if (string.IsNullOrWhiteSpace(Options.TableNameMapFile))
                return true;

            var tableNameMapFile = new FileInfo(Options.TableNameMapFile);
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

        /// <summary>
        /// Handle a validation results debug message
        /// </summary>
        /// <param name="message"></param>
        private void OnValidationResultsDebugEvent(string message)
        {
            mValidationResultsWriter?.WriteLine("  " + message);

            if (Options.QuietMode)
                mValidationResultsWriter?.WriteLine();
        }

        /// <summary>
        /// Handle a validation results error message
        /// </summary>
        /// <param name="message"></param>
        /// <param name="ex"></param>
        private void OnValidationResultsErrorEvent(string message, Exception ex)
        {
            if (!message.StartsWith("Error", StringComparison.OrdinalIgnoreCase))
                message = "Error: " + message;

            mValidationResultsWriter?.WriteLine(message);
            mValidationResultsWriter?.WriteLine();

            mValidationResultsWriter?.WriteLine(StackTraceFormatter.GetExceptionStackTraceMultiLine(ex));

            mValidationResultsWriter?.WriteLine();
        }

        /// <summary>
        /// Handle a validation results message or warning
        /// </summary>
        /// <param name="message"></param>
        private void OnValidationResultsEvent(string message)
        {
            mValidationResultsWriter?.WriteLine(message);
        }

        private string PossiblyAddSchema(GeneralParameters generalParams, string objectName)
        {
            if (!Options.UsePostgresSchema)
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

            // Postgres quotes names with double quotes
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
                var inputDirectory = new DirectoryInfo(Options.InputDirectory);
                if (!inputDirectory.Exists)
                {
                    OnErrorEvent("Input directory not found: " + inputDirectory.FullName);
                    return false;
                }

                // Load the various name map files
                if (!LoadNameMapFiles())
                    return false;

                string searchPattern;
                if (string.IsNullOrWhiteSpace(Options.FilenameFilter))
                {
                    searchPattern = "*.db";
                }
                else if (Options.FilenameFilter.EndsWith(".db", StringComparison.OrdinalIgnoreCase))
                {
                    searchPattern = Options.FilenameFilter;
                }
                else
                {
                    if (Options.FilenameFilter.Contains("*"))
                        searchPattern = Options.FilenameFilter + ".db";
                    else
                        searchPattern = Options.FilenameFilter + "*.db";
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

                if (Options.ValidateColumnNamesWithDatabase &&
                    Options.SaveValidateResultsToFile &&
                    !CreateValidationResultsFile(inputDirectory))
                {
                    return false;
                }

                // ReSharper disable once ForeachCanBeConvertedToQueryUsingAnotherGetEnumerator
                foreach (var modelConfigDb in filesToProcess)
                {
                    var success = ProcessFile(modelConfigDb);

                    if (!success)
                        return false;
                }

                if (Options.ValidateColumnNamesWithDatabase)
                {
                    ShowValidationSummary();
                }

                if (Options.ValidateColumnNamesWithDatabase && Options.SaveValidateResultsToFile)
                {
                    mValidationResultsWriter.Close();
                }

                return true;
            }
            catch (Exception ex)
            {
                mValidationResultsWriter?.Close();

                OnErrorEvent("Error in ProcessInputDirectory", ex);
                return false;
            }
        }

        private bool ProcessFile(FileInfo modelConfigDb)
        {
            try
            {
                if (!Options.QuietMode)
                {
                    ShowMessage();
                    ShowMessage("{0} {1}",
                        Options.ValidateColumnNamesWithDatabase ? "Validating" : "Processing",
                        PathUtils.CompactPathString(modelConfigDb.FullName, 80));
                }

                var connectionString = "Data Source=" + modelConfigDb.FullName + "; Version=3; DateTimeFormat=Ticks; Read Only=False;";

                // When calling the constructor, optionally set parseViaFramework to true if reading SqLite files located on a network share or in read-only folders
                mDbConnectionReader = new SQLiteConnection(connectionString, true);

                CurrentConfigDB = modelConfigDb.Name;

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

                if (generalParams.Parameters.Count == 0)
                {
                    // This model config DB does not have table general_params; ignore it
                    if (Options.ValidateColumnNamesWithDatabase)
                    {
                        ShowMessage("Ignoring file {0} since it does not have table {1}", CurrentConfigDB, DB_TABLE_GENERAL_PARAMS);
                        return true;
                    }
                }

                var formFieldsLoaded = ReadFormFields(out var formFields);

                if (!formFieldsLoaded)
                    return false;

                if (Options.ValidateColumnNamesWithDatabase)
                {
                    return ValidateColumnNames(generalParams, formFields);
                }

                if (!Options.PreviewUpdates && !InitializeWriter(modelConfigDb))
                    return false;

                if (Options.RenameEntryPageViewAndColumns)
                {
                    var entryPageView = RenameEntryPageView(generalParams);

                    // Update form_fields, form_field_choosers, form_field_options, and external_sources
                    UpdateFormFields(formFields, entryPageView);
                }

                if (Options.RenameListReportViewAndColumns)
                {
                    var listReportView = RenameListReportView(generalParams);
                    UpdateListReportHotlinks(listReportView);

                    // Update list_report_primary_filter and primary_filter_choosers
                    UpdateListReportPrimaryFilter(listReportView);
                }

                if (Options.RenameDetailReportViewAndColumns)
                {
                    var detailReportView = RenameDetailReportView(generalParams);
                    UpdateDetailReportHotlinks(detailReportView);
                }

                if (Options.RenameStoredProcedures)
                {
                    RenameStoredProcedures(generalParams);
                }

                CurrentConfigDB = string.Empty;
                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ProcessFile", ex);
                CurrentConfigDB = string.Empty;
                return false;
            }
        }

        internal bool ReadExternalSources(out List<BasicField> externalSources)
        {
            externalSources = new List<BasicField>();

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

                    externalSources.Add(new BasicField(id, formField));
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReadExternalSources", ex);
                return false;
            }
        }

        internal bool ReadFormFields(out List<FormFieldInfo> formFields)
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

        internal bool ReadFormFieldOptions(out List<BasicField> formFieldOptions)
        {
            formFieldOptions = new List<BasicField>();

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

                    formFieldOptions.Add(new BasicField(id, formField));
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReadFormFieldOptions", ex);
                return false;
            }
        }

        internal bool ReadFormFieldChoosers(out List<FormFieldChooserInfo> formFieldChoosers)
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

        internal bool ReadGeneralParams(out GeneralParameters generalParams)
        {
            generalParams = new GeneralParameters();

            try
            {
                if (!SQLiteUtilities.TableExists(mDbConnectionReader, DB_TABLE_GENERAL_PARAMS))
                {
                    return true;
                }

                using var dbCommand = mDbConnectionReader.CreateCommand();

                dbCommand.CommandText = "SELECT name, value FROM " + DB_TABLE_GENERAL_PARAMS;

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

        private List<HotLinkInfo> ReadHotlinks(string tableName)
        {
            var hotLinks = new List<HotLinkInfo>();

            if (!SQLiteUtilities.TableExists(mDbConnectionReader, tableName))
            {
                return hotLinks;
            }

            var idFieldName = GetIdFieldName(tableName);

            using var dbCommand = mDbConnectionReader.CreateCommand();

            dbCommand.CommandText = string.Format("SELECT {0}, name, LinkType, WhichArg FROM {1}", idFieldName, tableName);

            using var reader = dbCommand.ExecuteReader();

            while (reader.Read())
            {
                var id = SQLiteUtilities.GetInt32(reader, idFieldName);
                var fieldName = SQLiteUtilities.GetString(reader, "name");
                var linkType = SQLiteUtilities.GetString(reader, "LinkType");
                var whichArg = SQLiteUtilities.GetString(reader, "WhichArg");

                hotLinks.Add(new HotLinkInfo(id, fieldName, linkType, whichArg));
            }

            return hotLinks;
        }

        private List<PrimaryFilterInfo> ReadPrimaryFilters(string tableName)
        {
            var primaryFilters = new List<PrimaryFilterInfo>();

            if (!SQLiteUtilities.TableExists(mDbConnectionReader, tableName))
            {
                return primaryFilters;
            }

            using var dbCommand = mDbConnectionReader.CreateCommand();

            dbCommand.CommandText = string.Format("SELECT id, label, col FROM {0}", tableName);

            using var reader = dbCommand.ExecuteReader();

            while (reader.Read())
            {
                var id = SQLiteUtilities.GetInt32(reader, "id");
                var label = SQLiteUtilities.GetString(reader, "label");
                var fieldName = SQLiteUtilities.GetString(reader, "col");

                primaryFilters.Add(new PrimaryFilterInfo(id, label, fieldName));
            }

            return primaryFilters;
        }

        internal bool ReadStoredProcedureArguments(out List<StoredProcArgumentInfo> storedProcedureArguments)
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

                if (ColumnRenamed(viewNameToUse, generalParams.Parameters[GeneralParameters.ParameterType.DetailReportDataIdColumn], out var columnNameToUse))
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

                if (ColumnRenamed(viewNameToUse, generalParams.Parameters[GeneralParameters.ParameterType.ListReportSortColumn], out var columnNameToUse))
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
                OnStatusEvent("{0,-25} {1} is already {2}", CurrentConfigDB + ":", objectDescription, nameToUse);
                return nameToUse;
            }

            UpdateGeneralParameter(generalParams, parameterType, nameToUse);

            return nameToUse;
        }

        /// <summary>
        /// Add a blank link to the console output and optionally to the validation results file
        /// </summary>
        protected void ShowMessage()
        {
            ShowMessage(string.Empty);
        }

        /// <summary>Report a status message</summary>
        /// <param name="format">Status message format string</param>
        /// <param name="args">string format arguments</param>
        [StringFormatMethod("format")]
        protected void ShowMessage(string format, params object[] args)
        {
            var formattedMessage = string.Format(format, args);
            OnStatusEvent(formattedMessage);

            if (Options.SaveValidateResultsToFile)
            {
                OnValidationResultsEvent(formattedMessage);
            }
        }

        /// <summary>Report a warning message</summary>
        /// <param name="format">Status message format string</param>
        /// <param name="args">string format arguments</param>
        [StringFormatMethod("format")]
        protected void ShowWarning(string format, params object[] args)
        {
            var formattedMessage = string.Format(format, args);
            OnWarningEvent(formattedMessage);

            if (Options.SaveValidateResultsToFile)
            {
                OnValidationResultsEvent(formattedMessage);
            }
        }

        private void ShowValidationSummary()
        {
            if (mValidationResults.Count == 0)
            {
                OnWarningEvent("No model config DBs were found; could not validate column names");
            }

            if (mValidationResults.Count == 1)
                return;

            var fileCountWithErrors = mValidationResults.Values.Count(item => item > 0);

            ShowMessage();

            if (fileCountWithErrors == 0)
            {
                ShowMessage("Validated column names in {0} model config DBs; no errors were found", mValidationResults.Count);
                return;
            }

            ShowWarning("{0} / {1} model config DBs had column name or form field name errors", fileCountWithErrors, mValidationResults.Count);

            foreach (var item in mValidationResults)
            {
                if (item.Value == 0)
                    continue;

                ShowWarning("{0,-25} {1} error{2}", item.Key + ":", item.Value, item.Value > 1 ? "s" : string.Empty);
            }

            ShowMessage();
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

        /// <summary>
        /// Update column names referenced by detail report hotlinks
        /// </summary>
        /// <param name="detailReportView"></param>
        private void UpdateDetailReportHotlinks(string detailReportView)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(detailReportView))
                {
                    return;
                }

                var detailReportHotlinks = ReadHotlinks(DB_TABLE_DETAIL_REPORT_HOTLINKS);

                UpdateHotlinks(detailReportView, DB_TABLE_DETAIL_REPORT_HOTLINKS, detailReportHotlinks);
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

                var formFieldChoosersLoaded = ReadFormFieldChoosers(out var formFieldChoosers);
                if (!formFieldChoosersLoaded)
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
                    if (!ColumnRenamed(entryPageView, formField.FieldName, out var columnNameToUse, true))
                        continue;

                    formField.NewFieldName = columnNameToUse;
                    renamedFormFields.Add(formField.FieldName, formField);
                }

                if (Options.PreviewUpdates)
                {
                    OnStatusEvent(
                        "{0,-25} Would rename {1} form field{2}", CurrentConfigDB + ":",
                        renamedFormFields.Count, renamedFormFields.Count == 1 ? string.Empty : "s");

                    return;
                }

                using var dbCommand = mDbConnectionWriter.CreateCommand();

                foreach (var formField in renamedFormFields.Values)
                {
                    dbCommand.CommandText = string.Format(
                        "UPDATE form_fields SET Name = '{0}' WHERE id = {1}",
                        formField.NewFieldName, formField.ID);

                    dbCommand.ExecuteNonQuery();
                }

                OnStatusEvent(
                    "{0,-25} Renamed {1} form field{2} in 'form_fields'", CurrentConfigDB + ":",
                    renamedFormFields.Count, renamedFormFields.Count == 1 ? string.Empty : "s");

                var updatedItems = 0;

                foreach (var procedureArgument in storedProcedureArguments)
                {
                    if (!FormFieldRenamed(renamedFormFields, procedureArgument.FieldName, out var newFormFieldName))
                        continue;

                    dbCommand.CommandText = string.Format(
                        "UPDATE sproc_args SET field = '{0}' WHERE id = {1}",
                        newFormFieldName, procedureArgument.ID);

                    dbCommand.ExecuteNonQuery();
                    updatedItems++;
                }

                OnStatusEvent(
                    "{0,-25} Renamed {1} form field{2} in 'sproc_args'", CurrentConfigDB + ":",
                    updatedItems, updatedItems == 1 ? string.Empty : "s");

                updatedItems = 0;

                foreach (var formFieldChooser in formFieldChoosers)
                {
                    var fieldRenamed = FormFieldRenamed(renamedFormFields, formFieldChooser.FieldName, out var newFormFieldName);

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
                    "{0,-25} Renamed {1} form field{2} in 'form_field_choosers'", CurrentConfigDB + ":",
                    updatedItems, updatedItems == 1 ? string.Empty : "s");

                updatedItems = 0;

                foreach (var formFieldOption in formFieldOptions)
                {
                    if (!FormFieldRenamed(renamedFormFields, formFieldOption.FieldName, out var newFormFieldName))
                        continue;

                    dbCommand.CommandText = string.Format(
                        "UPDATE form_field_options SET field = '{0}' WHERE id = {1}",
                        newFormFieldName, formFieldOption.ID);

                    dbCommand.ExecuteNonQuery();
                    updatedItems++;
                }

                OnStatusEvent(
                    "{0,-25} Renamed {1} form field{2} in 'form_field_options'", CurrentConfigDB + ":",
                    updatedItems, updatedItems == 1 ? string.Empty : "s");

                updatedItems = 0;

                foreach (var externalSource in externalSources)
                {
                    if (!FormFieldRenamed(renamedFormFields, externalSource.FieldName, out var newFormFieldName))
                        continue;

                    dbCommand.CommandText = string.Format(
                        "UPDATE external_sources SET field = '{0}' WHERE id = {1}",
                        newFormFieldName, externalSource.ID);

                    dbCommand.ExecuteNonQuery();
                    updatedItems++;
                }

                OnStatusEvent(
                    "{0,-25} Renamed {1} form field{2} in 'external_sources'", CurrentConfigDB + ":",
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
                    OnStatusEvent("{0,-25} {1} is already {2}", CurrentConfigDB + ":", generalParamsKeyName, newValue);
                }

                return;
            }

            if (Options.PreviewUpdates)
            {
                if (reportUpdate)
                {
                    OnStatusEvent("{0,-25} Would change {1} from {2} to {3}", CurrentConfigDB + ":", generalParamsKeyName, currentValue ?? "an empty string", newValue);
                }

                return;
            }

            // Update the database
            using var dbCommand = mDbConnectionWriter.CreateCommand();

            // Note: escape single quotes using ''

            dbCommand.CommandText = string.Format("UPDATE {0} set value = '{1}' WHERE name = '{2}'", DB_TABLE_GENERAL_PARAMS, newValue.Replace("'", "''"), generalParamsKeyName);

            dbCommand.ExecuteNonQuery();

            // Update the cached value
            generalParams.Parameters[parameterType] = newValue;

            if (reportUpdate)
            {
                OnStatusEvent("{0,-25} Changed {1} from {2} to {3}", CurrentConfigDB + ":", generalParamsKeyName, currentValue ?? "an empty string", newValue);
            }
        }

        private void UpdateHotlinks(string sourceView, string tableName, List<HotLinkInfo> hotlinks)
        {
            foreach (var item in hotlinks)
            {
                var originalColumnName = GetCleanFieldName(item.FieldName, out var prefix);

                if (ColumnRenamed(sourceView, originalColumnName, out var columnNameToUse))
                {
                    item.NewFieldName = prefix + columnNameToUse;
                    item.Updated = true;
                }

                // List report hotlinks often use "value" to indicate the value to use in the link is the same column that the hotlink appears in
                // Hotlinks with an empty string for WhichArg include color_label and literal_link

                if (string.IsNullOrWhiteSpace(item.WhichArg) ||
                    tableName.Equals(DB_TABLE_LIST_REPORT_HOTLINKS) && item.WhichArg.Equals("value", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                // ReSharper disable once InvertIf
                if (ColumnRenamed(sourceView, item.WhichArg, out var targetColumnToUse))
                {
                    item.WhichArg = targetColumnToUse;
                    item.Updated = true;
                }
            }

            var saveChanges = hotlinks.Any(item => item.Updated);

            if (!saveChanges)
                return;

            if (Options.PreviewUpdates)
            {
                Console.WriteLine();
                OnStatusEvent("Hotlink updates for {0} in {1}", sourceView, CurrentConfigDB);
            }

            var idFieldName = GetIdFieldName(tableName);

            using var dbCommand = mDbConnectionWriter.CreateCommand();

            var updatedItems = 0;

            foreach (var item in hotlinks)
            {
                if (!item.Updated)
                    continue;

                var nameToUse = string.IsNullOrWhiteSpace(item.NewFieldName) ? item.FieldName : item.NewFieldName;

                if (Options.PreviewUpdates)
                {
                    OnStatusEvent("{0,2}: {1,-30} {2,-20} {3}", item.ID, nameToUse, item.LinkType, item.WhichArg);
                    continue;
                }

                dbCommand.CommandText = string.Format(
                    "UPDATE {0} SET name = '{1}', WhichArg = '{2}' WHERE {3} = {4}",
                    tableName, nameToUse, item.WhichArg, idFieldName, item.ID);

                dbCommand.ExecuteNonQuery();
                updatedItems++;
            }

            if (Options.PreviewUpdates)
                return;

            OnStatusEvent(
                "{0,-25} Renamed {1} hotlink{2} in '{3}'", CurrentConfigDB + ":",
                updatedItems, updatedItems == 1 ? string.Empty : "s", sourceView);
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
                        aliasNameToUse = ColumnRenamed(generalParams.Parameters[viewType], aliasName, out var newAliasName, snakeCaseNames)
                            ? newAliasName
                            : aliasName;
                    }
                    else
                    {
                        aliasNameToUse = string.Empty;
                    }

                    var columnNameToUse = ColumnRenamed(generalParams.Parameters[viewType], columnNameToFind, out var newColumnName, snakeCaseNames)
                        ? newColumnName
                        : columnNameToFind;

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

        /// <summary>
        /// Update column names referenced by list report hotlinks
        /// </summary>
        /// <param name="listReportView"></param>
        private void UpdateListReportHotlinks(string listReportView)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(listReportView))
                {
                    return;
                }

                var listReportHotlinks = ReadHotlinks(DB_TABLE_LIST_REPORT_HOTLINKS);

                UpdateHotlinks(listReportView, DB_TABLE_LIST_REPORT_HOTLINKS, listReportHotlinks);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdateListReportHotlinks", ex);
            }
        }

        /// <summary>
        /// Update column names referenced by list report primary filters
        /// </summary>
        /// <param name="listReportView"></param>
        private void UpdateListReportPrimaryFilter(string listReportView)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(listReportView))
                {
                    return;
                }

                var primaryFilters = ReadPrimaryFilters(DB_TABLE_LIST_REPORT_PRIMARY_FILTER);

                UpdatePrimaryFilters(listReportView, DB_TABLE_LIST_REPORT_PRIMARY_FILTER, primaryFilters);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in UpdateListReportPrimaryFilter", ex);
            }
        }

        private void UpdatePrimaryFilters(string sourceView, string tableName, List<PrimaryFilterInfo> primaryFilters)
        {
            foreach (var item in primaryFilters)
            {
                if (ColumnRenamed(sourceView, item.FieldName, out var columnNameToUse))
                {
                    item.NewFieldName = columnNameToUse;
                    item.Updated = true;
                }
            }

            var saveChanges = primaryFilters.Any(item => item.Updated);

            if (!saveChanges)
                return;

            if (Options.PreviewUpdates)
            {
                Console.WriteLine();
                OnStatusEvent("Primary filter updates for {0} in {1}", sourceView, CurrentConfigDB);
            }

            using var dbCommand = mDbConnectionWriter.CreateCommand();

            var updatedItems = 0;

            foreach (var item in primaryFilters)
            {
                if (!item.Updated)
                    continue;

                var nameToUse = string.IsNullOrWhiteSpace(item.NewFieldName) ? item.FieldName : item.NewFieldName;

                if (Options.PreviewUpdates)
                {
                    OnStatusEvent("{0,2}: {1,-30}", item.ID, nameToUse);
                    continue;
                }

                dbCommand.CommandText = string.Format("UPDATE {0} SET col = '{1}' WHERE id = {2}", tableName, nameToUse, item.ID);

                dbCommand.ExecuteNonQuery();
                updatedItems++;
            }

            if (Options.PreviewUpdates)
                return;

            OnStatusEvent(
                "{0,-25} Renamed {1} primary filter{2} in '{3}'", CurrentConfigDB + ":",
                updatedItems, updatedItems == 1 ? string.Empty : "s", sourceView);
        }

        private bool ValidateColumnNames(GeneralParameters generalParams, List<FormFieldInfo> formFields)
        {
            try
            {
                var validator = new ModelConfigDbValidator(this, generalParams, formFields);
                RegisterEvents(validator);

                if (Options.SaveValidateResultsToFile)
                {
                    validator.DebugEvent += OnValidationResultsDebugEvent;
                    validator.StatusEvent += OnValidationResultsEvent;
                    validator.ErrorEvent += OnValidationResultsErrorEvent;
                    validator.WarningEvent += OnValidationResultsEvent;
                }

                var success = validator.ValidateColumnNames(out var errorCount);

                mValidationResults.Add(CurrentConfigDB, errorCount);

                return success;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateColumnNames", ex);
                return false;
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
