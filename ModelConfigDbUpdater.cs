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
    /// <summary>
    /// Model config DB updater and validator
    /// </summary>
    public class ModelConfigDbUpdater : EventNotifier
    {
        // Ignore Spelling: dms, dpkg, hotlink, hotlinks, idx, mc, ont, Postgres, sw, validator

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
        /// Form field chooser definitions loaded from dms_chooser.db
        /// </summary>
        /// <remarks>Only used when validating field names</remarks>
        internal Dictionary<string, ChooserDefinition> ChooserDefinitions { get; }

        /// <summary>
        /// Filename of the current model config DB
        /// </summary>
        public string CurrentConfigDB { get; set; }

        /// <summary>
        /// Processing options
        /// </summary>
        public ModelConfigDbUpdaterOptions Options { get; }

        /// <summary>
        /// Utility queries (aka ad hoc queries) loaded from ad_hoc_query.db
        /// </summary>
        /// <remarks>Only used when validating field names</remarks>
        internal Dictionary<string, UtilityQueryDefinition> UtilityQueryDefinitions { get; }

        /// <summary>
        /// This object tracks three dictionaries used to track references between page families
        /// </summary>
        internal CachedNameContainer ValidationNameCache { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public ModelConfigDbUpdater(ModelConfigDbUpdaterOptions options)
        {
            Options = options;

            CurrentConfigDB = string.Empty;

            ChooserDefinitions = new Dictionary<string, ChooserDefinition>(StringComparer.OrdinalIgnoreCase);

            UtilityQueryDefinitions = new Dictionary<string, UtilityQueryDefinition>(StringComparer.OrdinalIgnoreCase);

            ValidationNameCache = new CachedNameContainer();

            mViewColumnNameMap = new Dictionary<string, Dictionary<string, ColumnNameInfo>>(StringComparer.OrdinalIgnoreCase);

            mViewNameMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            mViewNameMapWithSchema = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Return "s" if the count is not one, otherwise return an empty string
        /// </summary>
        /// <param name="itemCount"></param>
        internal static string CheckPlural(int itemCount)
        {
            return itemCount == 1 ? string.Empty : "s";
        }

        private bool ColumnRenamed(GeneralParameters.ParameterType viewType, string viewName, string currentColumnName, out string columnNameToUse, bool snakeCaseName = false)
        {
            string sourceViewDescription;

            switch (viewType)
            {
                case GeneralParameters.ParameterType.DetailReportView:
                case GeneralParameters.ParameterType.DetailReportDataIdColumn:
                case GeneralParameters.ParameterType.DetailReportDataColumns:
                case GeneralParameters.ParameterType.PostSubmissionDetailId:
                    sourceViewDescription = "Detail Report";
                    break;

                case GeneralParameters.ParameterType.EntryPageView:
                case GeneralParameters.ParameterType.EntryPageDataIdColumn:
                case GeneralParameters.ParameterType.EntryPageDataColumns:
                    sourceViewDescription = "Entry Page";
                    break;

                case GeneralParameters.ParameterType.ListReportView:
                case GeneralParameters.ParameterType.ListReportDataColumns:
                case GeneralParameters.ParameterType.ListReportSortColumn:
                    sourceViewDescription = "List Report";
                    break;

                case GeneralParameters.ParameterType.DetailReportSP:
                case GeneralParameters.ParameterType.ListReportSP:
                case GeneralParameters.ParameterType.EntryPageSP:
                case GeneralParameters.ParameterType.OperationsSP:
                    sourceViewDescription = "Stored Procedure";
                    break;

                case GeneralParameters.ParameterType.DatabaseGroup:
                    sourceViewDescription = "Database Group";
                    break;

                default:
                    sourceViewDescription = "??";
                    break;
            }

            return ColumnRenamed(sourceViewDescription, viewName, currentColumnName, out columnNameToUse, snakeCaseName);
        }

        private bool ColumnRenamed(string sourceViewDescription, string viewName, string currentColumnName, out string columnNameToUse, bool snakeCaseName = false)
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

            if (float.TryParse(currentColumnName, out _))
            {
                // Number; leave as-is
                columnNameToUse = currentColumnName;
                return false;
            }

            if (!TryGetColumnMap(viewName, sourceViewDescription, out var columnMap))
            {
                // View name not defined in the View column map file
                columnNameToUse = UpdateColumnName(currentColumnName, snakeCaseName);

                return !currentColumnName.Equals(columnNameToUse);
            }

            if (!columnMap.TryGetValue(currentColumnName, out var columnInfo))
            {
                // Column not defined in the View column map file
                columnNameToUse = UpdateColumnName(currentColumnName, snakeCaseName);

                return !currentColumnName.Equals(columnNameToUse);
            }

            columnNameToUse = snakeCaseName && Options.SnakeCaseColumnNames
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

                mValidationResultsWriter = new StreamWriter(new FileStream(resultsFilePath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
                {
                    AutoFlush = true
                };

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
        internal string GetCleanFieldName(string fieldName, out string prefix)
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
                    if (mViewColumnNameMap.TryGetValue(item.SourceTableName, out var existingRenamedColumnInfo))
                    {
                        // Merge the renamed column list
                        foreach (var renamedColumn in renamedColumns)
                        {
                            if (existingRenamedColumnInfo.TryGetValue(renamedColumn.Key, out var existingInfo))
                            {
                                if (!existingInfo.NewColumnName.Equals(renamedColumn.Value.NewColumnName))
                                {
                                    OnWarningEvent("Multiple synonyms are defined for column {0} in the input files: {1} vs. {2}",
                                        existingInfo.SourceColumnName, existingInfo.NewColumnName, renamedColumn.Value.NewColumnName);
                                }
                            }
                            else
                            {
                                existingRenamedColumnInfo.Add(renamedColumn.Key, renamedColumn.Value);
                            }
                        }
                    }
                    else
                    {
                        mViewColumnNameMap.Add(item.SourceTableName, renamedColumns);
                    }
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
        /// Handle a validation result debug message
        /// </summary>
        /// <param name="message"></param>
        private void OnValidationResultsDebugEvent(string message)
        {
            mValidationResultsWriter?.WriteLine("  " + message);

            if (Options.QuietMode)
                mValidationResultsWriter?.WriteLine();
        }

        /// <summary>
        /// Handle a validation result error message
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
        /// Handle a validation result message
        /// </summary>
        /// <param name="message"></param>
        private void OnValidationResultsStatusEvent(string message)
        {
            mValidationResultsWriter?.WriteLine(message);

            if (message.StartsWith("Skipping validation of stored procedure arguments"))
                mValidationResultsWriter?.WriteLine();
        }

        /// <summary>
        /// Handle a validation result warning
        /// </summary>
        /// <param name="message"></param>
        private void OnValidationResultsWarningEvent(string message)
        {
            mValidationResultsWriter?.WriteLine(message);
            mValidationResultsWriter?.WriteLine();
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

            if (objectName.Trim().Equals("*"))
            {
                // Asterisk; leave as-is
                return objectName;
            }

            if (objectName.StartsWith("\"") && objectName.EndsWith("\""))
            {
                // Name already surrounded with double quotes; leave as-is
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

                if (Options.ValidateColumnNamesWithDatabase)
                {
                    var chooserDefinitionFile = new FileInfo(Path.Combine(inputDirectory.FullName, "dms_chooser.db"));

                    if (chooserDefinitionFile.Exists)
                    {
                        var choosersLoaded = ReadChooserDefinitions(chooserDefinitionFile);

                        if (!choosersLoaded)
                            return false;
                    }
                    else
                    {
                        OnWarningEvent("Chooser definition file not found; cannot validate chooser names");
                        OnDebugEvent("Expected file path: " + chooserDefinitionFile.FullName);
                    }

                    var adHocQueryDefinitionFile = new FileInfo(Path.Combine(inputDirectory.FullName, "ad_hoc_query.db"));

                    if (adHocQueryDefinitionFile.Exists)
                    {
                        var utilityQueriesLoaded = ReadUtilityQueryDefinitions(adHocQueryDefinitionFile);

                        if (!utilityQueriesLoaded)
                            return false;
                    }
                    else
                    {
                        OnWarningEvent("Ad hoc query definition file not found; cannot validate ad-hoc query names");
                        OnDebugEvent("Expected file path: " + adHocQueryDefinitionFile.FullName);
                    }

                    if (Options.SaveValidateResultsToFile && !CreateValidationResultsFile(inputDirectory))
                    {
                        return false;
                    }
                }

                var filesProcessed = 0;
                var lastProgress = DateTime.UtcNow;

                // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                foreach (var modelConfigDb in filesToProcess)
                {
                    var success = ProcessFile(modelConfigDb);

                    if (!success)
                        return false;

                    filesProcessed++;
                    if (DateTime.UtcNow.Subtract(lastProgress).TotalSeconds < 3)
                        continue;

                    var percentComplete = (double)filesProcessed / filesToProcess.Count * 100;

                    Console.WriteLine();
                    OnStatusEvent("{0:F0}% complete; {1} / {2} processed", percentComplete, filesProcessed, filesToProcess.Count);

                    lastProgress = DateTime.UtcNow;
                }

                if (Options.ValidateColumnNamesWithDatabase)
                {
                    ValidateExternalSources();

                    if (filesProcessed >= 100)
                    {
                        // Only validate choosers if at least 100 files were processed
                        ValidateFormFieldChooserUsage();
                        ValidateListReportHelperUsage();
                    }

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

        /// <summary>
        /// Read chooser definitions from the dms_chooser.db file
        /// </summary>
        /// <param name="chooserDefinitionFile"></param>
        /// <returns>True if successful, false if an error</returns>
        private bool ReadChooserDefinitions(FileSystemInfo chooserDefinitionFile)
        {
            ChooserDefinitions.Clear();

            try
            {
                var connectionString = "Data Source=" + chooserDefinitionFile.FullName + "; Version=3; DateTimeFormat=Ticks; Read Only=False;";

                var chooserDbReader = new SQLiteConnection(connectionString, true);

                try
                {
                    chooserDbReader.Open();
                }
                catch (Exception ex)
                {
                    OnErrorEvent("Error opening SQLite database " + chooserDefinitionFile.Name, ex);
                    return false;
                }

                if (!SQLiteUtilities.TableExists(chooserDbReader, "chooser_definitions"))
                {
                    OnWarningEvent("File {0} does not have table chooser_definitions", chooserDefinitionFile.FullName);
                    return false;
                }

                using var dbCommand = chooserDbReader.CreateCommand();

                dbCommand.CommandText = "SELECT id, name, db, type, value FROM chooser_definitions";

                using var reader = dbCommand.ExecuteReader();

                while (reader.Read())
                {
                    var id = SQLiteUtilities.GetInt32(reader, "id");
                    var chooserName = SQLiteUtilities.GetString(reader, "name");
                    var database = SQLiteUtilities.GetString(reader, "db");
                    var chooserType = SQLiteUtilities.GetString(reader, "type");
                    var value = SQLiteUtilities.GetString(reader, "value");

                    var chooser = new ChooserDefinition(id, chooserName, database, chooserType)
                    {
                        Value = value
                    };

                    ChooserDefinitions.Add(chooserName, chooser);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReadChooserDefinitions", ex);
                return false;
            }
        }

        internal bool ReadExternalSources(out List<ExternalSourceInfo> externalSources)
        {
            externalSources = new List<ExternalSourceInfo>();

            try
            {
                if (!SQLiteUtilities.TableExists(mDbConnectionReader, "external_sources"))
                    return true;

                using var dbCommand = mDbConnectionReader.CreateCommand();

                dbCommand.CommandText = "SELECT id, field, source_page, type, value FROM external_sources";

                using var reader = dbCommand.ExecuteReader();

                while (reader.Read())
                {
                    var id = SQLiteUtilities.GetInt32(reader, "id");
                    var formField = SQLiteUtilities.GetString(reader, "field");
                    var sourcePage = SQLiteUtilities.GetString(reader, "source_page");
                    var sourceDataType = SQLiteUtilities.GetString(reader, "type");
                    var sourcePageColumn = SQLiteUtilities.GetString(reader, "value");

                    externalSources.Add(new ExternalSourceInfo(id, formField, sourcePage, sourcePageColumn, sourceDataType));
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

                dbCommand.CommandText = "SELECT id, name, label, type FROM form_fields";

                using var reader = dbCommand.ExecuteReader();

                while (reader.Read())
                {
                    var id = SQLiteUtilities.GetInt32(reader, "id");
                    var formField = SQLiteUtilities.GetString(reader, "name");
                    var label = SQLiteUtilities.GetString(reader, "label");
                    var type = SQLiteUtilities.GetString(reader, "type");

                    formFields.Add(new FormFieldInfo(id, formField, label, type));
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

                dbCommand.CommandText = "SELECT id, field, type, PickListName, Target, XRef FROM form_field_choosers";

                using var reader = dbCommand.ExecuteReader();

                while (reader.Read())
                {
                    var id = SQLiteUtilities.GetInt32(reader, "id");
                    var formField = SQLiteUtilities.GetString(reader, "field");
                    var chooserType = SQLiteUtilities.GetString(reader, "type");
                    var pickListName = SQLiteUtilities.GetString(reader, "PickListName");
                    var helperName = SQLiteUtilities.GetString(reader, "Target");
                    var crossReference = SQLiteUtilities.GetString(reader, "XRef");

                    var chooser = new FormFieldChooserInfo(id, formField, crossReference)
                    {
                        Type = chooserType,
                        PickListName = pickListName,
                        ListReportHelperName = helperName
                    };

                    formFieldChoosers.Add(chooser);
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

        internal List<HotLinkInfo> ReadHotlinks(string tableName)
        {
            var hotlinks = new List<HotLinkInfo>();

            if (!SQLiteUtilities.TableExists(mDbConnectionReader, tableName))
            {
                return hotlinks;
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

                hotlinks.Add(new HotLinkInfo(id, fieldName, linkType, whichArg));
            }

            return hotlinks;
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

                dbCommand.CommandText = "SELECT id, field, name, procedure FROM sproc_args";

                using var reader = dbCommand.ExecuteReader();

                while (reader.Read())
                {
                    var id = SQLiteUtilities.GetInt32(reader, "id");
                    var formField = SQLiteUtilities.GetString(reader, "field");
                    var argumentName = SQLiteUtilities.GetString(reader, "name");
                    var procedure = SQLiteUtilities.GetString(reader, "procedure");

                    storedProcedureArguments.Add(new StoredProcArgumentInfo(id, formField, argumentName, procedure));
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReadStoredProcedureArguments", ex);
                return false;
            }
        }

        private bool ReadUtilityQueryDefinitions(FileSystemInfo adHocQueryDefinitionFile)
        {
            UtilityQueryDefinitions.Clear();

            try
            {
                var connectionString = "Data Source=" + adHocQueryDefinitionFile.FullName + "; Version=3; DateTimeFormat=Ticks; Read Only=False;";

                var utilityQueryReader = new SQLiteConnection(connectionString, true);

                try
                {
                    utilityQueryReader.Open();
                }
                catch (Exception ex)
                {
                    OnErrorEvent("Error opening SQLite database " + adHocQueryDefinitionFile.Name, ex);
                    return false;
                }

                if (!SQLiteUtilities.TableExists(utilityQueryReader, "utility_queries"))
                {
                    OnWarningEvent("File {0} does not have table utility_queries", adHocQueryDefinitionFile.FullName);
                    return false;
                }

                using var dbCommand = utilityQueryReader.CreateCommand();

                dbCommand.CommandText = "SELECT id, name, label, db, \"table\", columns FROM utility_queries";

                using var reader = dbCommand.ExecuteReader();

                while (reader.Read())
                {
                    var id = SQLiteUtilities.GetInt32(reader, "id");
                    var queryName = SQLiteUtilities.GetString(reader, "name");
                    var label = SQLiteUtilities.GetString(reader, "label");
                    var database = SQLiteUtilities.GetString(reader, "db");
                    var table = SQLiteUtilities.GetString(reader, "table");
                    var columns = SQLiteUtilities.GetString(reader, "columns");

                    var utilityQuery = new UtilityQueryDefinition(id, queryName)
                    {
                        Label = label,
                        Database = database,
                        Table = table,
                        Columns = columns
                    };

                    UtilityQueryDefinitions.Add(queryName, utilityQuery);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ReadUtilityQueryDefinitions", ex);
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

                if (ColumnRenamed("Entry Page", viewNameToUse, generalParams.Parameters[GeneralParameters.ParameterType.EntryPageDataIdColumn], out var dataIdNameToUse, true))
                {
                    UpdateGeneralParameter(generalParams, GeneralParameters.ParameterType.EntryPageDataIdColumn, dataIdNameToUse);
                }

                if (!string.IsNullOrWhiteSpace(generalParams.Parameters[GeneralParameters.ParameterType.EntryPageDataColumns]))
                {
                    UpdateEntryPageDataColumns(generalParams);
                }

                if (ColumnRenamed("Entry Page", viewNameToUse, generalParams.Parameters[GeneralParameters.ParameterType.PostSubmissionDetailId], out var postSubmissionNameToUse, true))
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

                if (ColumnRenamed("Detail Report", viewNameToUse, generalParams.Parameters[GeneralParameters.ParameterType.DetailReportDataIdColumn], out var columnNameToUse))
                {
                    UpdateGeneralParameter(generalParams, GeneralParameters.ParameterType.DetailReportDataIdColumn, columnNameToUse);
                }

                if (!string.IsNullOrWhiteSpace(generalParams.Parameters[GeneralParameters.ParameterType.DetailReportDataColumns]))
                {
                    UpdateDetailReportDataColumns(generalParams);
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

                var listReportSortColumns = new List<string>();
                var storeNewValue = false;

                foreach (var value in generalParams.Parameters[GeneralParameters.ParameterType.ListReportSortColumn].Split(','))
                {
                    if (ColumnRenamed("List Report", viewNameToUse, value.Trim(), out var columnNameToUse))
                    {
                        listReportSortColumns.Add(columnNameToUse);
                        storeNewValue = true;
                    }
                    else
                    {
                        listReportSortColumns.Add(value.Trim());
                    }
                }

                if (storeNewValue)
                {
                    var updatedSortColumns = string.Join(", ", listReportSortColumns);

                    UpdateGeneralParameter(generalParams, GeneralParameters.ParameterType.ListReportSortColumn, updatedSortColumns);
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
                if (Options.RenameUndefinedViews)
                {
                    // Snake case the name since Perl script sqlserver2pgsql.pl renames all views to snake case
                    updatedName = ConvertToSnakeCaseAndUpdatePrefix(currentName);
                }
                else
                {
                    updatedName = currentName;
                }
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
                OnValidationResultsStatusEvent(formattedMessage);
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
                OnValidationResultsWarningEvent(formattedMessage);
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
            var trimmedName = objectName.Trim();

            if (trimmedName.StartsWith("[") && trimmedName.EndsWith("]"))
            {
                return trimmedName.Substring(1, trimmedName.Length - 2);
            }

            if (trimmedName.StartsWith("\"") && trimmedName.EndsWith("\""))
            {
                return trimmedName.Substring(1, trimmedName.Length - 2);
            }

            return objectName;
        }

        private bool TryGetColumnMap(string viewName, string sourceViewDescription, out Dictionary<string, ColumnNameInfo> columnMap)
        {
            if (string.IsNullOrWhiteSpace(viewName))
            {
                OnStatusEvent("Cannot check for explicit column renames since {0} view not defined", sourceViewDescription);
                Console.WriteLine();

                columnMap = new Dictionary<string, ColumnNameInfo>();
                return false;
            }

            if (mViewNameMap.TryGetValue(viewName, out var nameWithSchema))
            {
                columnMap = mViewColumnNameMap[nameWithSchema];
                return true;
            }

            var viewFound = mViewColumnNameMap.TryGetValue(viewName, out columnMap);

            if (viewFound || mMissingViews.Contains(viewName))
                return viewFound;

            var message = string.Format("Cannot check for explicit column renames since view not found in mViewColumnNameMap: {0}", viewName);

            if (Options.SnakeCaseColumnNames)
            {
                OnWarningEvent("{0}\n{1}{2}",
                    message,
                    "Column names will be converted to lowercase snake case",
                    Options.ReplaceSpacesWithUnderscores ? " after replacing spaces with underscores" : string.Empty);
            }
            else
            {
                OnWarningEvent("{0}{1}",
                    message,
                    Options.ReplaceSpacesWithUnderscores ? "\nSpaces in column names will be replaced with underscores" : string.Empty);
            }

            Console.WriteLine();

            mMissingViews.Add(viewName);

            return false;
        }

        private string UpdateColumnName(string currentColumnName, bool snakeCaseName)
        {
            var columnNameToUse = Options.ReplaceSpacesWithUnderscores
                ? currentColumnName.Replace(' ', '_')
                : currentColumnName;

            if (!snakeCaseName || !Options.SnakeCaseColumnNames)
                return columnNameToUse;

            return ConvertToSnakeCaseAndUpdatePrefix(currentColumnName);
        }

        private void UpdateDetailReportDataColumns(GeneralParameters generalParams)
        {
            UpdateListOfDataColumns(generalParams, GeneralParameters.ParameterType.DetailReportDataColumns, GeneralParameters.ParameterType.DetailReportView, false);
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

                UpdateHotlinks("Detail Report", detailReportView, DB_TABLE_DETAIL_REPORT_HOTLINKS, detailReportHotlinks);
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
                    if (!ColumnRenamed("Entry Page", entryPageView, formField.FieldName, out var columnNameToUse, true))
                        continue;

                    formField.NewFieldName = columnNameToUse;
                    renamedFormFields.Add(formField.FieldName, formField);
                }

                if (Options.PreviewUpdates)
                {
                    OnStatusEvent(
                        "{0,-25} Would rename {1} form field{2}", CurrentConfigDB + ":",
                        renamedFormFields.Count, CheckPlural(renamedFormFields.Count));

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
                    renamedFormFields.Count, CheckPlural(renamedFormFields.Count));

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
                    updatedItems, CheckPlural(updatedItems));

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
                    updatedItems, CheckPlural(updatedItems));

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
                    updatedItems, CheckPlural(updatedItems));

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
                    updatedItems, CheckPlural(updatedItems));
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

        private void UpdateHotlinks(string sourceViewDescription, string sourceView, string tableName, List<HotLinkInfo> hotlinks)
        {
            foreach (var item in hotlinks)
            {
                var originalColumnName = GetCleanFieldName(item.FieldName, out var prefix);

                if (ColumnRenamed(sourceViewDescription, sourceView, originalColumnName, out var columnNameToUse))
                {
                    item.NewFieldName = prefix + columnNameToUse;
                    item.Updated = true;
                }

                // List report hotlinks often use "value" to indicate that the value to use in the link is the same column that the hotlink appears in
                // Hotlinks with an empty string for WhichArg include color_label and literal_link

                if (string.IsNullOrWhiteSpace(item.WhichArg) ||
                    tableName.Equals(DB_TABLE_LIST_REPORT_HOTLINKS) && item.WhichArg.Equals("value", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                // ReSharper disable once InvertIf
                if (ColumnRenamed(sourceViewDescription, sourceView, item.WhichArg, out var targetColumnToUse))
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
                updatedItems, CheckPlural(updatedItems), sourceView);
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
                        aliasNameToUse = ColumnRenamed("Entry Page", generalParams.Parameters[viewType], aliasName, out var newAliasName, snakeCaseNames)
                            ? newAliasName
                            : aliasName;
                    }
                    else
                    {
                        aliasNameToUse = aliasName;
                    }

                    var columnNameToUse = ColumnRenamed(viewType, generalParams.Parameters[viewType], columnNameToFind, out var newColumnName, snakeCaseNames)
                        ? newColumnName
                        : columnNameToFind;

                    if (string.IsNullOrWhiteSpace(aliasNameToUse))
                    {
                        updatedColumns.Add(PossiblyQuoteName(columnNameToUse, Options.QuoteWithSquareBrackets));
                    }
                    else
                    {
                        var nameWithAlias = PossiblyQuoteName(columnNameToUse, Options.QuoteWithSquareBrackets) + " AS " + aliasNameToUse;
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

                UpdateHotlinks("List Report", listReportView, DB_TABLE_LIST_REPORT_HOTLINKS, listReportHotlinks);
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
                if (ColumnRenamed("List Report", sourceView, item.FieldName, out var columnNameToUse))
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
                updatedItems, CheckPlural(updatedItems), sourceView);
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
                    validator.StatusEvent += OnValidationResultsStatusEvent;
                    validator.ErrorEvent += OnValidationResultsErrorEvent;
                    validator.WarningEvent += OnValidationResultsWarningEvent;
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

        private void ValidateExternalSources()
        {
            try
            {
                foreach (var pageFamily in ValidationNameCache.ExternalSourceReferences)
                {
                    foreach (var sourcePage in pageFamily.Value.ExternalSources)
                    {
                        var externalPageFamily = sourcePage.Key;

                        if (!ValidationNameCache.DatabaseColumnsByPageFamily.TryGetValue(externalPageFamily, out var externalPageFamilyInfo))
                        {
                            if (externalPageFamily.Equals("predefined_analysis_preview") ||
                                externalPageFamily.Equals("predefined_analysis_preview_mds"))
                            {
                                Console.WriteLine();
                                OnStatusEvent(
                                    "Ignoring missing external source referenced by page family '{0}' since expected: {1}",
                                    pageFamily.Key, externalPageFamily);

                                continue;
                            }

                            ShowWarning(
                                "Page family '{0}' not found in the validation name cache; cannot validate external sources for page family '{1}'",
                                externalPageFamily, pageFamily.Key);

                            continue;
                        }

                        // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                        foreach (var sourceColumn in sourcePage.Value)
                        {
                            var matchFound = externalPageFamilyInfo.DatabaseColumnNames.Any(databaseObject => databaseObject.Value.Contains(sourceColumn));

                            if (!matchFound)
                            {
                                ShowWarning(
                                    "Page family '{0}' references column '{1}' in page family '{2}', but the column was not found in the validation name cache",
                                    pageFamily.Key, sourceColumn, externalPageFamily);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateExternalSources", ex);
            }
        }

        private void ValidateFormFieldChooserUsage()
        {
            try
            {
                var unusedChoosers = new SortedSet<string>();

                foreach (var chooser in ChooserDefinitions)
                {
                    if (!ValidationNameCache.PickListChooserUsage.TryGetValue(chooser.Key, out var chooserUsage) || chooserUsage == 0)
                    {
                        unusedChoosers.Add(chooser.Key);
                    }
                }

                if (unusedChoosers.Count > 0)
                {
                    ShowWarning("Pick list choosers not used by any of the validated model config DBs:");
                    ShowWarning("  {0}", string.Join("\n  ", unusedChoosers));
                }

                foreach (var chooser in ValidationNameCache.PickListChooserUsage)
                {
                    if (!ChooserDefinitions.ContainsKey(chooser.Key))
                    {
                        ShowWarning(
                            "One of the validated model config DBs referenced pick list chooser {0}, " +
                            "but that chooser is not defined in dms_chooser.db", chooser.Key);
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateFormFieldChooserUsage", ex);
            }
        }

        private void ValidateListReportHelperUsage()
        {
            try
            {
                var unusedHelpers = new SortedSet<string>();

                var inputDirectory = new DirectoryInfo(Options.InputDirectory);
                var listReportHelpers = new SortedSet<string>();

                foreach (var helperFile in inputDirectory.GetFiles("helper_*.db"))
                {
                    listReportHelpers.Add(Path.GetFileNameWithoutExtension(helperFile.Name));
                }

                foreach (var helperName in listReportHelpers)
                {
                    if (!ValidationNameCache.ListReportHelperUsage.TryGetValue(helperName, out var helperUsage) || helperUsage == 0)
                    {
                        unusedHelpers.Add(helperName);
                    }
                }

                if (unusedHelpers.Count > 0)
                {
                    ShowWarning("List report helpers not used by any of the validated model config DBs:");
                    ShowWarning("  {0}", string.Join("\n  ", unusedHelpers));
                }

                foreach (var helper in ValidationNameCache.ListReportHelperUsage)
                {
                    if (!listReportHelpers.Contains(helper.Key))
                    {
                        ShowWarning(
                            "One of the validated model config DBs referenced list report helper {0}, " +
                            "but that helper is not a page family on the DMS website (no .db file)", helper.Key);
                    }
                }
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateListReportHelperUsage", ex);
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
