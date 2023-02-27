using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PRISM;
using PRISMDatabaseUtils;

namespace DMSModelConfigDbUpdater
{
    internal class ModelConfigDbValidator : EventNotifier
    {
        // Ignore Spelling: citext, dbo, dms, gigasax, hotlink, hotlinks, Levenshtein, lr
        // Ignore Spelling: Postgres, proteinseqs, Proc, Sel, wellplate

        /// <summary>
        /// This RegEx matches form field choosers that reference an ad hoc query
        /// </summary>
        private readonly Regex mAdHocQueryMatcher = new("ad_hoc_query/(?<QueryName>[^/]+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Match any character that is not a letter, number, or underscore
        /// </summary>
        private readonly Regex mColumnCharNonStandardMatcher = new("[^a-z0-9_]", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

        /// <summary>
        /// Keys in this dictionary are database names
        /// Values are an instance of DatabaseColumnInfo
        /// </summary>
        private readonly Dictionary<string, DatabaseColumnInfo> mDatabaseColumns;

        /// <summary>
        /// Model Config DB Updater instance
        /// </summary>
        private readonly ModelConfigDbUpdater mDbUpdater;

        /// <summary>
        /// Form fields
        /// </summary>
        private readonly List<FormFieldInfo> mFormFields;

        /// <summary>
        /// General parameters
        /// </summary>
        private readonly GeneralParameters mGeneralParams;

        /// <summary>
        /// Keys in this dictionary are model config file names (without the file extension)
        /// Values are dictionaries where keys are source view names and values are a list of known missing columns
        /// </summary>
        private readonly Dictionary<string, Dictionary<string, List<string>>> mMissingColumnsToIgnore;

        /// <summary>
        /// Processing options
        /// </summary>
        private readonly ModelConfigDbUpdaterOptions mOptions;

        /// <summary>
        /// Current page family name
        /// </summary>
        private string CurrentPageFamily => Path.GetFileNameWithoutExtension(mDbUpdater.CurrentConfigDB);

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dbUpdater">Model Config DB Updater instance</param>
        /// <param name="generalParams">General parameters</param>
        /// <param name="formFields">Form fields</param>
        public ModelConfigDbValidator(ModelConfigDbUpdater dbUpdater, GeneralParameters generalParams, List<FormFieldInfo> formFields)
        {
            mDatabaseColumns = new Dictionary<string, DatabaseColumnInfo>(StringComparer.OrdinalIgnoreCase);
            mDbUpdater = dbUpdater;
            mFormFields = formFields;
            mGeneralParams = generalParams;
            mMissingColumnsToIgnore = new Dictionary<string, Dictionary<string, List<string>>>(StringComparer.OrdinalIgnoreCase);
            mOptions = mDbUpdater.Options;

            DefineMissingColumnsToIgnore();
        }

        /// <summary>
        /// Define form field names that do not need to correspond to a column in the source view
        /// </summary>
        private void DefineMissingColumnsToIgnore()
        {
            mMissingColumnsToIgnore.Add(
                "analysis_group",
                GetMissingColumnDictionary("v_analysis_job_entry", new List<string>
                {
                    "remove_datasets_with_jobs",
                    "data_package_id",
                    "request"
                }));

            mMissingColumnsToIgnore.Add(
                "experiment_fraction",
                GetMissingColumnDictionary("v_experiment_fractions_entry", new List<string>
                {
                    "suffix",
                    "name_search",
                    "name_replace",
                    "group_name",
                    "add_underscore_before_fraction_num",
                    "request_override",
                    "internal_standard",
                    "postdigest_int_std",
                    "researcher",
                    "container",
                    "wellplate",
                    "well",
                    "prep_lc_run_id"
                }));

            mMissingColumnsToIgnore.Add(
                "mrm_list_attachment",
                GetMissingColumnDictionary("v_mrm_list_attachment_list_report", new List<string>
                {
                    "download"
                }));

            mMissingColumnsToIgnore.Add(
                "param_file",
                GetMissingColumnDictionary("v_param_file_entry", new List<string>
                {
                    "replace_mass_mods",
                    "validate_unimod"
                }));

            mMissingColumnsToIgnore.Add(
                "requested_run_group",
                GetMissingColumnDictionary("v_requested_run_entry", new List<string>
                {
                    "experiment_group_id",
                    "request_name_suffix",
                    "type",
                    "requester",
                    "experiment_list",
                    "batch_name",
                    "batch_description",
                    "batch_completion_date",
                    "batch_priority",
                    "batch_priority_justification",
                    "batch_comment"
                }));

            mMissingColumnsToIgnore.Add(
                "update_analysis_jobs",
                GetMissingColumnDictionary("t_analysis_job", new List<string>
                {
                    "job_list",
                    "state",
                    "priority",
                    "comment",
                    "find_text",
                    "replace_text",
                    "assigned_processor",
                    "associated_processor_group",
                    "propagation_mode",
                    "param_file",
                    "settings_file",
                    "organism",
                    // ReSharper disable StringLiteralTypo
                    "prot_coll_name_list",
                    "prot_coll_options_list",
                    // ReSharper restore StringLiteralTypo
                }));
        }

        private bool GetColumnNamesInTableOrView(string tableOrViewName, out SortedSet<string> columnNames, out string targetDatabase)
        {
            columnNames = new SortedSet<string>();
            targetDatabase = GetTargetDatabase();

            try
            {
                if (!mDatabaseColumns.ContainsKey(targetDatabase))
                {
                    var success = RetrieveDatabaseColumnInfo(targetDatabase);

                    if (!success)
                        return false;
                }

                string schemaNameToFind;
                string nameWithoutSchema;

                if (tableOrViewName.Contains("."))
                {
                    schemaNameToFind = PossiblyUnquote(ModelConfigDbUpdater.GetSchemaName(tableOrViewName));
                    nameWithoutSchema = PossiblyUnquote(ModelConfigDbUpdater.GetNameWithoutSchema(tableOrViewName));
                }
                else
                {
                    schemaNameToFind = mOptions.UsePostgresSchema ? "public" : "dbo";
                    nameWithoutSchema = PossiblyUnquote(tableOrViewName);
                }

                var databaseColumnInfo = mDatabaseColumns[targetDatabase];

                columnNames = databaseColumnInfo.GetColumnsForTableOrView(schemaNameToFind, nameWithoutSchema);

                return columnNames.Count > 0;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in GetColumnNamesInTableOrView", ex);
                return false;
            }
        }

        private Dictionary<string, List<string>> GetMissingColumnDictionary(string tableOrView, List<string> columnsToIgnore)
        {
            return new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase)
            {
                { tableOrView, columnsToIgnore }
            };
        }

        internal static string GetTableOrViewDescription(string tableOrViewName, bool capitalizeFirstWord = false, bool storedProcedureDataSource = false)
        {
            string objectType;

            if (storedProcedureDataSource)
            {
                objectType = capitalizeFirstWord ? "Stored procedure" : "stored procedure";
            }
            else if (tableOrViewName.StartsWith("T_", StringComparison.OrdinalIgnoreCase))
            {
                objectType = capitalizeFirstWord ? "Table" : "table";
            }
            else
            {
                objectType = capitalizeFirstWord ? "View" : "view";
            }

            return string.Format("{0} {1}", objectType, tableOrViewName);
        }

        private string GetTargetDatabase()
        {
            if (mOptions.UsePostgresSchema)
                return "dms";

            return mGeneralParams.Parameters[GeneralParameters.ParameterType.DatabaseGroup].ToLower() switch
            {
                "package" => "DMS_Data_Package",
                "ontology" => "Ontology_Lookup",
                "broker" => "DMS_Pipeline",
                "capture" => "DMS_Capture",
                "manager_control" => "Manager_Control",
                _ => "DMS5"
            };
        }

        /// <summary>
        /// Check whether a column name referred to by a form field is allowed to be missing from the referenced table or view
        /// </summary>
        /// <param name="tableOrView"></param>
        /// <param name="columnName"></param>
        /// <returns>True if allowed to be missing, otherwise false</returns>
        private bool IgnoreMissingColumn(string tableOrView, string columnName)
        {
            if (!mMissingColumnsToIgnore.TryGetValue(CurrentPageFamily, out var tablesAndViews))
                return false;

            return tablesAndViews.TryGetValue(tableOrView, out var columnsToIgnore) && columnsToIgnore.Contains(columnName);
        }

        private bool IsOperationsProcedure(string procedureName)
        {
            return mGeneralParams.Parameters.TryGetValue(GeneralParameters.ParameterType.OperationsSP, out var operationsProcedure) &&
                   operationsProcedure.Equals(procedureName, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// If objectName only has letters, numbers, or underscores, remove any double quotes surrounding the name
        /// </summary>
        /// <param name="objectName"></param>
        private string PossiblyUnquote(string objectName)
        {
            var cleanName = objectName.Trim().Trim('"');

            return mColumnCharNonStandardMatcher.IsMatch(cleanName) ? objectName : cleanName;
        }

        /// <summary>
        /// Query the Information_Schema view to obtain the columns for all tables or views in the target database
        /// </summary>
        /// <param name="databaseName"></param>
        /// <returns>True if successful, false if an error</returns>
        private bool RetrieveDatabaseColumnInfo(string databaseName)
        {
            try
            {
                IDBTools dbTools;

                if (mOptions.UsePostgresSchema)
                {
                    var connectionString = DbToolsFactory.GetConnectionString(
                        DbServerTypes.PostgreSQL, mOptions.DatabaseServer, databaseName,
                        "d3l243", string.Empty, "ModelConfigDbValidator");

                    dbTools = DbToolsFactory.GetDBTools(DbServerTypes.PostgreSQL, connectionString);
                }
                else
                {
                    string serverToUse;

                    if (mOptions.DatabaseServer.Equals("gigasax", StringComparison.OrdinalIgnoreCase) &&
                        mGeneralParams.Parameters[GeneralParameters.ParameterType.DatabaseGroup].Equals("manager_control", StringComparison.OrdinalIgnoreCase))
                    {
                        // Auto-change the server since the manager control database is not on Gigasax
                        serverToUse = "proteinseqs";
                    }
                    else
                    {
                        serverToUse = mOptions.DatabaseServer;
                    }

                    string databaseToUse;

                    if (mOptions.UseDevelopmentDatabases)
                    {
                        databaseToUse = databaseName.ToLower() switch
                        {
                            "dms_capture" => "DMS_Capture_T3",
                            "dms_data_package" => "DMS_Data_Package_T3",
                            "dms_pipeline" => "DMS_Pipeline_T3",
                            "dms5" => "dms5_t3",
                            "ontology_lookup" => "Ontology_Lookup",
                            "manager_control" => "Manager_Control_T3",
                            "protein_sequences" => "Protein_Sequences_T3",
                            _ => databaseName
                        };
                    }
                    else
                    {
                        databaseToUse = databaseName;
                    }

                    // SQL Server
                    var connectionString = DbToolsFactory.GetConnectionString(
                        DbServerTypes.MSSQLServer, serverToUse, databaseToUse, "ModelConfigDbValidator");

                    dbTools = DbToolsFactory.GetDBTools(DbServerTypes.MSSQLServer, connectionString);
                }

                // Note that Information_Schema.Columns includes the column names for both the tables and views in a database
                // Postgres includes the information_schema objects in the list, plus also pg_catalog objects, so we exclude them in the query

                var sqlQuery = string.Format(
                    "SELECT table_schema, table_name, column_name " +
                    "FROM Information_Schema.Columns " +
                    "WHERE table_schema Not In ('information_schema', 'pg_catalog') " +
                    "ORDER BY table_schema, table_name, column_name");

                var cmd = dbTools.CreateCommand(sqlQuery);

                dbTools.GetQueryResultsDataTable(cmd, out var queryResults);

                var databaseColumnInfo = new DatabaseColumnInfo(databaseName);
                RegisterEvents(databaseColumnInfo);

                mDatabaseColumns.Add(databaseName, databaseColumnInfo);

                var currentSchema = string.Empty;
                var currentTable = string.Empty;
                var currentColumns = new SortedSet<string>();

                foreach (DataRow result in queryResults.Rows)
                {
                    var schema = result["table_schema"].CastDBVal<string>();
                    var tableOrView = result["table_name"].CastDBVal<string>();
                    var columnName = result["column_name"].CastDBVal<string>();

                    if (!currentSchema.Equals(schema) || !currentTable.Equals(tableOrView))
                    {
                        currentSchema = schema;
                        currentTable = tableOrView;
                        currentColumns = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

                        databaseColumnInfo.AddTableOrView(schema, tableOrView, currentColumns);
                    }

                    currentColumns.Add(columnName);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in RetrieveDatabaseColumnInfo", ex);
                return false;
            }
        }

        private void StoreSourceColumn(
            IDictionary<string, SortedSet<string>> columnsBySourcePage,
            ExternalSourceInfo externalSource,
            ref int errorCount)
        {
            if (string.IsNullOrWhiteSpace(externalSource.SourcePage))
            {
                OnWarningEvent(
                    "{0,-25} External source with ID {1}, type {2} does not have a source page defined",
                    mDbUpdater.CurrentConfigDB + ":",
                    externalSource.ID,
                    externalSource.SourceType);

                errorCount++;
                return;
            }

            if (!externalSource.SourceType.Equals("ColName") &&
                !externalSource.SourceType.Equals("Literal") &&
                !externalSource.SourceType.Equals("PostName") &&
                !externalSource.SourceType.Equals("ColName.action.Scrub") &&
                !externalSource.SourceType.Equals("ColName.action.ExtractUsername") &&
                !externalSource.SourceType.Equals("ColName.action.ExtractEUSId"))
            {
                OnWarningEvent(
                    "{0,-25} External source with ID {1} does not have source type ColName or Literal: {2}",
                    mDbUpdater.CurrentConfigDB + ":",
                    externalSource.ID,
                    externalSource.SourceType);

                errorCount++;
            }

            if (externalSource.SourceType.Equals("Literal", StringComparison.OrdinalIgnoreCase))
            {
                // String value instead of column name
                // This value will be stored in the target form field when populating the entry page with data from the source page
                // Do not store it in columnsBySourcePage
                return;
            }

            if (string.IsNullOrWhiteSpace(externalSource.SourceColumn))
            {
                OnWarningEvent(
                    "{0,-25} External source with ID {1} has an empty source column name and is type {2} instead of Literal; this may be incorrect",
                    mDbUpdater.CurrentConfigDB + ":",
                    externalSource.ID,
                    externalSource.SourceType);

                errorCount++;
                return;
            }

            SortedSet<string> sourceColumns;
            if (columnsBySourcePage.TryGetValue(externalSource.SourcePage, out var sourcePageColumns))
            {
                sourceColumns = sourcePageColumns;
            }
            else
            {
                sourceColumns = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
                columnsBySourcePage.Add(externalSource.SourcePage, sourceColumns);
            }

            if (!sourceColumns.Contains(externalSource.SourceColumn))
            {
                sourceColumns.Add(externalSource.SourceColumn);
            }
        }

        /// <summary>
        /// Examine column names to look for mismatches vs. column names in the referenced tables and views
        /// </summary>
        /// <param name="errorCount">Output: number of errors found</param>
        /// <returns>True if names were validated, false if a critical error</returns>
        public bool ValidateColumnNames(out int errorCount)
        {
            errorCount = 0;

            var validationCompleted = new List<bool>();

            try
            {
                validationCompleted.Add(ValidateFormFieldNames(ref errorCount));

                validationCompleted.Add(ValidateFormFieldChoosers(ref errorCount));

                validationCompleted.Add(ValidateFormFieldOptions(ref errorCount));

                validationCompleted.Add(ValidateExternalSources(ref errorCount));

                validationCompleted.Add(ValidateStoredProcedureArguments(ref errorCount));

                validationCompleted.Add(ValidateListReportHotlinks(ref errorCount));

                validationCompleted.Add(ValidateDetailReportHotlinks(ref errorCount));

                if (errorCount > 0)
                {
                    OnWarningEvent(
                        "{0} error{1} found in file {2}",
                        errorCount,
                        ModelConfigDbUpdater.CheckPlural(errorCount),
                        mDbUpdater.CurrentConfigDB);
                }

                // Return true if every item in validationCompleted is true
                return validationCompleted.All(item => item);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateColumnNames", ex);
                return false;
            }
        }

        /// <summary>
        /// Validate detail report column names vs. the source view
        /// </summary>
        /// <param name="errorCount"></param>
        /// <returns>True if names were validated, false if a critical error</returns>
        private bool ValidateDetailReportHotlinks(ref int errorCount)
        {
            try
            {
                var detailReportHotlinks = mDbUpdater.ReadHotlinks(ModelConfigDbUpdater.DB_TABLE_DETAIL_REPORT_HOTLINKS);

                return ValidateHotlinks(GeneralParameters.ParameterType.DetailReportView, detailReportHotlinks, ref errorCount, true);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateDetailReportHotlinks", ex);
                return false;
            }
        }

        private void ValidateBasicField(string parentDescription, BasicField field, ref int errorCount, out bool emptyFieldName)
        {
            if (string.IsNullOrWhiteSpace(field.FieldName))
            {
                if (field is StoredProcArgumentInfo storedProcedureArgument)
                {
                    OnWarningEvent(
                        "{0,-25} {1} {2} does not have a form field name defined",
                        mDbUpdater.CurrentConfigDB + ":",
                        parentDescription,
                        storedProcedureArgument.ArgumentName);
                }
                else
                {
                    OnWarningEvent(
                        "{0,-25} {1} with ID {2} does not have a form field name defined",
                        mDbUpdater.CurrentConfigDB + ":",
                        parentDescription,
                        field.ID);
                }

                errorCount++;
                emptyFieldName = true;
                return;
            }

            emptyFieldName = false;

            ValidateFieldNameVsFormFields(parentDescription, field.FieldName, ref errorCount);
        }

        /// <summary>
        /// Validate external sources
        /// </summary>
        /// <param name="errorCount"></param>
        /// <returns>True if names were validated, false if a critical error</returns>
        private bool ValidateExternalSources(ref int errorCount)
        {
            try
            {
                var externalSourcesLoaded = mDbUpdater.ReadExternalSources(out var externalSources);
                if (!externalSourcesLoaded)
                    return true;

                var columnsBySourcePage = new Dictionary<string, SortedSet<string>>(StringComparer.OrdinalIgnoreCase);

                foreach (var externalSource in externalSources)
                {
                    ValidateBasicField("External source", externalSource, ref errorCount, out _);

                    StoreSourceColumn(columnsBySourcePage, externalSource, ref errorCount);
                }

                if (columnsBySourcePage.Count == 0)
                    return true;

                // Cache the external source columns

                ExternalSourceColumnInfo cachedSourcePageInfo;

                if (mDbUpdater.ValidationNameCache.ExternalSourceReferences.TryGetValue(CurrentPageFamily, out var existingSourceInfo))
                {
                    cachedSourcePageInfo = existingSourceInfo;
                }
                else
                {
                    cachedSourcePageInfo = new ExternalSourceColumnInfo(CurrentPageFamily);
                    mDbUpdater.ValidationNameCache.ExternalSourceReferences.Add(CurrentPageFamily, cachedSourcePageInfo);
                }

                foreach (var sourcePage in columnsBySourcePage)
                {
                    cachedSourcePageInfo.AddExternalSourceColumns(sourcePage.Key, sourcePage.Value);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateExternalSources", ex);
                return false;
            }
        }

        private void ValidateFieldNameVsFormFields(string parentDescription, string fieldName, ref int errorCount)
        {
            if (string.IsNullOrWhiteSpace(fieldName))
                return;

            var matchingFormField = string.Empty;

            // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
            foreach (var formField in mFormFields)
            {
                if (!fieldName.Equals(formField.FieldName, StringComparison.OrdinalIgnoreCase))
                    continue;

                matchingFormField = formField.FieldName;
                break;
            }

            if (matchingFormField.Length > 0)
            {
                if (matchingFormField.Equals(fieldName))
                    return;

                OnWarningEvent(
                    "{0,-25} {1} {2} has a mismatched case vs. the actual form field: {3}",
                    mDbUpdater.CurrentConfigDB + ":",
                    parentDescription,
                    fieldName,
                    matchingFormField);

                errorCount++;
                return;
            }

            if (fieldName.Equals("<local>") || fieldName.Equals("infoOnly", StringComparison.OrdinalIgnoreCase))
            {
                // Ignore arguments named <local> and infoOnly for stored procedure arguments
                return;
            }

            if (mDbUpdater.CurrentConfigDB.Equals("requested_run_batch_blocking.db") && fieldName.Equals("itemList"))
            {
                // This is a parameter on stored procedure GetRequestedRunParametersAndFactors; skip validation
                return;
            }

            var closestMatches = new List<string>();
            var closestMatchDistance = int.MaxValue;

            // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
            foreach (var formField in mFormFields)
            {
                var distance = LevenshteinDistance.GetDistance(fieldName, formField.FieldName);

                if (distance < closestMatchDistance)
                {
                    closestMatches.Clear();
                    closestMatches.Add(formField.FieldName);
                    closestMatchDistance = distance;
                }
                else if (distance == closestMatchDistance)
                {
                    closestMatches.Add(formField.FieldName);
                }
            }

            var closestMatch = closestMatches.Count switch
            {
                1 => "; closest match: " + closestMatches[0],
                > 1 => "; closest matches: " + string.Join(", ", closestMatches),
                _ => string.Empty
            };

            OnWarningEvent(
                "{0,-25} {1} {2} does not match any of the form fields{3}",
                mDbUpdater.CurrentConfigDB + ":",
                parentDescription,
                fieldName,
                closestMatch);

            errorCount++;
        }

        /// <summary>
        /// Validate form field choosers
        /// </summary>
        /// <param name="errorCount"></param>
        /// <returns>True if names were validated, false if a critical error</returns>
        private bool ValidateFormFieldChoosers(ref int errorCount)
        {
            try
            {
                var formFieldChoosersLoaded = mDbUpdater.ReadFormFieldChoosers(out var formFieldChoosers);
                if (!formFieldChoosersLoaded)
                    return true;

                foreach (var formFieldChooser in formFieldChoosers)
                {
                    ValidateBasicField("Form field chooser", formFieldChooser, ref errorCount, out var emptyFieldName);

                    if (emptyFieldName)
                    {
                        continue;
                    }

                    var crossReference = formFieldChooser.CrossReference.TrimEnd().EndsWith("|required", StringComparison.OrdinalIgnoreCase)
                        ? formFieldChooser.CrossReference.Replace("|required", string.Empty)
                        : formFieldChooser.CrossReference;

                    ValidateFieldNameVsFormFields("Form field chooser XRef", crossReference, ref errorCount);

                    switch (formFieldChooser.Type)
                    {
                        case "list-report.helper":
                            ValidateListReportChooser(formFieldChooser, ref errorCount);
                            break;

                        case "link.list":
                        case "picker.append":
                        case "picker.list":
                        case "picker.prepend":
                        case "picker.prevDate":
                        case "picker.replace":
                            ValidatePickListChooser(formFieldChooser, ref errorCount);
                            break;

                        default:
                            OnWarningEvent("{0,-25} Unrecognized form field chooser type for chooser {1}: {2}",
                                mDbUpdater.CurrentConfigDB + ":",
                                formFieldChooser.FieldName,
                                formFieldChooser.Type);

                            errorCount++;
                            break;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateFormFieldChoosers", ex);
                return false;
            }
        }

        /// <summary>
        /// Validate form field names
        /// </summary>
        /// <remarks>Returns false if the source table or view was not found in the database</remarks>
        /// <param name="errorCount"></param>
        /// <returns>True if names were validated, false if a critical error</returns>
        private bool ValidateFormFieldNames(ref int errorCount)
        {
            try
            {
                var entryPageTableOrView = mGeneralParams.Parameters[GeneralParameters.ParameterType.EntryPageView];

                if (string.IsNullOrWhiteSpace(entryPageTableOrView) || entryPageTableOrView.Equals("V_@@@_Entry", StringComparison.OrdinalIgnoreCase))
                    return true;

                if (!GetColumnNamesInTableOrView(entryPageTableOrView, out var columnNames, out var targetDatabase))
                {
                    OnWarningEvent(
                        "{0,-25} {1} not found in database {2}; cannot validate form fields",
                        mDbUpdater.CurrentConfigDB + ":",
                        GetTableOrViewDescription(entryPageTableOrView, true),
                        targetDatabase);

                    return false;
                }

                var ignoredColumnMessages = new List<string>();

                foreach (var formField in mFormFields)
                {
                    var matchingColumnName = string.Empty;

                    // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                    foreach (var columnName in columnNames)
                    {
                        if (!columnName.Equals(formField.FieldName, StringComparison.OrdinalIgnoreCase))
                            continue;

                        matchingColumnName = columnName;
                        break;
                    }

                    if (matchingColumnName.Length > 0)
                    {
                        if (matchingColumnName.Equals(formField.FieldName))
                            continue;

                        OnWarningEvent(
                            "{0,-25} Form field {1} has a mismatched case vs. the column in {2}: {3}",
                            mDbUpdater.CurrentConfigDB + ":",
                            formField.FieldName,
                            GetTableOrViewDescription(entryPageTableOrView),
                            matchingColumnName);

                        errorCount++;
                        continue;
                    }

                    if (IgnoreMissingColumn(entryPageTableOrView, formField.FieldName))
                    {
                        ignoredColumnMessages.Add(string.Format("Ignoring column missing from {0} since expected: {1}", entryPageTableOrView, formField.FieldName));
                        continue;
                    }

                    if (formField.Type.Equals("hidden"))
                    {
                        ignoredColumnMessages.Add(string.Format("Ignoring column missing from {0} since a hidden field: {1}", entryPageTableOrView, formField.FieldName));
                        continue;
                    }

                    var closestMatches = new List<string>();
                    var closestMatchDistance = int.MaxValue;

                    // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                    foreach (var columnName in columnNames)
                    {
                        var distance = LevenshteinDistance.GetDistance(formField.FieldName, columnName);

                        if (distance < closestMatchDistance)
                        {
                            closestMatches.Clear();
                            closestMatches.Add(columnName);
                            closestMatchDistance = distance;
                        }
                        else if (distance == closestMatchDistance)
                        {
                            closestMatches.Add(columnName);
                        }
                    }

                    var closestMatch = closestMatches.Count switch
                    {
                        1 => "; closest match: " + closestMatches[0],
                        > 1 => "; closest matches: " + string.Join(", ", closestMatches),
                        _ => string.Empty
                    };

                    OnWarningEvent(
                        "{0,-25} Form field {1} is not a column in {2}{3}",
                        mDbUpdater.CurrentConfigDB + ":",
                        formField.FieldName,
                        GetTableOrViewDescription(entryPageTableOrView),
                        closestMatch);

                    errorCount++;
                }

                if (ignoredColumnMessages.Count > 0)
                {
                    OnDebugEvent(string.Join("\n  ", ignoredColumnMessages));
                }

                return errorCount == 0;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateFormFieldNames", ex);
                return false;
            }
        }

        /// <summary>
        /// Validate form field options
        /// </summary>
        /// <param name="errorCount"></param>
        /// <returns>True if names were validated, false if a critical error</returns>
        private bool ValidateFormFieldOptions(ref int errorCount)
        {
            try
            {
                var formFieldOptionsLoaded = mDbUpdater.ReadFormFieldOptions(out var formFieldChoosers);
                if (!formFieldOptionsLoaded)
                    return true;

                foreach (var formFieldOption in formFieldChoosers)
                {
                    ValidateBasicField("Form field option", formFieldOption, ref errorCount, out _);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateFormFieldOptions", ex);
                return false;
            }
        }

        private bool ValidateHotlinks(GeneralParameters.ParameterType sourceViewParameter, List<HotlinkInfo> hotlinks, ref int errorCount, bool cacheSourceColumnNames = false)
        {
            try
            {
                if (hotlinks.Count == 0 && !cacheSourceColumnNames)
                    return true;

                var sourceTableOrView = mGeneralParams.Parameters[sourceViewParameter];

                var sourceStoredProcedure = mGeneralParams.Parameters[sourceViewParameter == GeneralParameters.ParameterType.ListReportView
                    ? GeneralParameters.ParameterType.ListReportSP
                    : GeneralParameters.ParameterType.DetailReportSP];

                var reportType = sourceViewParameter switch
                {
                    GeneralParameters.ParameterType.ListReportView => "List report",
                    GeneralParameters.ParameterType.DetailReportView => "Detail report",
                    _ => "Unknown parent object"
                };

                string dataSourceType;
                string sourceTableViewOrProcedureName;
                bool storedProcedureDataSource;

                if (!string.IsNullOrWhiteSpace(sourceTableOrView))
                {
                    dataSourceType = sourceTableOrView.StartsWith("T_", StringComparison.OrdinalIgnoreCase) ? "table" : "view";
                    sourceTableViewOrProcedureName = sourceTableOrView;

                    if (sourceTableOrView.Equals("V_@@@_Detail_Report", StringComparison.OrdinalIgnoreCase))
                    {
                        // This is a special placeholder view in new.db
                        // It does not have any column names to cache
                        return true;
                    }

                    storedProcedureDataSource = false;
                }
                else if (!string.IsNullOrWhiteSpace(sourceStoredProcedure))
                {
                    dataSourceType = "stored procedure";
                    sourceTableViewOrProcedureName = sourceStoredProcedure;
                    storedProcedureDataSource = true;
                }
                else
                {
                    // Source view or stored procedure not defined
                    // This is the case for numerous entry pages

                    // ReSharper disable once InvertIf
                    if (hotlinks.Count > 0)
                    {
                        OnWarningEvent(
                            "{0,-25} {1} hotlinks are defined, but the source table, view, or stored procedure is not defined",
                            mDbUpdater.CurrentConfigDB + ":",
                            reportType);

                        errorCount++;
                    }

                    return true;
                }

                var columnNames = new SortedSet<string>();

                if (storedProcedureDataSource && StoredProcColumnNames.GetColumns(sourceTableViewOrProcedureName, out var storedProcedureColumnNames))
                {
                    foreach (var item in storedProcedureColumnNames)
                    {
                        columnNames.Add(item);
                    }
                }
                else if (GetColumnNamesInTableOrView(sourceTableViewOrProcedureName, out var tableOrViewColumnNames, out var targetDatabase))
                {
                    foreach (var item in tableOrViewColumnNames)
                    {
                        columnNames.Add(item);
                    }
                }
                else
                {
                    OnWarningEvent(
                        "{0,-25} {1} not found in database {2}; cannot {3}",
                        mDbUpdater.CurrentConfigDB + ":",
                        GetTableOrViewDescription(sourceTableViewOrProcedureName, true),
                        targetDatabase,
                        hotlinks.Count > 0 ? "validate hotlinks" : "cache the source data columns");

                    errorCount++;
                    return true;
                }

                if (cacheSourceColumnNames)
                {
                    PageFamilyColumnInfo cachedDatabaseColumnInfo;

                    if (mDbUpdater.ValidationNameCache.DatabaseColumnsByPageFamily.TryGetValue(CurrentPageFamily, out var existingCachedInfo))
                    {
                        cachedDatabaseColumnInfo = existingCachedInfo;
                    }
                    else
                    {
                        cachedDatabaseColumnInfo = new PageFamilyColumnInfo(CurrentPageFamily);
                        mDbUpdater.ValidationNameCache.DatabaseColumnsByPageFamily.Add(CurrentPageFamily, cachedDatabaseColumnInfo);
                    }

                    if (!cachedDatabaseColumnInfo.DatabaseColumnNames.ContainsKey(sourceTableViewOrProcedureName))
                    {
                        cachedDatabaseColumnInfo.DatabaseColumnNames.Add(sourceTableViewOrProcedureName, columnNames);
                    }
                }

                ValidateHotlinks(reportType, dataSourceType, sourceTableViewOrProcedureName, storedProcedureDataSource, hotlinks, columnNames, ref errorCount);

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateHotlinks", ex);
                return false;
            }
        }

        private void ValidateHotlinks(
            string reportType,
            string dataSourceType,
            string sourceTableViewOrProcedureName,
            bool storedProcedureDataSource,
            List<HotlinkInfo> hotlinks,
            SortedSet<string> columnNames,
            ref int errorCount)
        {
            // This sorted set is used to check for duplicates
            var fieldNames = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var item in hotlinks)
            {
                if (fieldNames.Contains(item.FieldName))
                {
                    OnWarningEvent(
                        "{0,-25} Two {1} hotlinks have the same name: {2}",
                        mDbUpdater.CurrentConfigDB + ":",
                        reportType.ToLower(),
                        item.FieldName);

                    errorCount++;
                }
                else
                {
                    fieldNames.Add(item.FieldName);
                }

                var columnName = mDbUpdater.GetCleanFieldName(item.FieldName, out _);

                var validColumn = false;
                var mismatchedCase = false;

                // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                foreach (var dbColumn in columnNames)
                {
                    if (!columnName.Equals(dbColumn, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (columnName.Equals(dbColumn, StringComparison.Ordinal))
                    {
                        validColumn = true;
                        break;
                    }

                    OnWarningEvent(
                        "{0,-25} {1} hotlink {2} has a mismatched case vs. the column name in the source {3}: {4}",
                        mDbUpdater.CurrentConfigDB + ":",
                        reportType,
                        columnName,
                        dataSourceType,
                        dbColumn);

                    mismatchedCase = true;
                    errorCount++;
                    break;
                }

                if (validColumn || mismatchedCase)
                    continue;

                if (columnName.Equals("Sel", StringComparison.OrdinalIgnoreCase))
                {
                    if (mDbUpdater.CurrentConfigDB.Equals("analysis_job_processor_group_association.db") ||
                        mDbUpdater.CurrentConfigDB.Equals("analysis_job_processor_group_membership.db") ||
                        mDbUpdater.CurrentConfigDB.Equals("eus_users.db") ||
                        mDbUpdater.CurrentConfigDB.StartsWith("helper_") ||
                        mDbUpdater.CurrentConfigDB.Equals("lc_cart_request_loading.db") ||
                        mDbUpdater.CurrentConfigDB.Equals("material_move_items.db") ||
                        mDbUpdater.CurrentConfigDB.Equals("mc_enable_control_by_manager.db") ||
                        mDbUpdater.CurrentConfigDB.Equals("mc_enable_control_by_manager_type.db") ||
                        mDbUpdater.CurrentConfigDB.Equals("requested_run_admin.db"))
                    {
                        continue;
                    }

                    Console.WriteLine("Possibly ignore the 'Sel' column in " + mDbUpdater.CurrentConfigDB);
                }

                if (columnName.Equals("@exclude"))
                {
                    // The Requested Run Factors parameter-based list report and the Requested Run Batch Blocking grid report
                    // use a special hotlink named @exclude to define the columns that are read-only and thus cannot be edited
                    continue;
                }

                if (IgnoreMissingColumn(sourceTableViewOrProcedureName, item.FieldName))
                {
                    OnDebugEvent("Ignoring column missing from {0} since expected: {1}", sourceTableViewOrProcedureName, item.FieldName);
                    continue;
                }

                // Example messages:
                //   List report hotlink id was not found in source view
                //   Detail report hotlink wellplate_name was not found in source
                //   List report hotlink id was not found in source stored procedure

                OnWarningEvent(
                    "{0,-25} {1} hotlink {2} was not found in source {3}",
                    mDbUpdater.CurrentConfigDB + ":",
                    reportType,
                    columnName,
                    GetTableOrViewDescription(sourceTableViewOrProcedureName, false, storedProcedureDataSource));

                errorCount++;
            }
        }

        private void ValidateListReportChooser(FormFieldChooserInfo formFieldChooser, ref int errorCount)
        {
            if (string.IsNullOrWhiteSpace(formFieldChooser.ListReportHelperName))
            {
                OnWarningEvent(
                    "{0,-25} Form field chooser {1} uses a list report helper, but the Target field is empty",
                    mDbUpdater.CurrentConfigDB, formFieldChooser.FieldName);

                errorCount++;
            }
            else
            {
                if (!formFieldChooser.ListReportHelperName.Equals(formFieldChooser.ListReportHelperName.Trim()))
                {
                    OnWarningEvent(
                        "{0,-25} Form field chooser {1} has extra whitespace at the start or end that should be removed",
                        mDbUpdater.CurrentConfigDB, formFieldChooser.FieldName);

                    errorCount++;
                }

                var adHocQueryMatch = mAdHocQueryMatcher.Match(formFieldChooser.ListReportHelperName);

                if (adHocQueryMatch.Success)
                {
                    // Ad-hoc query (aka utility query)
                    ValidateUtilityQueryChooser(formFieldChooser, adHocQueryMatch.Groups["QueryName"].Value, ref errorCount);
                }
                else
                {
                    ValidateListReportHelper(formFieldChooser, ref errorCount);
                }
            }

            if (string.IsNullOrWhiteSpace(formFieldChooser.PickListName))
                return;

            OnWarningEvent(
                "{0,-25} Form field chooser {1} is a list report helper chooser, but a pick list name is defined",
                mDbUpdater.CurrentConfigDB,
                formFieldChooser.FieldName);

            errorCount++;
        }

        private void ValidateListReportHelper(FormFieldChooserInfo formFieldChooser, ref int errorCount)
        {
            var slashIndex = formFieldChooser.ListReportHelperName.IndexOf("/", StringComparison.OrdinalIgnoreCase);

            var helperName = slashIndex > 0
                ? formFieldChooser.ListReportHelperName.Substring(0, slashIndex).Trim()
                : formFieldChooser.ListReportHelperName.Trim();

            if (helperName.Equals("helper_inst_source"))
            {
                // This is a special helper for showing files and directories on the instrument computer
                // It does not have a model config DB
                return;
            }

            var helperFile = new FileInfo(Path.Combine(mOptions.InputDirectory, string.Format("{0}.db", helperName)));

            if (!helperFile.Exists)
            {
                OnWarningEvent(
                    "{0,-25} Form field chooser {1} uses list report helper {2}, but the model config file does not exist",
                    mDbUpdater.CurrentConfigDB, formFieldChooser.FieldName, helperName);

                OnDebugEvent("Expected file path: " + helperFile.FullName);

                errorCount++;
            }
            else if (!Path.GetFileNameWithoutExtension(helperFile.Name).Equals(helperName))
            {
                OnWarningEvent(
                    "{0,-25} Form field chooser {1} uses list report helper {2}, but the model config file has different capitalization: {3}",
                    mDbUpdater.CurrentConfigDB, formFieldChooser.FieldName, helperName, helperFile.Name);

                errorCount++;
            }

            if (mDbUpdater.ValidationNameCache.ListReportHelperUsage.TryGetValue(helperName, out var usageCount))
            {
                mDbUpdater.ValidationNameCache.ListReportHelperUsage[helperName] = usageCount + 1;
            }
            else
            {
                mDbUpdater.ValidationNameCache.ListReportHelperUsage.Add(helperName, 1);
            }
        }

        /// <summary>
        /// Validate list report column names vs. the source view
        /// </summary>
        /// <param name="errorCount"></param>
        /// <returns>True if names were validated, false if a critical error</returns>
        private bool ValidateListReportHotlinks(ref int errorCount)
        {
            try
            {
                var listReportHotlinks = mDbUpdater.ReadHotlinks(ModelConfigDbUpdater.DB_TABLE_LIST_REPORT_HOTLINKS);

                return ValidateHotlinks(GeneralParameters.ParameterType.ListReportView, listReportHotlinks, ref errorCount);
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateListReportColumnNames", ex);
                return false;
            }
        }

        private void ValidatePickListChooser(FormFieldChooserInfo formFieldChooser, ref int errorCount)
        {
            var dateChooser = formFieldChooser.Type.Equals("picker.prevDate");
            var pickListName = formFieldChooser.PickListName.Trim();

            if (!dateChooser && string.IsNullOrWhiteSpace(pickListName))
            {
                OnWarningEvent(
                    "{0,-25} Form field chooser {1} is a pick list chooser, but the PickListName field is empty",
                    mDbUpdater.CurrentConfigDB,
                    formFieldChooser.FieldName);

                errorCount++;
            }
            else if (dateChooser)
            {
                if (pickListName.Length > 0)
                {
                    // Show a warning, but do not increment errorCount
                    // prevDatePickList or futureDatePickList
                    if (pickListName.Equals("prevDatePickList", StringComparison.OrdinalIgnoreCase) ||
                        pickListName.Equals("futureDatePickList", StringComparison.OrdinalIgnoreCase))
                    {
                        OnWarningEvent(
                            "{0,-25} Form field chooser {1} is a date selection chooser; " +
                            "we previously stored '{2} in the PickListName field, but that is no longer necessary. " +
                            "Consider deleting the text",
                            mDbUpdater.CurrentConfigDB, formFieldChooser.FieldName, pickListName);
                    }
                    else
                    {
                        OnWarningEvent(
                            "{0,-25} Form field chooser {1} is a date selection chooser; the PickListName field should be empty, not {2}",
                            mDbUpdater.CurrentConfigDB, formFieldChooser.FieldName, pickListName);
                    }
                }
            }
            else
            {
                if (!formFieldChooser.PickListName.Equals(pickListName))
                {
                    OnWarningEvent(
                        "{0,-25} Pick list chooser {1} has extra whitespace at the start or end that should be removed",
                        mDbUpdater.CurrentConfigDB,
                        formFieldChooser.FieldName);

                    errorCount++;
                }

                if (mDbUpdater.ChooserDefinitions.Count > 0)
                {
                    if (!mDbUpdater.ChooserDefinitions.TryGetValue(pickListName, out var chooserDefinition))
                    {
                        OnWarningEvent(
                            "{0,-25} Pick list chooser {1} uses pick list {2}, but that chooser is not defined in the chooser_definitions table in dms_chooser.db",
                            mDbUpdater.CurrentConfigDB,
                            formFieldChooser.FieldName, formFieldChooser.PickListName);

                        errorCount++;
                    }
                    else if (!chooserDefinition.Name.Equals(pickListName))
                    {
                        OnWarningEvent(
                            "{0,-25} Pick list chooser {1} uses pick list {2}, but the chooser definition " +
                            "in the chooser_definitions table in dms_chooser.db has different capitalization: {3}",
                            mDbUpdater.CurrentConfigDB,
                            formFieldChooser.FieldName, pickListName, chooserDefinition.Name);

                        errorCount++;
                    }
                }

                if (mDbUpdater.ValidationNameCache.PickListChooserUsage.TryGetValue(pickListName, out var usageCount))
                {
                    mDbUpdater.ValidationNameCache.PickListChooserUsage[pickListName] = usageCount + 1;
                }
                else
                {
                    mDbUpdater.ValidationNameCache.PickListChooserUsage.Add(pickListName, 1);
                }
            }

            if (string.IsNullOrWhiteSpace(formFieldChooser.ListReportHelperName))
                return;

            OnWarningEvent(
                "{0,-25} Form field chooser {1} is a pick list chooser, but the Target field has a list report helper defined",
                mDbUpdater.CurrentConfigDB,
                formFieldChooser.FieldName);

            errorCount++;
        }

        /// <summary>
        /// Validate stored procedure arguments
        /// </summary>
        /// <param name="errorCount"></param>
        /// <returns>True if names were validated, false if a critical error</returns>
        private bool ValidateStoredProcedureArguments(ref int errorCount)
        {
            try
            {
                var storedProcedureArgsLoaded = mDbUpdater.ReadStoredProcedureArguments(out var storedProcedureArguments);
                if (!storedProcedureArgsLoaded)
                    return true;

                if (mDbUpdater.CurrentConfigDB.Equals("file_attachment.db") ||
                    mDbUpdater.CurrentConfigDB.Equals("grid.db") ||
                    mDbUpdater.CurrentConfigDB.Equals("protein_collection_members.db") ||
                    mDbUpdater.CurrentConfigDB.Equals("requested_run_admin.db") ||
                    mDbUpdater.CurrentConfigDB.Equals("run_op_logs.db"))
                {
                    // Several model config DBs should have one or more stored procedures defined, but no form fields

                    if (mFormFields.Count == 0 && storedProcedureArguments.Count > 0)
                    {
                        Console.WriteLine();
                        OnStatusEvent(
                            "Skipping validation of stored procedure arguments for {0} since this page family does not have form fields",
                            mDbUpdater.CurrentConfigDB);

                        return true;
                    }
                }

                // ReSharper disable once ForeachCanBePartlyConvertedToQueryUsingAnotherGetEnumerator
                foreach (var procedureArgument in storedProcedureArguments)
                {
                    if (IsOperationsProcedure(procedureArgument.ProcedureName))
                        continue;

                    ValidateBasicField("Stored procedure argument", procedureArgument, ref errorCount, out _);
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateStoredProcedureArguments", ex);
                return false;
            }
        }

        private void ValidateUtilityQueryChooser(FormFieldChooserInfo formFieldChooser, string adHocQueryName, ref int errorCount)
        {
            if (!formFieldChooser.ListReportHelperName.StartsWith("data/lr/ad_hoc_query/", StringComparison.OrdinalIgnoreCase))
            {
                OnWarningEvent(
                    "{0,-25} Form field chooser {1} has ad_hoc_query in the Target field, but does not start with data/lr/ad_hoc_query/",
                    mDbUpdater.CurrentConfigDB, formFieldChooser.FieldName);

                errorCount++;
                return;
            }

            if (mDbUpdater.UtilityQueryDefinitions.Count == 0)
                return;

            if (!mDbUpdater.UtilityQueryDefinitions.TryGetValue(adHocQueryName, out var queryDefinition))
            {
                OnWarningEvent(
                    "{0,-25} Form field chooser {1} uses ad_hoc_query {2}, but that query is not defined in the utility_queries table in ad_hoc_query.db",
                    mDbUpdater.CurrentConfigDB, formFieldChooser.FieldName, adHocQueryName);

                errorCount++;
            }
            else if (!queryDefinition.Name.Equals(adHocQueryName))
            {
                OnWarningEvent(
                    "{0,-25} Form field chooser {1} uses ad_hoc_query {2}, but the query definition " +
                    "in the utility_queries table in ad_hoc_query.db has different capitalization: {3}",
                    mDbUpdater.CurrentConfigDB, formFieldChooser.FieldName, adHocQueryName, queryDefinition.Name);

                errorCount++;
            }
        }
    }
}
