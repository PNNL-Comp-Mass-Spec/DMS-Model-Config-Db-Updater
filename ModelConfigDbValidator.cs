using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using PRISM;
using PRISMDatabaseUtils;

namespace DMSModelConfigDbUpdater
{
    internal class ModelConfigDbValidator : EventNotifier
    {
        // Ignore Spelling: citext, dbo, dms, gigasax, Levenshtein, Postgres, proteinseqs, wellplate

        /// <summary>
        /// Keys in this dictionary are database names
        /// Values are an instance of DatabaseColumnInfo
        /// </summary>
        private readonly Dictionary<string, DatabaseColumnInfo> mDatabaseColumns;

        private readonly ModelConfigDbUpdater mDbUpdater;

        private readonly List<FormFieldInfo> mFormFields;

        private readonly GeneralParameters mGeneralParams;

        /// <summary>
        /// Keys in this dictionary are model config file names (without the file extension)
        /// Values are dictionaries where keys are source view names and values are a list of known missing columns
        /// </summary>
        private readonly Dictionary<string, Dictionary<string, List<string>>> mMissingColumnsToIgnore;

        private readonly ModelConfigDbUpdaterOptions mOptions;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="dbUpdater"></param>
        /// <param name="generalParams"></param>
        /// <param name="formFields"></param>
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
                GetMissingColumnDictionary("V_Analysis_Job_Entry", new List<string>
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
                    "tab",
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
                    // ReSharper disable StringLiteralTypo
                    "parm_file",
                    "settings_file",
                    "organism",
                    "prot_coll_name_list",
                    "prot_coll_options_list",
                    // ReSharper restore StringLiteralTypo
                }));
        }

        private Dictionary<string, List<string>> GetMissingColumnDictionary(string tableOrView, List<string> columnsToIgnore)
        {
            return new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase)
            {
                { tableOrView, columnsToIgnore }
            };
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
                    schemaNameToFind = ModelConfigDbUpdater.GetSchemaName(tableOrViewName);
                    nameWithoutSchema = ModelConfigDbUpdater.GetNameWithoutSchema(tableOrViewName);
                }
                else
                {
                    schemaNameToFind = "dbo";
                    nameWithoutSchema = tableOrViewName;
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

        internal static string GetTableOrViewDescription(string tableOrViewName, bool capitalizeFirstWord = false)
        {
            string objectType;

            if (tableOrViewName.StartsWith("T_", StringComparison.OrdinalIgnoreCase))
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
            if (!mMissingColumnsToIgnore.TryGetValue(Path.GetFileNameWithoutExtension(mDbUpdater.CurrentConfigDB), out var tablesAndViews))
                return false;

            return tablesAndViews.TryGetValue(tableOrView, out var columnsToIgnore) && columnsToIgnore.Contains(columnName);
        }

        private bool IsOperationsProcedure(string procedureName)
        {
            return mGeneralParams.Parameters.TryGetValue(GeneralParameters.ParameterType.OperationsSP, out var operationsProcedure) &&
                   operationsProcedure.Equals(procedureName, StringComparison.OrdinalIgnoreCase);
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
                        DbServerTypes.PostgreSQL, mOptions.DatabaseServer, databaseName, "ModelConfigDbValidator");

                    dbTools = DbToolsFactory.GetDBTools(DbServerTypes.PostgreSQL, connectionString);
                }
                else
                {
                    string serverToUse;

                    if (mOptions.DatabaseServer.Equals("gigasax", StringComparison.OrdinalIgnoreCase) &&
                        mGeneralParams.Parameters[GeneralParameters.ParameterType.DatabaseGroup].Equals("manager_control", StringComparison.OrdinalIgnoreCase))
                    {
                        serverToUse = "proteinseqs";
                    }
                    else
                    {
                        serverToUse = mOptions.DatabaseServer;
                    }

                    // SQL Server
                    var connectionString = DbToolsFactory.GetConnectionString(
                        DbServerTypes.MSSQLServer, serverToUse, databaseName, "ModelConfigDbValidator");

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

                // ToDo:
                // Validate form field names in other tables, including sproc_args, form_field_choosers (including XRef), form_field_options, external sources,
                //    and general params with the post_submission_detail_id and entry_page_data_id_col parameters

                validationCompleted.Add(ValidateFormFieldChoosers(ref errorCount));

                validationCompleted.Add(ValidateFormFieldOptions(ref errorCount));

                validationCompleted.Add(ValidateExternalSources(ref errorCount));

                validationCompleted.Add(ValidateStoredProcedureArguments(ref errorCount));

                validationCompleted.Add(ValidateListReportColumnNames(ref errorCount));

                validationCompleted.Add(ValidateDetailReportColumnNames(ref errorCount));

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
        private bool ValidateDetailReportColumnNames(ref int errorCount)
        {
            try
            {
                errorCount++;
                errorCount--;

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateDetailReportColumnNames", ex);
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

                foreach (var externalSource in externalSources)
                {
                    ValidateBasicField("External source", externalSource, ref errorCount, out _);
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

            if (fieldName.Equals("<local>"))
            {
                // Stored procedure arguments often have field name <local>
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

                    if (!emptyFieldName)
                    {
                        ValidateFieldNameVsFormFields("Form field chooser XRef", formFieldChooser.CrossReference, ref errorCount);
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

                if (string.IsNullOrWhiteSpace(entryPageTableOrView) || entryPageTableOrView.Equals("V_@@@_Entry"))
                    return true;

                if (!GetColumnNamesInTableOrView(entryPageTableOrView, out var columnNames, out var targetDatabase))
                {
                    OnWarningEvent(
                        "{0,-25} {1} not found in database {2}",
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

        /// <summary>
        /// Validate list report column names vs. the source view
        /// </summary>
        /// <param name="errorCount"></param>
        /// <returns>True if names were validated, false if a critical error</returns>
        private bool ValidateListReportColumnNames(ref int errorCount)
        {
            try
            {
                errorCount++;
                errorCount--;

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateListReportColumnNames", ex);
                return false;
            }
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
    }
}
