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
        // ReSharper disable CommentTypo

        // Ignore Spelling: citext, Crit, dbo, dms, Excl, gigasax, hotlink, hotlinks, Labelling, Levenshtein, Lvl
        // Ignore Spelling: Parm, Postgres, proteinseqs, Pri, Proc, Prot, sel, Sep, wellplate

        // ReSharper restore CommentTypo

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

        private bool GetColumnNamesInStoredProcedure(string storedProcedureName, out List<string> columnNames)
        {
            if (storedProcedureName.Equals("GetDatasetStatsByCampaign"))
            {
                columnNames = new List<string>
                {
                    "Campaign",
                    "Work Package",
                    "Pct EMSL Funded",
                    "Runtime Hours",
                    "Datasets",
                    "Building",
                    "Instrument",
                    "Request Min",
                    "Request Max",
                    "Pct Total Runtime"
                };
            }
            else if (storedProcedureName.Equals("GetPackageDatasetJobToolCrosstab"))
            {
                columnNames = new List<string>
                {
                    "Dataset",
                    "Jobs"
                };
            }
            else if (storedProcedureName.Equals("FindExistingJobsForRequest"))
            {
                columnNames = new List<string>
                {
                    "Job",
                    "State",
                    "Priority",
                    "Request",
                    "Created",
                    "Start",
                    "Finish",
                    "Processor",
                    "Dataset"
                };
            }
            else if (storedProcedureName.Equals("FindMatchingDatasetsForJobRequest"))
            {
                columnNames = new List<string>
                {
                    "Sel",
                    "Dataset",
                    "Jobs",
                    "New",
                    "Busy",
                    "Complete",
                    "Failed",
                    "Holding"
                };
            }
            else if (storedProcedureName.Equals("GetCurrentMangerActivity"))
            {
                columnNames = new List<string>
                {
                    "Source",
                    "When",
                    "Who",
                    "What",
                    "#Alert"
                };
            }
            else if (storedProcedureName.Equals("PredefinedAnalysisDatasets"))
            {
                columnNames = new List<string>
                {
                    "Dataset",
                    "ID",
                    "InstrumentClass",
                    "Instrument",
                    "Campaign",
                    "Experiment",
                    "Organism",
                    "Exp Labelling",
                    "Exp Comment",
                    "DS Comment",
                    "DS Type",
                    "DS Rating",
                    "Rating",
                    "Sep Type",
                    "Tool",
                    "Parameter File",
                    "Settings File",
                    "Protein Collections",
                    "Legacy FASTA"
                };
            }
            else if (storedProcedureName.Equals("EvaluatePredefinedAnalysisRules"))
            {
                // ReSharper disable StringLiteralTypo

                columnNames = new List<string>
                {
                    // Columns for mode: Show Jobs
                    "Job",
                    "Dataset",
                    "Jobs",
                    "Tool",
                    "Pri",
                    "Processor_Group",
                    "Comment",
                    "Param_File",
                    "Settings_File",
                    "OrganismDB_File",
                    "Organism",
                    "Protein_Collections",
                    "Protein_Options",
                    "Owner",
                    "Export_Mode",
                    "Special_Processing",
                    // Columns for mode: Show Rules
                    "Step",
                    "Level",
                    "Seq.",
                    "Rule_ID",
                    "Next Lvl.",
                    "Trigger Mode",
                    "Export Mode",
                    "Action",
                    "Reason",
                    "Notes",
                    "Analysis Tool",
                    "Instrument Class Crit.",
                    "Instrument Crit.",
                    "Instrument Exclusion",
                    "Campaign Crit.",
                    "Campaign Exclusion",
                    "Experiment Crit.",
                    "Experiment Exclusion",
                    "Organism Crit.",
                    "Dataset Crit.",
                    "Dataset Exclusion",
                    "Dataset Type",
                    "Exp. Comment Crit.",
                    "Labelling Incl.",
                    "Labelling Excl.",
                    "Separation Type Crit.",
                    "ScanCount Min",
                    "ScanCount Max",
                    "Parm File",
                    "Settings File",
                    // "Organism",
                    "Organism DB",
                    "Prot. Coll.",
                    "Prot. Opts.",
                    "Special Proc.",
                    "Priority",
                    "Processor Group"
                };

                // ReSharper restore StringLiteralTypo
            }
            else if (storedProcedureName.Equals("EvaluatePredefinedAnalysisRulesMDS"))
            {
                columnNames = new List<string>
                {
                    "ID",
                    "Job",
                    "Dataset",
                    "Jobs",
                    "Tool",
                    "Pri",
                    "Processor_Group",
                    "Comment",
                    "Param_File",
                    "Settings_File",
                    "OrganismDB_File",
                    "Organism",
                    "Protein_Collections",
                    "Protein_Options",
                    "Special_Processing",
                    "Owner",
                    "Export_Mode"
                };
            }
            else if (storedProcedureName.Equals("ReportProductionStats"))
            {
                columnNames = new List<string>
                {
                    "Instrument",
                    "Total Datasets",
                    "Days in range",
                    "Datasets per day",
                    "Blank Datasets",
                    "QC Datasets",
                    "Bad Datasets",
                    "Study Specific Datasets",
                    "Study Specific Datasets per day",
                    "EMSL-Funded Study Specific Datasets",
                    "EF Study Specific Datasets per day",
                    "Total AcqTimeDays",
                    "Study Specific AcqTimeDays",
                    "EF Total AcqTimeDays",
                    "EF Study Specific AcqTimeDays",
                    "Hours AcqTime per Day",
                    "Inst.",
                    "% Inst EMSL Owned",
                    "EF Total Datasets",
                    "EF Datasets per day",
                    "% Blank Datasets",
                    "% QC Datasets",
                    "% Bad Datasets",
                    "% Study Specific Datasets",
                    "% EF Study Specific Datasets",
                    "% EF Study Specific by AcqTime",
                    "Inst"
                };
            }
            else if (storedProcedureName.Equals("GetProteinCollectionMemberDetail"))
            {
                columnNames = new List<string>
                {
                    "Protein_Collection_ID",
                    "Protein_Name",
                    "Description",
                    "Protein_Sequence",
                    "Monoisotopic_Mass",
                    "Average_Mass",
                    "Residue_Count",
                    "Molecular_Formula",
                    "Protein_ID",
                    "Reference_ID",
                    "SHA1_Hash",
                    "Member_ID",
                    "Sorting_Index"
                };
            }
            else if (storedProcedureName.Equals("GetFactorCrosstabByBatch"))
            {
                columnNames = new List<string>
                {
                    "Sel",
                    "BatchID",
                    "Name",
                    "Status",
                    "Dataset_ID",
                    "Request",
                    "Block",
                    "Run Order"
                };
            }
            else
            {
                columnNames = new List<string>();
            }

            return columnNames.Count > 0;
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

                return ValidateHotLinks(GeneralParameters.ParameterType.DetailReportView, detailReportHotlinks, ref errorCount);
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

        private bool ValidateHotLinks(GeneralParameters.ParameterType sourceViewParameter, List<HotLinkInfo> hotlinks, ref int errorCount)
        {
            try
            {
                if (hotlinks.Count == 0)
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
                    OnWarningEvent(
                        "{0,-25} Hotlinks are defined, but the {1} table, view, or stored procedure is not defined",
                        mDbUpdater.CurrentConfigDB + ":",
                        reportType.ToLower());

                    return true;
                }

                var columnNames = new SortedSet<string>();

                if (storedProcedureDataSource && GetColumnNamesInStoredProcedure(sourceTableViewOrProcedureName, out var storedProcedureColumnNames))
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
                        "{0,-25} {1} not found in database {2}; cannot validate hotlinks",
                        mDbUpdater.CurrentConfigDB + ":",
                        GetTableOrViewDescription(sourceTableViewOrProcedureName, true),
                        targetDatabase);

                    errorCount++;
                    return true;
                }

                foreach (var item in hotlinks)
                {
                    var columnName = mDbUpdater.GetCleanFieldName(item.FieldName, out _);

                    var validColumn = false;

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

                        errorCount++;
                        break;
                    }

                    if (validColumn)
                        continue;

                    if (columnName.Equals("Sel."))
                    {
                        if (mDbUpdater.CurrentConfigDB.Equals("analysis_job_processor_group_association.db") ||
                            mDbUpdater.CurrentConfigDB.Equals("analysis_job_processor_group_membership.db") ||
                            mDbUpdater.CurrentConfigDB.Equals("z"))
                        {
                            continue;
                        }

                        Console.WriteLine("Possibly ignore the 'Sel.' column in " + mDbUpdater.CurrentConfigDB);
                    }

                    if (columnName.Equals("Sel"))
                    {
                        if (mDbUpdater.CurrentConfigDB.Equals("eus_users.db") ||
                            mDbUpdater.CurrentConfigDB.StartsWith("helper_") ||
                            mDbUpdater.CurrentConfigDB.Equals("lc_cart_request_loading.db") ||
                            mDbUpdater.CurrentConfigDB.Equals("material_move_items.db") ||
                            mDbUpdater.CurrentConfigDB.Equals("mc_enable_control_by_manager.db") ||
                            mDbUpdater.CurrentConfigDB.Equals("mc_enable_control_by_manager_type.db") ||
                            mDbUpdater.CurrentConfigDB.Equals("requested_run_admin.db"))
                        {
                            continue;
                        }

                        Console.WriteLine("Possibly ignore the 'Sel.' column in " + mDbUpdater.CurrentConfigDB);
                    }

                    if (columnName.Equals("@exclude"))
                    {
                        // The Requested Run Factors parameter-based list report and the Requested Run Batch Blocking grid report
                        // use a special hotlink named @exclude to define the columns that are read-only and thus cannot be edited
                        continue;
                    }

                    if (item.FieldName.Equals("Download") && mDbUpdater.CurrentConfigDB.Equals("mrm_list_attachment.db"))
                    {
                        continue;
                    }

                    OnWarningEvent(
                        "{0,-25} {1} hotlink {2} was not found in source {3}",
                        mDbUpdater.CurrentConfigDB + ":",
                        reportType,
                        columnName,
                        GetTableOrViewDescription(sourceTableViewOrProcedureName, false, storedProcedureDataSource));

                    errorCount++;
                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ValidateHotLinks", ex);
                return false;
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

                return ValidateHotLinks(GeneralParameters.ParameterType.ListReportView, listReportHotlinks, ref errorCount);
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
    }
}
