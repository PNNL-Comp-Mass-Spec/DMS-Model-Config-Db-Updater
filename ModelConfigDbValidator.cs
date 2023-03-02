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
        // Ignore Spelling: bigint, bytea, citext, cstring, dbo, dms, gigasax, hotlink, hotlinks
        // Ignore Spelling: inet, Levenshtein, lr, Postgres, Proc, proteinseqs, Sel, varchar, wellplate

        // ReSharper disable CommentTypo

        // Ignore Spelling: pronamespace, proname, prokind, proretset, oid, regnamespace, proallargtypes, proargnames, proargmodes

        // ReSharper restore CommentTypo

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
        /// Keys in this dictionary are database names
        /// Values are an instance of DatabaseFunctionAndProcedureInfo
        /// </summary>
        private readonly Dictionary<string, DatabaseFunctionAndProcedureInfo> mDatabaseFunctionAndProcedureArguments;

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
            mDatabaseFunctionAndProcedureArguments = new Dictionary<string, DatabaseFunctionAndProcedureInfo>(StringComparer.OrdinalIgnoreCase);
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

        /// <summary>
        /// Extract the function or procedure argument names from argumentList
        /// </summary>
        /// <param name="currentObject">Function or procedure name, with schema</param>
        /// <param name="argumentList">Comma separated list of argument names for a Postgres function or procedure</param>
        /// <param name="ordinalPositionByName">Dictionary where keys are argument names and values are ordinal position (1-based)</param>
        /// <param name="dataTypeByOrdinalPosition">Dictionary where keys are ordinal position (1-based) and values are data type</param>
        private void DetermineArgumentOrdinalPositions(
            string currentObject,
            string argumentList,
            IDictionary<string, int> ordinalPositionByName,
            IDictionary<int, string> dataTypeByOrdinalPosition)
        {
            ordinalPositionByName.Clear();
            dataTypeByOrdinalPosition.Clear();

            var columnNumber = 0;

            // Split on commas
            foreach (var argumentInfo in argumentList.Split(','))
            {
                columnNumber++;

                GetArgumentNameTypeAndDirection(argumentInfo, out var argumentName, out var dataType, out _);

                if (!string.IsNullOrWhiteSpace(argumentName))
                {
                    if (ordinalPositionByName.ContainsKey(argumentName))
                    {
                        OnWarningEvent("Ignoring duplicate argument '{0}' for procedure or function {1}", argumentName, currentObject);
                    }
                    else
                    {
                        ordinalPositionByName.Add(argumentName, columnNumber);
                    }
                }

                dataTypeByOrdinalPosition.Add(columnNumber, dataType);
            }
        }

        private void GetArgumentNameTypeAndDirection(string argumentInfo, out string argumentName, out string dataType, out GeneralParameters.ArgumentDirection direction)
        {
            // Trim leading spaces
            var trimmedArgumentInfo = argumentInfo.TrimStart();

            // Look for the argument direction (IN, OUT, or INOUT)
            string nameAndType;

            if (trimmedArgumentInfo.StartsWith("INOUT"))
            {
                direction = GeneralParameters.ArgumentDirection.InOut;
                nameAndType = trimmedArgumentInfo.Substring(5).Trim();
            }
            else if (trimmedArgumentInfo.StartsWith("IN"))
            {
                direction = GeneralParameters.ArgumentDirection.In;
                nameAndType = trimmedArgumentInfo.Substring(2).Trim();
            }
            else if (trimmedArgumentInfo.StartsWith("OUT"))
            {
                direction = GeneralParameters.ArgumentDirection.Out;
                nameAndType = trimmedArgumentInfo.Substring(3).Trim();
            }
            else
            {
                direction = GeneralParameters.ArgumentDirection.In;
                nameAndType = trimmedArgumentInfo.Trim();
            }

            // Look for data type
            var spaceIndex = nameAndType.IndexOf(' ');

            if (spaceIndex > 0  && !nameAndType.Equals("double precision"))
            {
                argumentName = nameAndType.Substring(0, spaceIndex);
                dataType = spaceIndex < nameAndType.Length - 1 ? nameAndType.Substring(spaceIndex + 1) : string.Empty;
            }
            else
            {
                // Most likely nameAndType only has the data type of the parameter
                // This is often the case with system functions
                if (nameAndType.Equals("bigint") ||
                    nameAndType.Equals("boolean") ||
                    nameAndType.Equals("bytea") ||
                    nameAndType.Equals("character") ||
                    nameAndType.Equals("citext") ||
                    nameAndType.Equals("cstring") ||
                    nameAndType.Equals("double precision") ||
                    nameAndType.Equals("inet") ||
                    nameAndType.Equals("integer") ||
                    nameAndType.Equals("internal") ||
                    nameAndType.Equals("oid") ||
                    nameAndType.Equals("text") ||
                    nameAndType.Equals("text[]"))
                {
                    argumentName = string.Empty;
                }
                else
                {
                    argumentName = nameAndType;
                }

                dataType = nameAndType;
            }
        }

        /// <summary>
        /// Parse the query results to determine the function or stored procedure argument name, type, and direction
        /// </summary>
        /// <param name="currentObject">Function or procedure name, with schema</param>
        /// <param name="result">Query result row</param>
        /// <param name="isPostgres">True if parsing PostgreSQL query results</param>
        /// <param name="isProcedure">True if parsing PostgreSQL query results</param>
        /// <param name="ordinalPositionByName">Dictionary where keys are argument names and values are ordinal position (only used with Postgres)</param>
        /// <param name="dataTypeByOrdinalPosition">Dictionary where keys are ordinal position (1-based) and values are data type</param>
        /// <returns>Instance of FunctionOrProcedureArgumentInfo</returns>
        private FunctionOrProcedureArgumentInfo GetArgumentInfo(
            string currentObject,
            DataRow result,
            bool isPostgres,
            bool isProcedure,
            IReadOnlyDictionary<string, int> ordinalPositionByName,
            IDictionary<int, string> dataTypeByOrdinalPosition)
        {
            // Postgres columns:
            // argument_info: Argument direction (optional), name (optional), and type

            // SQL Server columns:
            // parameter_id: Ordinal Position (0 if a return value, -1 if unknown)
            // parameter_name: Argument name
            // parameter_data_type: Argument data type
            // parameter_max_length: Max length (e.g., for varchar)
            // is_output_parameter: 1 if an output parameter, otherwise 0

            int ordinalPosition;
            string argumentName;
            string dataType;
            GeneralParameters.ArgumentDirection argumentDirection;

            if (isPostgres)
            {
                var argumentInfo = result["argument_info"].CastDBVal<string>();

                // Parse parameterInfo to determine the argument name, type, and direction
                // Example values:
                //   IN _name text
                //   IN _infoOnly boolean
                //   INOUT _message text
                //   OUT level bigint
                //   _filename citext
                //   _value double precision
                //   bytea
                //   text

                GetArgumentNameTypeAndDirection(argumentInfo, out argumentName, out dataType, out argumentDirection);

                if (ordinalPositionByName.TryGetValue(argumentName, out var position))
                {
                    ordinalPosition = position;

                    if (dataTypeByOrdinalPosition.TryGetValue(ordinalPosition, out var dataTypeFromParameterList) &&
                        !dataType.Equals(dataTypeFromParameterList))
                    {
                        if (dataType.Equals("timestamp with time zone") && dataTypeFromParameterList.Equals("timestamp without time zone") ||
                            dataType.Equals("timestamp without time zone") && dataTypeFromParameterList.Equals("timestamp with time zone") ||
                            dataType.Equals("numeric") && dataTypeFromParameterList.Equals("double precision") ||
                            dataType.Equals("text") && dataTypeFromParameterList.Equals("regclass") ||
                            dataType.Equals("regclass") && dataTypeFromParameterList.Equals("text"))
                        {
                            // These differences can be ignored
                        }
                        else
                        {
                            OnWarningEvent("Data type mismatch for argument {0} in procedure or function {1}: {2} vs. {3}",
                                argumentName, currentObject, dataType, dataTypeFromParameterList);
                        }
                    }
                }
                else
                {
                    ordinalPosition = -1;
                }
            }
            else
            {
                ordinalPosition = result["parameter_id"].CastDBVal<int>();
                argumentName = result["parameter_name"].CastDBVal<string>();
                dataType = result["parameter_data_type"].CastDBVal<string>();
                var outputParameterFlag = result["is_output_parameter"].CastDBVal<int>();

                if (outputParameterFlag > 0)
                {
                    argumentDirection = GeneralParameters.ArgumentDirection.InOut;
                }
                else
                {
                    argumentDirection = GeneralParameters.ArgumentDirection.In;
                }

                // Unused:
                // var maxLength = result["parameter_max_length"].CastDBVal<int>();

            }

            var argumentNameToStore = RemoveArgumentPrefix(currentObject, argumentName, isPostgres, isProcedure);

            return new FunctionOrProcedureArgumentInfo(ordinalPosition, argumentNameToStore, dataType, argumentDirection);
        }

        private string GetDatabaseName(string databaseName)
        {
            if (mOptions.UseDevelopmentDatabases)
            {
                return databaseName.ToLower() switch
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

            return databaseName;
        }

        private string GetDatabaseServer()
        {
            if (mOptions.DatabaseServer.Equals("gigasax", StringComparison.OrdinalIgnoreCase) &&
                mGeneralParams.Parameters[GeneralParameters.ParameterType.DatabaseGroup].Equals("manager_control", StringComparison.OrdinalIgnoreCase))
            {
                // Auto-change the server since the manager control database is not on Gigasax
                return "proteinseqs";
            }

            return mOptions.DatabaseServer;
        }

        private bool GetFunctionOrProcedureInfo(string functionOrProcedureName, out FunctionOrProcedureInfo objectInfo, out string targetDatabase)
        {
            var undefinedObjectInfo = new FunctionOrProcedureInfo(string.Empty, string.Empty, false);
            targetDatabase = GetTargetDatabase();

            try
            {
                if (!mDatabaseFunctionAndProcedureArguments.ContainsKey(targetDatabase))
                {
                    var success = RetrieveDatabaseFunctionAndProcedureInfo(targetDatabase);

                    if (!success)
                    {
                        objectInfo = undefinedObjectInfo;
                        return false;
                    }
                }

                string schemaNameToFind;
                string nameWithoutSchema;

                if (functionOrProcedureName.Contains("."))
                {
                    schemaNameToFind = PossiblyUnquote(ModelConfigDbUpdater.GetSchemaName(functionOrProcedureName));
                    nameWithoutSchema = PossiblyUnquote(ModelConfigDbUpdater.GetNameWithoutSchema(functionOrProcedureName));
                }
                else
                {
                    schemaNameToFind = mOptions.UsePostgresSchema ? "public" : "dbo";
                    nameWithoutSchema = PossiblyUnquote(functionOrProcedureName);
                }

                var functionAndProcedureInfo = mDatabaseFunctionAndProcedureArguments[targetDatabase];

                objectInfo = functionAndProcedureInfo.GetArgumentListForFunctionOrProcedure(schemaNameToFind, nameWithoutSchema);

                return objectInfo.ArgumentList.Count > 0;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in GetFunctionOrProcedureInfo", ex);
                objectInfo = undefinedObjectInfo;
                return false;
            }
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
        /// For SQL Server arguments, remove @ from the start of the argument name
        /// For PostgreSQL arguments, remove _ from the start of the argument name
        /// </summary>
        /// <remarks>SQL Server arguments with ordinal position 0 will be named 'Returns'</remarks>
        /// <param name="currentObject">Function or procedure name, with schema</param>
        /// <param name="argumentName"></param>
        /// <param name="isPostgres"></param>
        /// <param name="isProcedure"></param>
        /// <returns>Updated argument name</returns>
        private string RemoveArgumentPrefix(string currentObject, string argumentName, bool isPostgres, bool isProcedure)
        {
            var prefix = isPostgres ? "_" : "@";

            if (argumentName.StartsWith(prefix))
            {
                return argumentName.Substring(1);
            }

            if (isPostgres)
            {
                if (argumentName.Length == 0)
                    return string.Empty;

                // Procedure argument names should start with an underscore; function argument names often do not start with an underscore
                if (!isProcedure)
                    return argumentName;
            }
            else if (argumentName.Equals("Returns", StringComparison.OrdinalIgnoreCase))
            {
                return argumentName;
            }

            OnWarningEvent("Argument {0} in procedure or function {1} does not start with '{2}'",
                argumentName, currentObject, prefix);

            return argumentName;
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
                    // Note that this connection string includes the username: d3l243
                    var connectionString = DbToolsFactory.GetConnectionString(
                        DbServerTypes.PostgreSQL, mOptions.DatabaseServer, databaseName,
                        "d3l243", string.Empty, "ModelConfigDbValidator");

                    dbTools = DbToolsFactory.GetDBTools(DbServerTypes.PostgreSQL, connectionString);
                }
                else
                {
                    var serverToUse = GetDatabaseServer();

                    var databaseToUse = GetDatabaseName(databaseName);

                    // SQL Server
                    var connectionString = DbToolsFactory.GetConnectionString(
                        DbServerTypes.MSSQLServer, serverToUse, databaseToUse, "ModelConfigDbValidator");

                    dbTools = DbToolsFactory.GetDBTools(DbServerTypes.MSSQLServer, connectionString);
                }

                // Note that Information_Schema.Columns includes the column names for both the tables and views in a database
                // Postgres includes the information_schema objects in the list, plus also pg_catalog objects, so we exclude them in the query

                const string sqlQuery =
                    "SELECT table_schema, table_name, column_name " +
                    "FROM Information_Schema.Columns " +
                    "WHERE table_schema Not In ('information_schema', 'pg_catalog') " +
                    "ORDER BY table_schema, table_name, column_name";

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
        /// Query system tables to obtain the argument list for all functions and procedures in the target database
        /// </summary>
        /// <param name="databaseName"></param>
        /// <returns>True if successful, false if an error</returns>
        private bool RetrieveDatabaseFunctionAndProcedureInfo(string databaseName)
        {
            try
            {
                IDBTools dbTools;

                if (mOptions.UsePostgresSchema)
                {
                    // Note that this connection string includes the username: d3l243
                    var connectionString = DbToolsFactory.GetConnectionString(
                        DbServerTypes.PostgreSQL, mOptions.DatabaseServer, databaseName,
                        "d3l243", string.Empty, "ModelConfigDbValidator");

                    dbTools = DbToolsFactory.GetDBTools(DbServerTypes.PostgreSQL, connectionString);
                }
                else
                {
                    var serverToUse = GetDatabaseServer();

                    var databaseToUse = GetDatabaseName(databaseName);

                    // SQL Server
                    var connectionString = DbToolsFactory.GetConnectionString(
                        DbServerTypes.MSSQLServer, serverToUse, databaseToUse, "ModelConfigDbValidator");

                    dbTools = DbToolsFactory.GetDBTools(DbServerTypes.MSSQLServer, connectionString);
                }

                // Note that Information_Schema.Columns includes the column names for both the tables and views in a database
                // Postgres includes the information_schema objects in the list, plus also pg_catalog objects, so we exclude them in the query

                string sqlQuery;

                // ReSharper disable StringLiteralTypo
                // ReSharper disable CommentTypo

                if (mOptions.UsePostgresSchema)
                {
                    sqlQuery =
                        "SELECT pronamespace::regnamespace::text as object_schema," +
                        "       proname as object_name," +
                        "       prokind as object_type," +                          // p for procedure, f for function
                        "       proretset as returns_table," +
                        "       pg_get_function_identity_arguments(oid) AS argument_list," +                                        // Comma separated list of arguments
                        "       trim(UNNEST(string_to_array(pg_get_function_identity_arguments(oid), ',' ))) AS argument_info," +   // argument direction (optional), name, and type
                        "       oid " +
                        // "       proallargtypes as argument_types," +
                        // "       proargnames as argument_names," +
                        // "       proargmodes as argument_modes," +
                        // "       pg_get_function_arguments(oid) AS args_def " +   // full definition of arguments with defaults
                        "FROM pg_proc " +
                        "WHERE NOT pronamespace::regnamespace in ('pg_catalog'::regnamespace, 'information_schema'::regnamespace) " +
                        "ORDER BY pronamespace::regnamespace, proname";
                }
                else
                {
                    sqlQuery =
                        "SELECT SCHEMA_NAME(SCHEMA_ID) AS object_schema," +
                        "       SO.name AS object_name," +
                        "       SO.Type_Desc AS object_type," +                     // CLR_STORED_PROCEDURE, SQL_TABLE_VALUED_FUNCTION, SQL_STORED_PROCEDURE, SQL_SCALAR_FUNCTION
                        "       P.parameter_id AS parameter_id," +                  // Ordinal position of the parameter
                        "       Case When P.parameter_id = 0 Then 'Returns' Else P.name End AS parameter_name," +
                        "       TYPE_NAME(P.user_type_id) AS parameter_data_type," +
                        "       P.max_length AS parameter_max_length," +
                        "       P.is_output AS is_output_parameter " +
                        "FROM sys.objects AS SO " +
                        "     INNER JOIN sys.parameters AS P " +
                        "       ON SO.OBJECT_ID = P.OBJECT_ID " +
                        "ORDER BY SCHEMA_NAME(SCHEMA_ID), SO.name, P.parameter_id";
                }

                // ReSharper restore CommentTypo
                // ReSharper restore StringLiteralTypo

                var cmd = dbTools.CreateCommand(sqlQuery);

                dbTools.GetQueryResultsDataTable(cmd, out var queryResults);

                var functionAndProcedureInfo = new DatabaseFunctionAndProcedureInfo(databaseName);
                RegisterEvents(functionAndProcedureInfo);

                mDatabaseFunctionAndProcedureArguments.Add(databaseName, functionAndProcedureInfo);

                var currentSchema = string.Empty;
                var currentObject = string.Empty;
                var currentObjectWithSchema = string.Empty;
                var currentObjectInfo = new FunctionOrProcedureInfo(string.Empty, string.Empty, false);

                // Keys in this dictionary are argument names, values are ordinal position (1-based)
                // This dictionary is only used with Postgres
                var ordinalPositionByName = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                // Keys in this dictionary are ordinal position (1-based), values are data type
                // This dictionary is only used with Postgres
                var dataTypeByOrdinalPosition = new Dictionary<int, string>();

                foreach (DataRow result in queryResults.Rows)
                {
                    var schema = result["object_schema"].CastDBVal<string>() ?? string.Empty;
                    var objectName = result["object_name"].CastDBVal<string>() ?? string.Empty;
                    var objectType = result["object_type"].CastDBVal<string>() ?? string.Empty;

                    if (!currentSchema.Equals(schema) || !currentObject.Equals(objectName))
                    {
                        currentSchema = schema;
                        currentObject = objectName;

                        if (string.IsNullOrWhiteSpace(currentSchema))
                        {
                            currentObjectWithSchema = objectName;
                        }
                        else
                        {
                            currentObjectWithSchema = string.Format("{0}.{1}", currentSchema, objectName);
                        }

                        string objectTypeName;

                        switch (objectType.ToUpper())
                        {
                            case "F":
                                if (mOptions.UsePostgresSchema)
                                {
                                    var returnsTable = result["returns_table"].CastDBVal<bool>();

                                    objectTypeName = returnsTable ? "function (table-valued)" : "function (scalar)";
                                }
                                else
                                {
                                    objectTypeName = "function (??)";
                                }

                                break;

                            case "SQL_TABLE_VALUED_FUNCTION":
                                objectTypeName = "function (table-valued)";
                                break;

                            case "SQL_SCALAR_FUNCTION":
                                objectTypeName = "function (scalar)";
                                break;

                            case "P":
                            case "CLR_STORED_PROCEDURE":
                            case "SQL_STORED_PROCEDURE":
                                objectTypeName = "procedure";
                                break;

                            default:
                                objectTypeName = "unknown object type";
                                break;
                        }

                        var isProcedure = objectTypeName.EndsWith("procedure");

                        currentObjectInfo = new FunctionOrProcedureInfo(objectName, objectTypeName, isProcedure);

                        functionAndProcedureInfo.AddFunctionOrProcedure(schema, objectName, currentObjectInfo);

                        if (mOptions.UsePostgresSchema)
                        {
                            var argumentList = result["argument_list"].CastDBVal<string>();
                            DetermineArgumentOrdinalPositions(currentObjectWithSchema, argumentList, ordinalPositionByName, dataTypeByOrdinalPosition);
                        }
                    }

                    var argumentInfo = GetArgumentInfo(
                        currentObjectWithSchema,
                        result,
                        mOptions.UsePostgresSchema,
                        currentObjectInfo.IsProcedure,
                        ordinalPositionByName,
                        dataTypeByOrdinalPosition);

                    currentObjectInfo.ArgumentList.Add(argumentInfo);
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

                var storedProcedureInfo = new Dictionary<string, FunctionOrProcedureInfo>(StringComparer.OrdinalIgnoreCase);

                foreach (var storedProcedureName in (from item in storedProcedureArguments select item.ProcedureName).Distinct())
                {
                    var procedureInfoLoaded = GetFunctionOrProcedureInfo(storedProcedureName, out var procedureInfo, out var targetDatabase);

                    if (!procedureInfoLoaded)
                    {
                        if (mOptions.IgnoreMissingStoredProcedures)
                        {
                            OnStatusEvent(
                                "{0,-25} stored procedure {1} does not exist in database {2} on server {3}",
                                mDbUpdater.CurrentConfigDB + ":",
                                storedProcedureName,
                                targetDatabase,
                                GetDatabaseServer());
                        }
                        else
                        {

                            OnWarningEvent(
                                "{0,-25} stored procedure {1} does not exist in database {2} on server {3}",
                                mDbUpdater.CurrentConfigDB + ":",
                                storedProcedureName,
                                targetDatabase,
                                GetDatabaseServer());

                            errorCount++;
                        }

                        continue;
                    }

                    storedProcedureInfo.Add(storedProcedureName, procedureInfo);
                }

                foreach (var procedureArgument in storedProcedureArguments)
                {
                    if (!IsOperationsProcedure(procedureArgument.ProcedureName))
                    {
                        ValidateBasicField("Stored procedure argument", procedureArgument, ref errorCount, out _);
                    }

                    if (procedureArgument.ArgumentName != procedureArgument.ArgumentName.Trim())
                    {
                        OnWarningEvent(
                            "{0,-25} argument name has leading or trailing spaces; see '{1}' (ID {2}) for procedure {3}",
                            mDbUpdater.CurrentConfigDB + ":",
                            procedureArgument.ArgumentName,
                            procedureArgument.ID,
                            procedureArgument.ProcedureName);

                        errorCount++;
                    }

                    if (!storedProcedureInfo.TryGetValue(procedureArgument.ProcedureName, out var procedureInfo))
                    {
                        // Stored procedure is missing; the user has already been warned
                        continue;
                    }

                    string argumentName;
                    bool nameIsQuoted;

                    // Check for quoted argument names
                    if (procedureArgument.ArgumentName.StartsWith("\"") &&
                        procedureArgument.ArgumentName.EndsWith("\""))
                    {
                        argumentName = procedureArgument.ArgumentName.Substring(1, procedureArgument.ArgumentName.Length - 2);
                        nameIsQuoted = true;
                    }
                    else
                    {
                        argumentName = procedureArgument.ArgumentName;
                        nameIsQuoted = false;
                    }

                    var matchesDatabaseInfo = false;

                    foreach (var databaseArgument in procedureInfo.ArgumentList)
                    {
                        if (!databaseArgument.ArgumentName.Equals(argumentName))
                            continue;

                        matchesDatabaseInfo = true;
                        break;
                    }

                    if (matchesDatabaseInfo)
                        continue;

                    foreach (var databaseArgument in procedureInfo.ArgumentList)
                    {
                        if (!databaseArgument.ArgumentName.Equals(argumentName, StringComparison.OrdinalIgnoreCase))
                            continue;

                        if (nameIsQuoted)
                        {
                            OnWarningEvent(
                                "{0,-25} case mismatch for {1}; ID {2} has quoted name {3} vs. {4} in the database",
                                mDbUpdater.CurrentConfigDB + ":",
                                procedureArgument.ProcedureName,
                                procedureArgument.ID,
                                procedureArgument.ArgumentName,
                                databaseArgument.ArgumentName
                            );

                            errorCount++;
                            matchesDatabaseInfo = true;
                            break;
                        }

                        // The name is not quoted

                        if (mOptions.UsePostgresSchema || !mOptions.RequireMatchingCaseForProcedureArgumentNames)
                        {
                            string ignoreReason;
                            string databaseArgumentName;

                            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
                            if (mOptions.UsePostgresSchema)
                            {
                                // On Postgres, uppercase letters are auto-changed to lowercase
                                ignoreReason = string.Format("auto-converts to {0}", argumentName.ToLower());
                                databaseArgumentName = databaseArgument.ArgumentName;
                            }
                            else
                            {
                                // On SQL Server, case does not matter for unquoted argument names
                                ignoreReason = "case is ignored on SQL Server";
                                databaseArgumentName = databaseArgument.ArgumentName.ToLower();
                            }

                            if (databaseArgumentName.Equals(argumentName.ToLower()))
                            {
                                OnDebugEvent(
                                    "{0,-25} case mismatch for {1}; ID {2} has {3} vs. {4} in the database; not an error since {5}",
                                    mDbUpdater.CurrentConfigDB + ":",
                                    procedureArgument.ProcedureName,
                                    procedureArgument.ID,
                                    procedureArgument.ArgumentName,
                                    databaseArgumentName,
                                    ignoreReason
                                );
                                matchesDatabaseInfo = true;
                                break;
                            }
                        }

                        OnWarningEvent(
                            "{0,-25} case mismatch for {1}; ID {2} has {3} vs. {4} in the database",
                            mDbUpdater.CurrentConfigDB + ":",
                            procedureArgument.ProcedureName,
                            procedureArgument.ID,
                            procedureArgument.ArgumentName,
                            databaseArgument.ArgumentName
                        );

                        errorCount++;
                        matchesDatabaseInfo = true;
                        break;
                    }

                    if (matchesDatabaseInfo)
                        continue;

                    OnWarningEvent(
                        "{0,-25} stored procedure {1} does not have argument {2} (see ID {3})",
                        mDbUpdater.CurrentConfigDB + ":",
                        procedureArgument.ProcedureName,
                        procedureArgument.ArgumentName,
                        procedureArgument.ID);

                    errorCount++;
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
