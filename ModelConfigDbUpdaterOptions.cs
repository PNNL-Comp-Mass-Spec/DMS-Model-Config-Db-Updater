using System;
using System.IO;
using System.Reflection;
using PRISM;

namespace DMSModelConfigDbUpdater
{
    public class ModelConfigDbUpdaterOptions
    {
        // Ignore Spelling: dataset, hotlink, Postgres, pre

        /// <summary>
        /// Program date
        /// </summary>
        public const string PROGRAM_DATE = "October 29, 2022";

        [Option("InputDirectory", "Input", "I", ArgPosition = 1, HelpShowsDefault = false, IsInputFilePath = false,
            HelpText = "Directory with the DMS model config database files to update\n" +
                       "The SQLite files should have the extension .db")]
        public string InputDirectory { get; set; }

        [Option("FilenameFilter", "FileFilter", "F", HelpShowsDefault = false, IsInputFilePath = false,
            HelpText = "Filter for the model config databases to process, e.g. dataset*.db")]
        public string FilenameFilter { get; set; }

        [Option("OutputDirectory", "Output", "O", HelpShowsDefault = false, IsInputFilePath = false,
            HelpText = "Directory to write updated files\n" +
                       "Treated as a path relative to the input files if not rooted\n" +
                       "If an empty string, updates files in-place")]
        public string OutputDirectory { get; set; }

        [Option("ViewColumnMap", "Map", "M", HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "View column map file (typically created by PgSqlViewCreatorHelper.exe)\n" +
                       "Tab-delimited file with three columns:\n" +
                       "View  SourceColumnName  NewColumnName")]
        public string ViewColumnMapFile { get; set; }

        [Option("TableNameMap", "TableNames", HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "Text file with table names (one name per line) used to track renamed tables\n" +
                       "(typically sent to DB_Schema_Export_Tool.exe via the DataTables parameter when using the ExistingDDL option " +
                       "to pre-process a DDL file prior to calling sqlserver2pgsql.pl)\n" +
                       "Tab-delimited file that must include columns SourceTableName and TargetTableName")]
        public string TableNameMapFile { get; set; }

        [Option("PreviewUpdates", "Preview", HelpShowsDefault = true,
            HelpText = "When true, show changes that would be made, but do not update any files")]
        public bool PreviewUpdates { get; set; }

        [Option("QuietMode", "Quiet", "Q", HelpShowsDefault = true,
            HelpText = "When true, show fewer messages")]
        public bool QuietMode { get; set; }

        [Option("RenameListReportView", "RenameList", HelpShowsDefault = true,
            HelpText = "When true, rename the list report view and columns\n" +
                       "View renames will either be based on data loaded from the table name map file, or by converting to snake case\n" +
                       "Column renames are based on data loaded from the view column map file, optionally replacing spaces with underscores if the name is not defined in the map file")]
        public bool RenameListReportViewAndColumns { get; set; }

        [Option("RenameDetailReportView", "RenameDetail", HelpShowsDefault = true,
            HelpText = "When true, rename the detail report view and columns")]
        public bool RenameDetailReportViewAndColumns { get; set; }

        [Option("RenameEntryPageView", "RenameEntry", HelpShowsDefault = true,
            HelpText = "When true, rename the entry page view and columns\n" +
                       "This also updates form field names in the model config DB tables, since form fields match the entry page view's column names")]
        public bool RenameEntryPageViewAndColumns { get; set; }

        [Option("RenameStoredProcedures", "RenameSPs", HelpShowsDefault = true,
            HelpText = "When true, rename the referenced stored procedures to use snake case (does not change argument names)")]
        public bool RenameStoredProcedures { get; set; }

        [Option("RenameUndefinedViews", "RenameUnknownViews", HelpShowsDefault = true,
            HelpText = "When true, snake case any view name that was not loaded from the View column map file")]
        public bool RenameUndefinedViews { get; set; } = true;

        [Option("ReplaceSpacesWithUnderscores", "ReplaceSpaces", HelpShowsDefault = true,
            HelpText = "When true, replace spaces in column names with underscores")]
        public bool ReplaceSpacesWithUnderscores { get; set; }

        [Option("SnakeCaseColumnNames", "SnakeCaseColumns", HelpShowsDefault = true,
            HelpText = "When true, snake case any column names that are not defined in the View column map file")]
        public bool SnakeCaseColumnNames { get; set; } = true;

        [Option("QuoteWithSquareBrackets", "SquareBrackets", HelpShowsDefault = true,
            HelpText = "When false, quote names with double quotes (PostgreSQL compatible)\nWhen true, quote names with square brackets (SQL Server compatible)")]
        public bool QuoteWithSquareBrackets { get; set; }

        [Option("ConvertHiddenColumnsToNoDisplay", "ConvertHiddenColumns", HelpShowsDefault = true,
            HelpText = "When true, look for columns that start with # and convert them to list report hotlink type no_display\n" +
                       "In addition, remove the # sign from the name")]
        public bool ConvertHiddenColumnsToNoDisplay { get; set; }

        [Option("UsePostgresSchema", "UsePgSchema", HelpShowsDefault = true,
            HelpText = "When true, if the object name does not already have a schema and the db_group for the page family is defined, " +
                       "preface object names with the Postgres schema that applies to the database group\n" +
                       "This should only be set to true if the DMS website is now retrieving data from Postgres and schema names need to be added to page families\n" +
                       "This should also be set to true when ValidateColumnNames is true and using a PostgreSQL database server")]
        public bool UsePostgresSchema { get; set; }

        [Option("ValidateColumnNames", "ValidateColumns", "Validate", HelpShowsDefault = true,
            HelpText = "When true, read column names used in each SQLite file and validate against the column names " +
                       "in the source tables or views for list reports, detail reports, and entry pages\n" +
                       "When this is true, the name map files are not loaded, and no object renaming is performed")]
        public bool ValidateColumnNamesWithDatabase { get; set; }

        [Option("DatabaseServer", "Server", HelpShowsDefault = true,
            HelpText = "Server name to contact to validate form field names against columns in tables or views\n" +
                       "Assumed to be SQL Server if UsePostgresSchema is false; otherwise, assumes PostgreSQL")]
        public string DatabaseServer { get; set; } = "Gigasax";

        [Option("ValidateColumnNamesIgnoreErrors", "ValidateIgnoreErrors", HelpShowsDefault = true,
            HelpText = "When true, ignore errors while validating model config DBs (errors will be reported, but all config DBs will be validated)")]
        public bool ValidateColumnNamesIgnoreErrors { get; set; }

        [Option("SaveValidationResults", "SaveValidation", "WriteResults", HelpShowsDefault = true,
            HelpText = "When true, save the validation results to a text file")]
        public bool SaveValidateResultsToFile { get; set; }

        [Option("ValidationResultsFileName", "ResultsFile", HelpShowsDefault = true,
            HelpText = "Validation results file name or path")]
        public string ValidateResultsFileName { get; set; } = "ValidationResults.txt";

        /// <summary>
        /// Constructor
        /// </summary>
        public ModelConfigDbUpdaterOptions()
        {
            InputDirectory = string.Empty;
            FilenameFilter = string.Empty;
            OutputDirectory = string.Empty;
            ViewColumnMapFile = string.Empty;
            TableNameMapFile = string.Empty;
        }

        /// <summary>
        /// Get the program version
        /// </summary>
        public static string GetAppVersion()
        {
            return Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
        }

        /// <summary>
        /// Show the options at the console
        /// </summary>
        public void OutputSetOptions()
        {
            Console.WriteLine("Options:");

            Console.WriteLine(" {0,-32} {1}", "Input Directory:", PathUtils.CompactPathString(InputDirectory, 80));

            if (!string.IsNullOrWhiteSpace(FilenameFilter))
            {
                Console.WriteLine(" {0,-32} {1}", "Filename Filter:", FilenameFilter);
            }

            Console.WriteLine(" {0,-32} {1}", "Output Directory:",
                string.IsNullOrWhiteSpace(OutputDirectory)
                    ? "n/a: updating files in-place"
                    : PathUtils.CompactPathString(OutputDirectory, 80));

            if (ValidateColumnNamesWithDatabase)
            {
                Console.WriteLine(" {0,-32} {1}", "Validate Column Names With DB:", ValidateColumnNamesWithDatabase);

                Console.WriteLine(" {0,-32} {1}", "Database Server:", DatabaseServer);

                Console.WriteLine(" {0,-32} {1}", "Continue Validating If Errors:", ValidateColumnNamesIgnoreErrors);

                Console.WriteLine(" {0,-32} {1}", "Save Validation Results:", SaveValidateResultsToFile);

                if (SaveValidateResultsToFile)
                {
                    var resultsFile = new FileInfo(ModelConfigDbUpdater.GetValidateResultsFilePath(InputDirectory, ValidateResultsFileName));

                    Console.WriteLine(" {0,-32} {1}", "Validation Results File:", PathUtils.CompactPathString(resultsFile.FullName, 80));
                }

                return;
            }

            Console.WriteLine(" {0,-32} {1}", "View Column Map File:", PathUtils.CompactPathString(ViewColumnMapFile, 80));

            if (!string.IsNullOrWhiteSpace(TableNameMapFile))
            {
                Console.WriteLine(" {0,-32} {1}", "Table Name Map File:", PathUtils.CompactPathString(TableNameMapFile, 80));
            }

            Console.WriteLine(" {0,-32} {1}", "Preview Updates:", PreviewUpdates);

            Console.WriteLine(" {0,-32} {1}", "Quiet Mode:", QuietMode);

            Console.WriteLine(" {0,-40} {1}", "Rename List Report View and Columns:", RenameListReportViewAndColumns);

            Console.WriteLine(" {0,-40} {1}", "Rename Detail Report View and Columns:", RenameDetailReportViewAndColumns);

            Console.WriteLine(" {0,-40} {1}", "Rename Entry Page View and Columns:", RenameEntryPageViewAndColumns);

            Console.WriteLine(" {0,-40} {1}", "Rename Stored Procedures:", RenameStoredProcedures);

            Console.WriteLine(" {0,-40} {1}", "Rename Undefined Views:", RenameUndefinedViews);

            Console.WriteLine(" {0,-40} {1}", "Replace Spaces With Underscores:", ReplaceSpacesWithUnderscores);

            Console.WriteLine(" {0,-40} {1}", "Snake Case Column Names:", SnakeCaseColumnNames);

            Console.WriteLine(" {0,-40} {1}", "Quote With Square Brackets:", QuoteWithSquareBrackets);

            Console.WriteLine(" {0,-40} {1}", "Convert Hidden Columns To no_display:", ConvertHiddenColumnsToNoDisplay);

            Console.WriteLine(" {0,-40} {1}", "Validate Column Names With DB:", ValidateColumnNamesWithDatabase);
        }

        /// <summary>
        /// Validate the options
        /// </summary>
        /// <returns>True if options are valid, false if /I or /M is missing</returns>
        public bool ValidateArgs(string parameterFilePath, out string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(InputDirectory))
            {
                errorMessage = "Use /I to specify the directory with the model config DBs to process";
                return false;
            }

            if (!ValidateColumnNamesWithDatabase && !ConvertHiddenColumnsToNoDisplay && string.IsNullOrWhiteSpace(ViewColumnMapFile))
            {
                errorMessage = "Use /M to specify the view column map file";
                return false;
            }

            if (InputDirectory.Trim().Equals(".") && !string.IsNullOrWhiteSpace(parameterFilePath))
            {
                // Set the input directory to the directory with the parameter file
                var parameterFile = new FileInfo(parameterFilePath);

                if (parameterFile.Directory == null)
                {
                    errorMessage = "Cannot determine the input directory; unable to determine the parent directory of the parameter file: " + parameterFilePath;
                    return false;
                }

                InputDirectory = parameterFile.Directory.FullName;
            }

            if (!string.IsNullOrWhiteSpace(OutputDirectory) && !Path.IsPathRooted(OutputDirectory))
            {
                var inputDirectory = new DirectoryInfo(InputDirectory);
                OutputDirectory = Path.Combine(inputDirectory.FullName, OutputDirectory);
            }

            errorMessage = string.Empty;

            return true;
        }
    }
}
