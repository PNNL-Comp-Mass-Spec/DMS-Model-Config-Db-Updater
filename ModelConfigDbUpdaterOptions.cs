﻿using System;
using System.Reflection;
using PRISM;

namespace DMSModelConfigDbUpdater
{
    public class ModelConfigDbUpdaterOptions
    {
        // Ignore Spelling: dataset, pre

        /// <summary>
        /// Program date
        /// </summary>
        public const string PROGRAM_DATE = "May 11, 2022";

        [Option("Input", "I", ArgPosition = 1, HelpShowsDefault = false, IsInputFilePath = false,
            HelpText = "Directory with the DMS model config database files to update\n" +
                       "The SQLite files should have the extension .db")]
        public string InputDirectory { get; set; }

        [Option("FilenameFilter", "FileFilter", "F", HelpShowsDefault = false, IsInputFilePath = false,
            HelpText = "Filter for the model config databases to process, e.g. dataset*.db")]
        public string FilenameFilter { get; set; }

        [Option("OutputDirectory", "Output", HelpShowsDefault = false, IsInputFilePath = false,
            HelpText = "Directory to write updated files\n" +
                       "Treated as a path relative to the input files if not rooted\n" +
                       "If an empty string, updates files in-place\n")]
        public string OutputDirectory { get; set; }

        [Option("Map", "M", HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "View column map file (typically created by PgSqlViewCreatorHelper.exe)\n" +
                       "Tab-delimited file with four columns:\n" +
                       "View  SourceColumnName  NewColumnName  IsColumnAlias")]
        public string ViewColumnMapFile { get; set; }

        [Option("TableNameMap", "TableNames", HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "Text file with table names (one name per line) used to track renamed tables\n" +
                       "(typically sent to DB_Schema_Export_Tool.exe via the DataTables parameter when using the ExistingDDL option " +
                       "to pre-process a DDL file prior to calling sqlserver2pgsql.pl)\n" +
                       "Tab-delimited file that must include columns SourceTableName and TargetTableName")]
        public string TableNameMapFile { get; set; }

        [Option("PreviewUpdates", "Preview", HelpShowsDefault = true,
            HelpText = "When true, show changes that would be made")]
        public bool PreviewUpdates { get; set; }

        [Option("RenameListReportView", "RenameList", HelpShowsDefault = true,
            HelpText = "When true, rename the list report view and columns")]
        public bool RenameListReportViewAndColumns { get; set; }

        [Option("RenameDetailReportView", "RenameDetail", HelpShowsDefault = true,
            HelpText = "When true, rename the detail report view and columns")]
        public bool RenameDetailReportViewAndColumns { get; set; }

        [Option("RenameEntryPageView", "RenameEntry", HelpShowsDefault = true,
            HelpText = "When true, rename the entry page view and columns\n" +
                       "This also updates field names in the model config DB tables, since field names match the entry page view's column names")]
        public bool RenameEntryPageViewAndColumns { get; set; }

        [Option("RenameStoredProcedures", "RenameSPs", HelpShowsDefault = true,
            HelpText = "When true, rename the referenced stored procedures to use snake case (does not change argument names)")]
        public bool RenameStoredProcedures { get; set; }

        [Option("UsePostgresSchema", "UsePgSchema", HelpShowsDefault = true,
            HelpText = "When true, if the object name does not already have a schema and the db_group for the page family is defined, " +
                       "preface object names with the PostgreSQL schema that applies to the database group\n" +
                       "This should only be set to true if the DMS website is now retrieving data from PostgreSQL and schema names need to be added to page families")]
        public bool UsePostgresSchema { get; set; }

        [Option("Verbose", "V", HelpShowsDefault = true,
            HelpText = "When true, display the old and new version of each updated line")]
        public bool VerboseOutput { get; set; }

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

            Console.WriteLine(" {0,-30} {1}", "Input Directory:", PathUtils.CompactPathString(InputDirectory, 80));

            if (!string.IsNullOrWhiteSpace(FilenameFilter))
            {
                Console.WriteLine(" {0,-30} {1}", "Filename Filter:", FilenameFilter);
            }

            Console.WriteLine(" {0,-30} {1}", "Output Directory:",
                string.IsNullOrWhiteSpace(OutputDirectory)
                    ? "n/a: updating files in-place"
                    : PathUtils.CompactPathString(OutputDirectory, 80));

            Console.WriteLine(" {0,-30} {1}", "View Column Map File:", PathUtils.CompactPathString(ViewColumnMapFile, 80));

            if (!string.IsNullOrWhiteSpace(TableNameMapFile))
            {
                Console.WriteLine(" {0,-30} {1}", "Table Name Map File:", PathUtils.CompactPathString(TableNameMapFile, 80));
            }

            Console.WriteLine(" {0,-30} {1}", "Preview Updates:", PreviewUpdates);

            Console.WriteLine(" {0,-40} {1}", "Rename List Report View and Columns:", RenameListReportViewAndColumns);

            Console.WriteLine(" {0,-40} {1}", "Rename Detail Report View and Columns:", RenameDetailReportViewAndColumns);

            Console.WriteLine(" {0,-40} {1}", "Rename Entry Page View and Columns:", RenameEntryPageViewAndColumns);

            Console.WriteLine(" {0,-40} {1}", "Rename Stored Procedures:", RenameStoredProcedures);

            Console.WriteLine(" {0,-40} {1}", "Verbose output:", VerboseOutput);

            Console.WriteLine();
        }

        /// <summary>
        /// Validate the options
        /// </summary>
        /// <returns>True if options are valid, false if /I or /M is missing</returns>
        public bool ValidateArgs(out string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(InputDirectory))
            {
                errorMessage = "Use /I to specify the directory with the model config DBs to process";
                return false;
            }

            if (string.IsNullOrWhiteSpace(ViewColumnMapFile))
            {
                errorMessage = "Use /M to specify the view column map file";
                return false;
            }

            errorMessage = string.Empty;

            return true;
        }
    }
}
