# DMS Model Config Database Updater

This program updates column names in the SQLite files used by the DMS website to
retrieve data from the DMS database and display it for users. It reads information 
about renamed columns and views from tab-delimited text files and looks through 
the SQLite files for references to the parent views, updating the names of any 
renamed columns. View names are also updated.

## Console Switches

The DMS Model Config DB Updater is a console application, and must be run from the Windows command prompt.

```
DMSModelConfigDbUpdater.exe
  /I:InputDirectoryPath
  /F:FilenameFilter
  /O:OutputDirectory
  /Map:ViewColumnMapFile
  [/TableNameMap:TableNameMapFile]
  [/Preview]
  [/RenameList] [/RenameDetail] [/RenameEntry] [/RenameSPs]
  [/UsePgSchema]
  [/ParamFile:ParamFileName.conf] [/CreateParamFile]
```

The input directory should have the DMS model config database files to update
* The SQLite files should have the extension `.db`

Optionally use `/F` to filter the filenames to process
* For example, `/F:dataset*.db`

Optionally use `/O` to specify the output directory for writing updated SQLite files
* If the directory is not rooted (e.g. starts with C:), this is treated as a path relative to the input files 
* If the output directory is not defined, SQLite files will be updated in-place

The `/Map` file is is a tab-delimited text file with three columns
* The Map file matches the format of the renamed column file created by the PgSql View Creator Helper (PgSqlViewCreatorHelper.exe)
* Example data:

| View                               | SourceColumnName | NewColumnName |
|------------------------------------|------------------|---------------|
| "public"."v_analysis_job_entry"    | AJ_jobID         | job           |
| "public"."v_analysis_job_entry"    | AJ_priority      | priority      |
| "public"."v_analysis_job_entry"    | AJ_ToolName      | aj_tool_name  |
| "public"."v_analysis_job_entry"    | AJ_ParmFile      | aj_parm_file  |
| "public"."v_analysis_job_entry"    | AJ_batchID       | batch_id      |
| "public"."v_analysis_job_entry"    | Job_ID           | job           |
| "public"."v_dataset_list_report_2" | ID               | dataset_id    |
| "public"."v_dataset_list_report_2" | Dataset_Num      | dataset       |
| "public"."v_dataset_list_report_2" | Experiment_Num   | experiment    |
| "public"."v_dataset_list_report_2" | Campaign_Num     | campaign      |


Use `/TableNameMap` (or `/TableNames`) to optionally specify a tab-delimited text file listing old and new names for renamed tables and views
* The Table Name Map file matches the file defined for the `DataTables` parameter when using the DB Schema Export Tool (https://github.com/PNNL-Comp-Mass-Spec/DB-Schema-Export-Tool) to pre-process an existing DDL file
  * The text file must include columns `SourceTableName` and `TargetTableName`
* Example data (showing additional columns that are used by the DB Schema Export Tool, but are ignored by this program)

| SourceTableName           | TargetSchemaName | TargetTableName          | PgInsert  | KeyColumn(s)      |
|---------------------------|------------------|--------------------------|-----------|-------------------|
| T_Analysis_State_Name     | public           | t_analysis_job_state     | true      | job_state_id      |
| T_DatasetRatingName       | public           | t_dataset_rating_name    | true      | dataset_rating_id |
| T_Log_Entries             | public           | t_log_entries            | false     |                   |
| T_Job_Events              | cap              | t_job_Events             | false     |                   |
| T_Job_State_Name          | cap              | t_job_state_name         | true      | job               |
| T_Users                   | public           | t_users                  | true      | user_id           |
| V_Campaign_Cell_Culture   | public           | v_campaign_biomaterial   |           |                   |
| V_Cell_Culture            | public           | v_biomaterial            |           |                   |
| V_Experiment_Cell_Culture | public           | v_experiment_biomaterial |           |                   |
| V_Export_Cell_Culture     | public           | v_export_biomaterial     |           |                   |
| V_MyEMSL_Main_Metadata    | public           | v_myemsl_main_metadata   |           |                   |


Use `/Preview` to show changes that would be made, but do not update any files

Use `/RenameList` to rename the list report view and columns

Use `/RenameDetail` to rename the detail report view and columns

Use `/RenameEntry` to rename the entry page view and columns
* This also updates form field names in the model config DB tables, since form fields match the entry page view's column names

Use `/RenameSPs` to rename the referenced stored procedures to use snake case
* This does not change argument names

When `/UsePgSchema` is provided, if the object name does not already have a schema and the db_group for the page family is defined,
preface object names with the PostgreSQL schema that applies to the database group
* This should only be set to true if the DMS website is now retrieving data from PostgreSQL and schema names need to be added to page families

The processing options can be specified in a parameter file using `/ParamFile:Options.conf` or `/Conf:Options.conf`
* Define options using the format `ArgumentName=Value`
* Lines starting with `#` or `;` will be treated as comments
* Additional arguments on the command line can supplement or override the arguments in the parameter file

Use `/CreateParamFile` to create an example parameter file
* By default, the example parameter file content is shown at the console
* To create a file named Options.conf, use `/CreateParamFile:Options.conf`

## Contacts

Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) \
E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov\
Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://panomics.pnnl.gov/ or https://www.pnnl.gov/integrative-omics/
Source code: https://github.com/PNNL-Comp-Mass-Spec/PgSQL-Table-Creator-Helper

## License

Licensed under the 2-Clause BSD License; you may not use this program except
in compliance with the License.  You may obtain a copy of the License at
https://opensource.org/licenses/BSD-2-Clause

Copyright 2022 Battelle Memorial Institute
