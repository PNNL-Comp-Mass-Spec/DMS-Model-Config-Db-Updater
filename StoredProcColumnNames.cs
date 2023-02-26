using System.Collections.Generic;

// ReSharper disable StringLiteralTypo

namespace DMSModelConfigDbUpdater
{
    internal static class StoredProcColumnNames
    {
        // ReSharper disable CommentTypo

        // Ignore Spelling: Crit, Excl, Labelling, Lvl, Pri, Proc, Prot, Sel, Sep

        // ReSharper enable CommentTypo

        /// <summary>
        /// Get the expected column names for the data table returned by the given stored procedure
        /// </summary>
        /// <param name="storedProcedureName">Stored procedure name</param>
        /// <param name="columnNames">Output: list of column names</param>
        /// <returns>True if a valid stored procedure name, false if not recognized</returns>
        public static bool GetColumns(string storedProcedureName, out List<string> columnNames)
        {
            columnNames = storedProcedureName switch
            {
                "get_dataset_stats_by_campaign" => new List<string>
                {
                    "campaign",
                    "work_package",
                    "pct_emsl_funded",
                    "runtime_hours",
                    "datasets",
                    "building",
                    "instrument",
                    "request_min",
                    "request_max",
                    "pct_total_runtime"
                },
                "get_package_dataset_job_tool_crosstab" => new List<string>
                {
                    "dataset",
                    "jobs",
                    "id"
                },
                "find_existing_jobs_for_request" => new List<string>
                {
                    "job",
                    "state",
                    "priority",
                    "request",
                    "created",
                    "start",
                    "finish",
                    "processor",
                    "dataset"
                },
                "find_matching_datasets_for_job_request" => new List<string>
                {
                    "sel",
                    "dataset",
                    "jobs",
                    "new",
                    "busy",
                    "complete",
                    "failed",
                    "holding"
                },
                "GetCurrentMangerActivity" => new List<string>
                {
                    "Source",
                    "When",
                    "Who",
                    "What",
                    "alert"
                },
                "predefined_analysis_datasets" => new List<string>
                {
                    "dataset",
                    "id",
                    "instrument_class",
                    "instrument",
                    "campaign",
                    "experiment",
                    "organism",
                    "exp_labelling",
                    "exp_comment",
                    "ds_comment",
                    "ds_type",
                    "ds_rating",
                    "rating",
                    "sep_type",
                    "tool",
                    "parameter_file",
                    "settings_file",
                    "protein_collections",
                    "legacy_fasta"
                },
                "EvaluatePredefinedAnalysisRules" => new List<string>  // Obsolete stored procedure
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
                    "Param File",
                    "Settings File",
                    // "Organism",
                    "Organism DB",
                    "Prot. Coll.",
                    "Prot. Opts.",
                    "Special Proc.",
                    "Priority",
                    "Processor Group"
                },
                "predefined_analysis_jobs_proc" => new List<string>
                {
                    "job",
                    "predefine_id",
                    "dataset",
                    "jobs",
                    "tool",
                    "pri",
                    "comment",
                    "param_file",
                    "settings_file",
                    "organism",
                    "protein_collections",
                    "protein_options",
                    "organism_db_name",
                    "special_processing",
                    "owner",
                    "export_mode"
                },
                "predefined_analysis_rules_proc" => new List<string>
                {
                    "step",
                    "level",
                    "seq",
                    "predefine_id",
                    "next_lvl",
                    "trigger_mode",
                    "export_mode",
                    "action",
                    "reason",
                    "notes",
                    "analysis_tool",
                    "instrument_class_criteria",
                    "instrument_criteria",
                    "instrument_exclusion",
                    "campaign_criteria",
                    "campaign_exclusion",
                    "experiment_criteria",
                    "experiment_exclusion",
                    "organism_criteria",
                    "dataset_criteria",
                    "dataset_exclusion",
                    "dataset_type",
                    "exp_comment_criteria",
                    "labelling_inclusion",
                    "labelling_exclusion",
                    "separation_type_criteria",
                    "scan_count_min",
                    "scan_count_max",
                    "param_file",
                    "settings_file",
                    "organism",
                    "protein_collections",
                    "protein_options",
                    "organism_db",
                    "special_processing",
                    "priority",
                    "processor_group"
                },
                "predefined_analysis_jobs_mds_proc" => new List<string>
                {
                    "id",
                    "job",
                    "predefine_id",
                    "dataset",
                    "jobs",
                    "tool",
                    "pri",
                    "comment",
                    "param_file",
                    "settings_file",
                    "organism",
                    "protein_collections",
                    "protein_options",
                    "organism_db_name",
                    "special_processing",
                    "owner",
                    "export_mode"
                },
                "report_production_stats" => new List<string>
                {
                    "instrument",
                    "total_datasets",
                    "days_in_range",
                    "datasets_per_day",
                    "blank_datasets",
                    "qc_datasets",
                    "bad_datasets",
                    "study_specific_datasets",
                    "study_specific_datasets_per_day",
                    "emsl_funded_study_specific_datasets",
                    "ef_study_specific_datasets_per_day",
                    "total_acq_time_days",
                    "study_specific_acq_time_days",
                    "ef_total_acq_time_days",
                    "ef_study_specific_acq_time_days",
                    "hours_acq_time_per_day",
                    "inst_",
                    "pct_inst_emsl_owned",
                    "ef_total_datasets",
                    "ef_datasets_per_day",
                    "pct_blank_datasets",
                    "pct_qc_datasets",
                    "pct_bad_datasets",
                    "pct_study_specific_datasets",
                    "pct_ef_study_specific_datasets",
                    "pct_ef_study_specific_by_acq_time",
                    "inst"
                },
                "get_protein_collection_member_detail" => new List<string>
                {
                    "protein_collection_id",
                    "protein_name",
                    "description",
                    "protein_sequence",
                    "monoisotopic_mass",
                    "average_mass",
                    "residue_count",
                    "molecular_formula",
                    "protein_id",
                    "reference_id",
                    "sha1_hash",
                    "member_id",
                    "sorting_index"
                },
                "get_factor_crosstab_by_batch" => new List<string>
                {
                    "sel",
                    "batch_id",
                    "name",
                    "status",
                    "dataset_id",
                    "request",
                    "block",
                    "run order"
                },
                "get_requested_run_factors_for_edit" => new List<string>
                {
                    "sel",
                    "batch_id",
                    "experiment",
                    "dataset",
                    "name",
                    "status",
                    "request"
                },
                "ReportRequestDaily" => new List<string>
                {
                    "Date",
                    "Total entered",
                    "Remaining in request/schedule queue",
                    "In history list",
                    "Datasets Created"
                },
                "report_tissue_usage_stats" => new List<string>
                {
                    "tissue_id",
                    "tissue",
                    "experiments",
                    "datasets",
                    "instruments",
                    "instrument_first",
                    "instrument_last",
                    "dataset_acq_time_min",
                    "dataset_acq_time_max",
                    "organism_first",
                    "organism_last",
                    "campaign_first",
                    "campaign_last",
                    "exp_created_min",
                    "exp_created_max"
                },
                "get_monthly_instrument_usage_report" => new List<string>
                {
                    "instrument",
                    "emsl_inst_id",
                    "start",
                    "type",
                    "minutes",
                    "proposal",
                    "usage",
                    "users",
                    "operator",
                    "comment",
                    "year",
                    "month",
                    "dataset_id",
                    "percentage",
                    "available",
                    "duration",
                    "interval",
                    "total",
                    "percentage"
                },
                _ => new List<string>()
            };

            return columnNames.Count > 0;
        }
    }
}
