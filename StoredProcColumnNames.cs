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
                "GetDatasetStatsByCampaign" => new List<string>
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
                },
                "GetPackageDatasetJobToolCrosstab" => new List<string>
                {
                    "Dataset",
                    "Jobs",
                    "id"
                },
                "FindExistingJobsForRequest" => new List<string>
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
                },
                "FindMatchingDatasetsForJobRequest" => new List<string>
                {
                    "Sel",
                    "Dataset",
                    "Jobs",
                    "New",
                    "Busy",
                    "Complete",
                    "Failed",
                    "Holding"
                },
                "GetCurrentMangerActivity" => new List<string>
                {
                    "Source",
                    "When",
                    "Who",
                    "What",
                    "alert"
                },
                "PredefinedAnalysisDatasets" => new List<string>
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
                },
                "EvaluatePredefinedAnalysisRules" => new List<string>
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
                "EvaluatePredefinedAnalysisRulesMDS" => new List<string>
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
                },
                "ReportProductionStats" => new List<string>
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
                },
                "GetProteinCollectionMemberDetail" => new List<string>
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
                },
                "GetFactorCrosstabByBatch" => new List<string>
                {
                    "Sel",
                    "BatchID",
                    "Name",
                    "Status",
                    "Dataset_ID",
                    "Request",
                    "Block",
                    "Run Order"
                },
                "GetRequestedRunFactorsForEdit" => new List<string>
                {
                    "Sel",
                    "BatchID",
                    "Experiment",
                    "Dataset",
                    "Name",
                    "Status",
                    "Request"
                },
                "ReportRequestDaily" => new List<string>
                {
                    "Date",
                    "Total entered",
                    "Remaining in request/schedule queue",
                    "In history list",
                    "Datasets Created"
                },
                "ReportTissueUsageStats" => new List<string>
                {
                    "Tissue_ID",
                    "Tissue",
                    "Experiments",
                    "Exp_Created_Min",
                    "Exp_Created_Max",
                    "Organism_First",
                    "Organism_Last",
                    "Campaign_First",
                    "Campaign_Last"
                },
                "GetMonthlyInstrumentUsageReport" => new List<string>
                {
                    "Instrument",
                    "EMSL_Inst_ID",
                    "Start",
                    "Type",
                    "Minutes",
                    "Proposal",
                    "Usage",
                    "Users",
                    "Operator",
                    "Comment",
                    "Year",
                    "Month",
                    "Dataset_ID",
                    "Start",
                    "Type",
                    "Minutes",
                    "Proposal",
                    "Usage",
                    "Comment",
                    "Dataset_ID",
                    "Type",
                    "Minutes",
                    "Percentage",
                    "Usage",
                    "Proposal"
                },
                _ => new List<string>()
            };

            return columnNames.Count > 0;
        }
    }
}
