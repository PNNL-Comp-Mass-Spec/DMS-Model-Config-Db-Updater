using System;
using System.Collections.Generic;

namespace DMSModelConfigDbUpdater
{
    internal class GeneralParameters
    {
        public enum ParameterType
        {
            DatabaseGroup = 0,
            DetailReportView = 1,
            DetailReportDataIdColumn = 2,
            EntryPageView = 3,
            EntryPageDataIdColumn = 4,
            EntryPageDataColumns = 5,
            ListReportView = 6,
            ListReportDataColumns = 7,
            ListReportSortColumn = 8,
            PostSubmissionDetailId = 9,
            DetailReportSP = 10,
            ListReportSP = 11,
            EntryPageSP = 12,
            OperationsSP = 13
        }

        public Dictionary<ParameterType, string> FieldDescriptions { get; }

        public Dictionary<ParameterType, string> FieldNames { get; }

        public Dictionary<ParameterType, string> Parameters { get; }

        public GeneralParameters()
        {
            Parameters = new Dictionary<ParameterType, string>();

            foreach (ParameterType param in Enum.GetValues(typeof(ParameterType)))
            {
                Parameters.Add(param, string.Empty);
            }

            FieldDescriptions = new Dictionary<ParameterType, string>
            {
                { ParameterType.DatabaseGroup, "database group" },
                { ParameterType.DetailReportView, "detail report view" },
                { ParameterType.DetailReportDataIdColumn, "detail report data ID column" },
                { ParameterType.EntryPageView, "entry page view" },
                { ParameterType.EntryPageDataIdColumn, "entry page data ID column" },
                { ParameterType.EntryPageDataColumns, "entry page data columns" },
                { ParameterType.ListReportView, "list report view" },
                { ParameterType.ListReportDataColumns, "list report data columns" },
                { ParameterType.ListReportSortColumn, "list report sort column" },
                { ParameterType.PostSubmissionDetailId, "post submission detail ID" },
                { ParameterType.DetailReportSP, "detail report stored procedure" },
                { ParameterType.ListReportSP, "list report stored procedure" },
                { ParameterType.EntryPageSP, "entry page stored procedure" },
                { ParameterType.OperationsSP, "operations stored procedure" }
            };

            // The field names should all be lower case
            FieldNames = new Dictionary<ParameterType, string>
            {
                { ParameterType.DatabaseGroup, "my_db_group" },
                { ParameterType.DetailReportView, "detail_report_data_table" },
                { ParameterType.DetailReportDataIdColumn, "detail_report_data_id_col" },
                { ParameterType.EntryPageView, "entry_page_data_table" },
                { ParameterType.EntryPageDataIdColumn, "entry_page_data_id_col" },
                { ParameterType.EntryPageDataColumns, "entry_page_data_cols" },
                { ParameterType.ListReportView, "list_report_data_table" },
                { ParameterType.ListReportDataColumns, "list_report_data_cols" },
                { ParameterType.ListReportSortColumn, "list_report_data_sort_col" },
                { ParameterType.PostSubmissionDetailId, "post_submission_detail_id" },
                { ParameterType.DetailReportSP, "detail_report_sproc" },
                { ParameterType.ListReportSP, "list_report_sproc" },
                { ParameterType.EntryPageSP, "entry_sproc" },
                { ParameterType.OperationsSP, "operations_sproc" }
            };
        }
    }
}
