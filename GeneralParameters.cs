namespace DMSModelConfigDbUpdater
{
    internal class GeneralParameters
    {
        public string DatabaseGroup { get; set; }

        public string DetailReportView { get; set; }

        public string DetailReportDataIdColumn { get; set; }

        public string DetailReportStoredProcedure { get; set; }

        public string EntryPageView { get; set; }

        public string EntryPageDataIdColumn { get; set; }

        public string EntryStoredProcedure { get; set; }

        public string ListReportView { get; set; }

        public string ListReportDataColumns { get; set; }

        public string ListReportSortColumn { get; set; }

        public string ListReportStoredProcedure { get; set; }

        public string OperationsStoredProcedure { get; set; }

        public string PostSubmissionDetailId { get; set; }

        public GeneralParameters()
        {
            DatabaseGroup = string.Empty;

            DetailReportView = string.Empty;
            DetailReportDataIdColumn = string.Empty;
            DetailReportStoredProcedure = string.Empty;

            EntryPageView = string.Empty;
            EntryPageDataIdColumn = string.Empty;
            EntryStoredProcedure = string.Empty;

            ListReportView = string.Empty;
            ListReportDataColumns = string.Empty;
            ListReportSortColumn = string.Empty;
            ListReportStoredProcedure = string.Empty;

            OperationsStoredProcedure = string.Empty;
            PostSubmissionDetailId = string.Empty;
        }
    }
}
