namespace DMSModelConfigDbUpdater
{
    internal class FormFieldChooserInfo : BasicField
    {
        /// <summary>
        /// Chooser cross reference
        /// </summary>
        public string CrossReference { get; }

        /// <summary>
        /// List report helper name
        /// </summary>
        /// <remarks>
        /// Used by choosers of type list-report.helper
        /// </remarks>
        public string ListReportHelperName { get; set; }

        /// <summary>
        /// Pick list name
        /// </summary>
        /// <remarks>
        /// Pick lists are referenced by form field choosers of type picker.append, picker.list, picker.replace, etc., and also by link.list choosers
        /// </remarks>
        public string PickListName { get; set; }

        /// <summary>
        /// Chooser type
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="id"></param>
        /// <param name="formFieldName"></param>
        /// <param name="crossReference"></param>
        public FormFieldChooserInfo(int id, string formFieldName, string crossReference) : base(id, formFieldName)
        {
            CrossReference = crossReference;
            ListReportHelperName = string.Empty;
            PickListName = string.Empty;
            Type = string.Empty;
        }

        /// <summary>
        /// Show the original form field name, plus the cross reference name (from column "XRef"), if defined
        /// </summary>
        public override string ToString()
        {
            return string.IsNullOrWhiteSpace(CrossReference)
                ? base.ToString()
                : string.Format("{0} (cross reference {1})", FieldName, CrossReference);
        }
    }
}
