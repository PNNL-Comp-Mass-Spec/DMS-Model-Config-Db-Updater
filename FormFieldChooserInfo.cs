namespace DMSModelConfigDbUpdater
{
    internal class FormFieldChooserInfo : BasicField
    {
        public string CrossReference { get; }

        public FormFieldChooserInfo(int id, string formFieldName, string crossReference) : base(id, formFieldName)
        {
            CrossReference = crossReference;
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
