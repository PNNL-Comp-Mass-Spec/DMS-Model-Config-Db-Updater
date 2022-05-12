namespace DMSModelConfigDbUpdater
{
    internal class FormFieldChooserInfo : BasicFormField
    {
        public string CrossReference { get; }

        public FormFieldChooserInfo(int id, string formFieldName, string crossReference) : base(id, formFieldName)
        {
            CrossReference = crossReference;
        }

        public override string ToString()
        {
            return string.IsNullOrWhiteSpace(CrossReference)
                ? base.ToString()
                : string.Format("{0} (cross reference {1})", FormFieldName, CrossReference);
        }
    }
}
