namespace DMSModelConfigDbUpdater
{
    internal class FormFieldInfo : BasicFormField
    {
        public string Label { get; }

        public string NewName { get; set; }

        public FormFieldInfo(int id, string formFieldName, string label) : base (id, formFieldName)
        {
            Label = label;
            NewName = string.Empty;
        }
    }
}
