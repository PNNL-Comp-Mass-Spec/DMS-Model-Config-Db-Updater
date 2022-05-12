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

        public override string ToString()
        {
            return string.IsNullOrWhiteSpace(NewName)
                ? base.ToString()
                : string.Format("{0} -> {1}", FormFieldName, NewName);
        }
    }
}
