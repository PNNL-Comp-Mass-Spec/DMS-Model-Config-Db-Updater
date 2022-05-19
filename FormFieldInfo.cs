namespace DMSModelConfigDbUpdater
{
    internal class FormFieldInfo : BasicField
    {
        public string Label { get; }

        public string NewFieldName { get; set; }

        public FormFieldInfo(int id, string formFieldName, string label) : base (id, formFieldName)
        {
            Label = label;
            NewFieldName = string.Empty;
        }

        /// <summary>
        /// Show the original form field name, plus the new name if defined
        /// </summary>
        public override string ToString()
        {
            return string.IsNullOrWhiteSpace(NewFieldName)
                ? base.ToString()
                : string.Format("{0} -> {1}", FieldName, NewFieldName);
        }
    }
}
