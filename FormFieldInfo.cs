namespace DMSModelConfigDbUpdater
{
    internal class FormFieldInfo : BasicField
    {
        /// <summary>
        /// Form field label
        /// </summary>
        public string Label { get; }

        /// <summary>
        /// New form field name
        /// </summary>
        public string NewFieldName { get; set; }

        public FormFieldInfo(int id, string formFieldName, string label) : base (id, formFieldName)
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="id"></param>
        /// <param name="formFieldName"></param>
        /// <param name="label"></param>
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
