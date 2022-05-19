namespace DMSModelConfigDbUpdater
{
    internal class PrimaryFilterInfo : BasicField
    {
        /// <summary>
        /// Primary filter label
        /// </summary>
        public string Label { get; set; }

        /// <summary>
        /// New field name
        /// </summary>
        public string NewFieldName { get; set; }

        /// <summary>
        /// Set this to true if NewFieldName or WhichArg are updated
        /// </summary>
        public bool Updated { get; set; }

        public PrimaryFilterInfo(int id, string label, string fieldName) : base(id, fieldName)
        {
            Label = label;
            NewFieldName = string.Empty;
            Updated = false;
        }

        /// <summary>
        /// Show the primary filter label plus either the original field name (from column "col") or new field name to store in "col"
        /// </summary>
        public override string ToString()
        {
            return string.Format("{0}: {1}", Label, Updated ? NewFieldName : FieldName);
        }
    }
}
