namespace DMSModelConfigDbUpdater
{
    internal class HotlinkInfo : BasicField
    {
        // Ignore Spelling: hotlink

        /// <summary>
        /// Set this to true if this is a new hotlink that should be appended to the table
        /// </summary>
        public bool IsNewHotlink { get; set; }

        /// <summary>
        /// Hot link type
        /// </summary>
        public string LinkType { get; set; }

        /// <summary>
        /// New field name
        /// </summary>
        public string NewFieldName { get; set; }

        /// <summary>
        /// Target field name
        /// </summary>
        public string Target { get; set; }

        /// <summary>
        /// This is set to true if NewFieldName or WhichArg are updated
        /// It is also set to true if IsNewHotlink is true
        /// </summary>
        public bool Updated { get; set; }

        /// <summary>
        /// Column to use when constructing the link
        /// </summary>
        public string WhichArg { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="id"></param>
        /// <param name="fieldName"></param>
        /// <param name="linkType"></param>
        /// <param name="whichArg"></param>
        /// <param name="target"></param>
        /// <param name="isNew"></param>
        public HotlinkInfo(int id, string fieldName, string linkType, string whichArg, string target, bool isNew = false) : base(id, fieldName)
        {
            LinkType = linkType;
            WhichArg = whichArg;
            Target = target;

            NewFieldName = string.Empty;
            Updated = isNew;
            IsNewHotlink = isNew;
        }

        /// <summary>
        /// Show the original hot link field name, plus the new name if defined
        /// </summary>
        public override string ToString()
        {
            return string.IsNullOrWhiteSpace(NewFieldName)
                ? base.ToString()
                : string.Format("{0} -> {1}", FieldName, NewFieldName);
        }
    }
}
