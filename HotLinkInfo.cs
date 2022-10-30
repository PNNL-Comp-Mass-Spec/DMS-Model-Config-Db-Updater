namespace DMSModelConfigDbUpdater
{
    internal class HotLinkInfo : BasicField
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
        /// Set this to true if NewFieldName or WhichArg are updated
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
        /// <param name="isNew"></param>
        public HotLinkInfo(int id, string fieldName, string linkType, string whichArg, bool isNew = false) : base(id, fieldName)
        {
            LinkType = linkType;
            NewFieldName = string.Empty;
            Updated = false;
            WhichArg = whichArg;
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
