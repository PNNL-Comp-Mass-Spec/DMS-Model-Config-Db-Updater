namespace DMSModelConfigDbUpdater
{
    internal class ExternalSourceInfo : BasicField
    {
        /// <summary>
        /// Source page (typically a detail report)
        /// </summary>
        public string SourcePage { get; }

        /// <summary>
        /// Source page column
        /// </summary>
        public string SourceColumn { get; }

        /// <summary>
        /// Source data type
        /// </summary>
        /// <remarks>Should be ColName, Literal, or PostName, though ColName.action.Scrub is also used</remarks>
        public string SourceType { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="id"></param>
        /// <param name="fieldName"></param>
        /// <param name="sourcePage"></param>
        /// <param name="sourcePageColumn"></param>
        /// <param name="sourceDataType"></param>
        public ExternalSourceInfo(int id, string fieldName, string sourcePage, string sourcePageColumn, string sourceDataType) : base(id, fieldName)
        {
            SourcePage = sourcePage;
            SourceColumn = sourcePageColumn;
            SourceType = sourceDataType;
        }
    }
}
