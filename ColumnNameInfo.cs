namespace DMSModelConfigDbUpdater
{
    internal class ColumnNameInfo
    {
        /// <summary>
        /// True if this is a column alias
        /// </summary>
        public bool IsColumnAlias { get; }

        /// <summary>
        /// New column name
        /// </summary>
        public string NewColumnName { get; }

        /// <summary>
        /// Source column name
        /// </summary>
        public string SourceColumnName { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="sourceColumnName"></param>
        /// <param name="newColumnName"></param>
        /// <param name="isColumnAlias"></param>
        public ColumnNameInfo(string sourceColumnName, string newColumnName, bool isColumnAlias)
        {
            SourceColumnName = sourceColumnName;

            NewColumnName = newColumnName;

            IsColumnAlias = isColumnAlias;
        }

        /// <summary>
        /// Show the original column name and the new column name
        /// </summary>
        public override string ToString()
        {
            return string.Format("{0} -> {1}", SourceColumnName, NewColumnName);
        }
    }
}
