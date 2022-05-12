namespace DMSModelConfigDbUpdater
{
    internal class ColumnNameInfo
    {
        public bool IsColumnAlias { get; }

        public string NewColumnName { get; }

        public string SourceColumnName { get; }

        public ColumnNameInfo(string sourceColumnName, string newColumnName, bool isColumnAlias)
        {
            SourceColumnName = sourceColumnName;

            NewColumnName = newColumnName;

            IsColumnAlias = isColumnAlias;
        }

        public override string ToString()
        {
            return string.Format("{0} -> {1}", SourceColumnName, NewColumnName);
        }
    }
}
