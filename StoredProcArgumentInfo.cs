namespace DMSModelConfigDbUpdater
{
    internal class StoredProcArgumentInfo : BasicField
    {
        public string ArgumentName { get; }

        public StoredProcArgumentInfo(int id, string formFieldName, string argumentName) : base(id, formFieldName)
        {
            ArgumentName = argumentName;
        }

        public override string ToString()
        {
            return string.Format("{0}: {1}", FieldName, ArgumentName);
        }
    }
}
