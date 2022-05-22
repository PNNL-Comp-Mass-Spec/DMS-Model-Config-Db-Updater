namespace DMSModelConfigDbUpdater
{
    internal class StoredProcArgumentInfo : BasicField
    {
        public string ArgumentName { get; }

        public string ProcedureName { get; }

        public StoredProcArgumentInfo(int id, string formFieldName, string argumentName, string procedureName) : base(id, formFieldName)
        {
            ArgumentName = argumentName;
            ProcedureName = procedureName;
        }

        /// <summary>
        /// Show the form field name, the stored procedure name, and the argument name
        /// </summary>
        public override string ToString()
        {
            return string.Format("{0}: {1}=>{2}", FieldName, ProcedureName, ArgumentName);
        }
    }
}
