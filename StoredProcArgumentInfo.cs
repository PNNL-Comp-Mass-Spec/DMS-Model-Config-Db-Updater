namespace DMSModelConfigDbUpdater
{
    internal class StoredProcArgumentInfo : BasicField
    {
        /// <summary>
        /// Stored procedure argument name
        /// </summary>
        public string ArgumentName { get; }

        /// <summary>
        /// Stored procedure name
        /// </summary>
        public string ProcedureName { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="id"></param>
        /// <param name="formFieldName"></param>
        /// <param name="argumentName"></param>
        /// <param name="procedureName"></param>
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
