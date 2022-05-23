namespace DMSModelConfigDbUpdater
{
    internal class BasicField
    {
        /// <summary>
        /// Field id
        /// </summary>
        public int ID { get; }

        /// <summary>
        /// Field name
        /// </summary>
        public string FieldName { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="id"></param>
        /// <param name="fieldName"></param>
        public BasicField(int id, string fieldName)
        {
            ID = id;
            FieldName = fieldName;
        }

        /// <summary>
        /// Show the field name
        /// </summary>
        public override string ToString()
        {
            return FieldName;
        }
    }
}
