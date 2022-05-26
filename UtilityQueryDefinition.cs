namespace DMSModelConfigDbUpdater
{
    internal class UtilityQueryDefinition
    {
        /// <summary>
        /// Query ID
        /// </summary>
        public int ID { get; }

        /// <summary>
        /// Query name
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Label
        /// </summary>
        public string Label { get; set; }

        /// <summary>
        /// Database
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// Table or view to query
        /// </summary>
        public string Table { get; set; }

        /// <summary>
        /// Column names to retrieve
        /// </summary>
        /// <remarks>* to retrieve all columns</remarks>
        public string Columns { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="id">Utility query ID</param>
        /// <param name="name">Utility query (aka ad hoc query) name</param>
        public UtilityQueryDefinition(int id, string name)
        {
            ID = id;
            Name = name;
        }
    }
}
