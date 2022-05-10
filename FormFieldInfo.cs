namespace DMSModelConfigDbUpdater
{
    internal class FormFieldInfo
    {
        public int ID { get; }

        public string Name { get; }

        public string Label { get; }

        public string NewName { get; set; }

        public FormFieldInfo(int id, string name, string label)
        {
            ID = id;
            Name = name;
            Label = label;
            NewName = string.Empty;
        }
    }
}
