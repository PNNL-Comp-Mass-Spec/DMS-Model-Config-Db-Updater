namespace DMSModelConfigDbUpdater
{
    internal class FunctionOrProcedureArgumentInfo
    {
        /// <summary>
        /// Argument name
        /// </summary>
        public string ArgumentName { get; }

        /// <summary>
        /// Argument data type
        /// </summary>
        public string DataType{ get; }

        /// <summary>
        /// Argument direction (In, Out, or InOut)
        /// </summary>
        public GeneralParameters.ArgumentDirection Direction { get; }

        /// <summary>
        /// True if the argument is Out or InOut
        /// </summary>
        public bool IsOutputArgument => Direction is GeneralParameters.ArgumentDirection.Out or GeneralParameters.ArgumentDirection.InOut;

        /// <summary>
        /// Ordinal position
        /// </summary>
        public int OrdinalPosition { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="ordinalPosition"></param>
        /// <param name="argumentName"></param>
        /// <param name="dataType"></param>
        /// <param name="argumentDirection"></param>
        public FunctionOrProcedureArgumentInfo(int ordinalPosition, string argumentName, string dataType, GeneralParameters.ArgumentDirection argumentDirection = GeneralParameters.ArgumentDirection.In)
        {
            OrdinalPosition = ordinalPosition;
            ArgumentName = argumentName;
            DataType = dataType;
            Direction = argumentDirection;
        }

        /// <summary>
        /// Show the argument's ordinal position, name, type, and direction
        /// </summary>
        public override string ToString()
        {
            return string.Format("{0}: {1}, {2}, {3}", OrdinalPosition, ArgumentName, DataType, Direction);
        }
    }
}
