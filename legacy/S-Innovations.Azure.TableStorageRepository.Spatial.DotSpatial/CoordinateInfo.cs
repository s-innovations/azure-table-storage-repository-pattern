namespace SInnovations.Azure.TableStorageRepository.Spatial.DotSpatial
{
    internal class CoordinateInfo
    {
        public double X { get; set; }
        public double Y { get; set; }
        public double? Z { get; set; }
        public double? M { get; set; }

        public Dimensions GetDimensions()
        {
            if (Z.HasValue && M.HasValue)
                return (Dimensions.Z | Dimensions.M);
            if (Z.HasValue)
                return Dimensions.Z;
            if (M.HasValue)
                return Dimensions.M;
            return Dimensions.XY;
        }
    }
}