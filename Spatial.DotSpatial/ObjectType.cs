namespace SInnovations.Azure.TableStorageRepository.Spatial.DotSpatial
{
    internal enum ObjectType
    {
        Point,
        MultiPoint,
        LineString,
        MultiLineString,
        Polygon,
        MultiPolygon,
        GeometryCollection,
        Feature,
        FeatureCollection,

        Envelope,
        Circle
    }
}