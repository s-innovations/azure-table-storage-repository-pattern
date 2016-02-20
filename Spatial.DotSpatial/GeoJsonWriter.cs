using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace SInnovations.Azure.TableStorageRepository.Spatial.DotSpatial
{
    internal class GeoJsonWriter
    {
        private readonly IShapeConverter _shapeConverter;

        public GeoJsonWriter(IShapeConverter shapeConverter)
        {
            _shapeConverter = shapeConverter;
        }

        public virtual void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteValue((object)null);
                return;
            }

            if (TryWriteGeometry(writer, value, serializer))
                return;

            if (TryWriteGeometryCollection(writer, value, serializer))
                return;

            if (TryWriteFeature(writer, value, serializer))
                return;

            if (TryWriteFeatureCollection(writer, value, serializer))
                return;

            throw new Exception("Error!");
        }

        public bool TryWriteFeature(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (!_shapeConverter.CanConvert(ObjectType.Feature))
                return false;

            ObjectType geomType = _shapeConverter.GetObjectType(value);
            if (geomType != ObjectType.Feature)
                return false;

            writer.WriteStartObject();
            writer.WritePropertyName("type");
            writer.WriteValue(Enum.GetName(typeof(ObjectType), ObjectType.Feature));

            object id;
            Dictionary<string, object> props;
            var geometry = _shapeConverter.FromFeature(value, out id, out props);

            if (id != null)
            {
                writer.WritePropertyName("id");
                serializer.Serialize(writer, id);
            }

            if (props != null && props.Count > 0)
            {
                writer.WritePropertyName("properties");
                serializer.Serialize(writer, props);
            }

            writer.WritePropertyName("geometry");
            serializer.Serialize(writer, geometry);

            writer.WriteEndObject();
            return true;
        }

        public bool TryWriteFeatureCollection(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (!_shapeConverter.CanConvert(ObjectType.FeatureCollection))
                return false;

            ObjectType geomType = _shapeConverter.GetObjectType(value);
            if (geomType != ObjectType.FeatureCollection)
                return false;

            writer.WriteStartObject();
            writer.WritePropertyName("type");
            writer.WriteValue(Enum.GetName(typeof(ObjectType), ObjectType.FeatureCollection));

            writer.WritePropertyName("features");
            writer.WriteStartArray();
            foreach (var feature in _shapeConverter.FromFeatureCollection(value))
                serializer.Serialize(writer, feature);
            writer.WriteEndArray();

            writer.WriteEndObject();
            return true;
        }

        public bool TryWriteGeometryCollection(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (!_shapeConverter.CanConvert(ObjectType.GeometryCollection))
                return false;

            ObjectType geomType = _shapeConverter.GetObjectType(value);
            if (geomType != ObjectType.GeometryCollection)
                return false;

            writer.WriteStartObject();
            writer.WritePropertyName("type");
            writer.WriteValue(Enum.GetName(typeof(ObjectType), geomType));

            writer.WritePropertyName("geometries");
            writer.WriteStartArray();
            foreach (var geometry in _shapeConverter.FromGeometryCollection(value))
                serializer.Serialize(writer, geometry);
            writer.WriteEndArray();

            writer.WriteEndObject();
            return true;
        }

        public bool TryWriteGeometry(JsonWriter writer, object value, JsonSerializer serializer)
        {
            ObjectType geomType = _shapeConverter.GetObjectType(value);

            if (!_shapeConverter.CanConvert(geomType))
                return false;

            switch (geomType)
            {
                case ObjectType.Point:
                case ObjectType.LineString:
                case ObjectType.Polygon:
                case ObjectType.MultiPoint:
                case ObjectType.MultiLineString:
                case ObjectType.MultiPolygon:
                    break;
                default:
                    return false;
            }

            writer.WriteStartObject();
            writer.WritePropertyName("type");
            writer.WriteValue(Enum.GetName(typeof(ObjectType), geomType));

            writer.WritePropertyName("coordinates");

            switch (geomType)
            {
                case ObjectType.Point:
                    WriteJsonCoordinate(writer, _shapeConverter.FromPoint(value));
                    break;
                case ObjectType.LineString:
                    WriteJsonCoordinates(writer, _shapeConverter.FromLineString(value));
                    break;

                case ObjectType.MultiPoint:
                    WriteJsonCoordinates(writer, _shapeConverter.FromMultiPoint(value));
                    break;

                case ObjectType.Polygon:
                    WriteJsonCoordinatesEnumerable(writer, _shapeConverter.FromPolygon(value));
                    break;

                case ObjectType.MultiLineString:
                    WriteJsonCoordinatesEnumerable(writer, _shapeConverter.FromMultiLineString(value));
                    break;

                case ObjectType.MultiPolygon:
                    WriteJsonCoordinatesEnumerable2(writer, _shapeConverter.FromMultiPolygon(value));
                    break;
            }

            writer.WriteEndObject();
            return true;
        }

        private static void WriteJsonCoordinate(JsonWriter writer, CoordinateInfo coordinate)
        {
            writer.WriteStartArray();

            writer.WriteValue(coordinate.X);
            writer.WriteValue(coordinate.Y);

            if (coordinate.Z.HasValue)
                writer.WriteValue(coordinate.Z.Value);

            if (coordinate.Z.HasValue && coordinate.M.HasValue)
                writer.WriteValue(coordinate.M.Value);

            writer.WriteEndArray();
        }

        protected static void WriteJsonCoordinates(JsonWriter writer, CoordinateInfo[] coordinates)
        {
            writer.WriteStartArray();
            foreach (var coordinate in coordinates)
                WriteJsonCoordinate(writer, coordinate);
            writer.WriteEndArray();
        }

        protected static void WriteJsonCoordinatesEnumerable(JsonWriter writer, CoordinateInfo[][] coordinates)
        {
            writer.WriteStartArray();
            foreach (var coordinate in coordinates)
                WriteJsonCoordinates(writer, coordinate);
            writer.WriteEndArray();
        }

        protected static void WriteJsonCoordinatesEnumerable2(JsonWriter writer, CoordinateInfo[][][] coordinates)
        {
            writer.WriteStartArray();
            foreach (var coordinate in coordinates)
                WriteJsonCoordinatesEnumerable(writer, coordinate);
            writer.WriteEndArray();
        }
    }
}