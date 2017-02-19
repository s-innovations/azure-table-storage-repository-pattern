using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace SInnovations.Azure.TableStorageRepository.Spatial.DotSpatial
{
    internal class GeoJsonReader
    {
        private readonly IShapeConverter _shapeConverter;

        public GeoJsonReader(IShapeConverter shapeConverter)
        {
            _shapeConverter = shapeConverter;
        }

        public object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null)
                return null;

            var json = JToken.ReadFrom(reader);
            object result;
            TryRead(json as JObject, out result); // Throw exception?
            return result;
        }

        protected virtual bool TryRead(JObject json, out object result)
        {
            if (TryParseGeometry(json, out result))
                return true;
            if (TryParseFeature(json, out result))
                return true;
            if (TryParseFeatureCollection(json, out result))
                return true;

            result = null;
            return false;
        }

        private bool TryParseTypeString(JObject obj, out string result)
        {
            JToken type = null;
            if (obj != null)
                obj.TryGetValue("type", out type);

            var value = type as JValue;
            if (value != null)
                result = value.Value as string;
            else
                result = null;

            return type != null;
        }

        private bool TryParseFeatureCollection(JObject obj, out object result)
        {
            result = null;
            string typeString;
            if (TryParseTypeString(obj, out typeString) && typeString.ToLowerInvariant() == "featurecollection")
            {
                JToken feats;
                if (obj.TryGetValue("features", out feats))
                {
                    var features = feats as JArray;
                    if (features != null)
                    {
                        var temp = new object[features.Count];
                        for (var index = 0; index < features.Count; index++)
                        {
                            var geometry = features[index];
                            if (!TryParseFeature((JObject)geometry, out temp[index]))
                                return false;
                        }
                        result = _shapeConverter.ToFeatureCollection(temp);
                        return true;
                    }
                }
            }
            return false;
        }

        private bool TryParseFeature(JObject obj, out object result)
        {
            string typeString;
            if (TryParseTypeString(obj, out typeString) && typeString.ToLowerInvariant() == "feature")
            {
                JToken geometry;
                object geo;
                if (obj.TryGetValue("geometry", out geometry) && TryParseGeometry((JObject)geometry, out geo))
                {
                    JToken prop;
                    Dictionary<string, object> pr = null;
                    if (obj.TryGetValue("properties", out prop) && prop is JObject)
                    {
                        var props = (JObject)prop;
                        if (props.Count > 0)
                        {
                            pr = Enumerable.ToDictionary< KeyValuePair<string, JToken>,string,object>(props, x => x.Key, x => SantizeJObjects(x.Value));
                        }
                    }

                    object id = null;
                    JToken idToken;
                    if (obj.TryGetValue("id", out idToken))
                    {
                        id = SantizeJObjects(idToken);
                    }

                    result = _shapeConverter.ToFeature(geo, id, pr);
                    return true;
                }
            }
            result = null;
            return false;
        }

        private bool TryParseGeometry(JObject obj, out object result)
        {
            result = null;
            string typeString;
            if (!TryParseTypeString(obj, out typeString))
                return false;

            typeString = typeString.ToLowerInvariant();

            switch (typeString)
            {
                case "point":
                    return TryParsePoint(obj, out result);
                case "linestring":
                    return TryParseLineString(obj, out result);
                case "polygon":
                    return TryParsePolygon(obj, out result);
                case "multipoint":
                    return TryParseMultiPoint(obj, out result);
                case "multilinestring":
                    return TryParseMultiLineString(obj, out result);
                case "multipolygon":
                    return TryParseMultiPolygon(obj, out result);
                case "geometrycollection":
                    return TryParseGeometryCollection(obj, out result);
                default:
                    return false;
            }
        }

        private bool TryParsePoint(JObject obj, out object result)
        {
            result = null;
            JToken coord;
            if (obj.TryGetValue("coordinates", out coord))
            {
                var coordinates = coord as JArray;

                if (coordinates == null || coordinates.Count < 2)
                    return false;

                CoordinateInfo coordinate;
                if (TryParseCoordinate(coordinates, out coordinate))
                {
                    result = _shapeConverter.ToPoint(coordinate);
                    return true;
                }
            }
            return false;
        }


        private bool TryParseLineString(JObject obj, out object result)
        {
            JToken coord;
            if (obj.TryGetValue("coordinates", out coord))
            {
                var coordinates = coord as JArray;
                CoordinateInfo[] co;
                if (coordinates != null && TryParseCoordinateArray(coordinates, out co))
                {
                    result = _shapeConverter.ToLineString(co);
                    return true;
                }
            }
            result = null;
            return false;
        }

        private bool TryParsePolygon(JObject obj, out object result)
        {
            JToken coord;
            if (obj.TryGetValue("coordinates", out coord))
            {
                var coordinates = coord as JArray;

                CoordinateInfo[][] temp;
                if (coordinates != null && coordinates.Count > 0 && TryParseCoordinateArrayArray(coordinates, out temp))
                {
                    result = _shapeConverter.ToPolygon(temp);
                    return true;
                }
            }
            result = null;
            return false;
        }

        private bool TryParseMultiPoint(JObject obj, out object result)
        {
            JToken coord;
            if (obj.TryGetValue("coordinates", out coord))
            {
                var coordinates = coord as JArray;
                CoordinateInfo[] co;
                if (coordinates != null && TryParseCoordinateArray(coordinates, out co))
                {
                    result = _shapeConverter.ToMultiPoint(co);
                    return true;
                }
            }
            result = null;
            return false;
        }

        private bool TryParseMultiLineString(JObject obj, out object result)
        {
            JToken coord;
            if (obj.TryGetValue("coordinates", out coord))
            {
                var coordinates = coord as JArray;
                CoordinateInfo[][] co;
                if (coordinates != null && TryParseCoordinateArrayArray(coordinates, out co))
                {
                    result = _shapeConverter.ToMultiLineString(co);
                    return true;
                }
            }
            result = null;
            return false;
        }

        private bool TryParseMultiPolygon(JObject obj, out object result)
        {
            JToken coord;
            if (obj.TryGetValue("coordinates", out coord))
            {
                var coordinates = coord as JArray;
                CoordinateInfo[][][] co;
                if (coordinates != null && TryParseCoordinateArrayArrayArray(coordinates, out co))
                {
                    result = _shapeConverter.ToMultiPolygon(co);
                    return true;
                }
            }
            result = null;
            return false;
        }

        private bool TryParseGeometryCollection(JObject obj, out object result)
        {
            result = null;
            JToken geom;
            if (obj.TryGetValue("geometries", out geom))
            {
                var geometries = geom as JArray;

                if (geometries != null)
                {
                    var temp = new object[geometries.Count];
                    for (var index = 0; index < geometries.Count; index++)
                    {
                        var geometry = geometries[index];
                        if (!TryParseGeometry((JObject)geometry, out temp[index]))
                            return false;
                    }
                    result = _shapeConverter.ToGeometryCollection(temp);
                    return true;
                }
            }
            return false;
        }

        private bool TryParseCoordinate(JArray coordinates, out CoordinateInfo result)
        {
            if (coordinates != null && coordinates.Count > 1 && coordinates.All(x => x is JValue))
            {
                var vals = coordinates.Cast<JValue>().ToList();
                if (vals.All(x => x.Type == JTokenType.Float || x.Type == JTokenType.Integer))
                {
                    result = new CoordinateInfo
                    {
                        X = Convert.ToDouble(vals[0].Value),
                        Y = Convert.ToDouble(vals[1].Value),
                        Z = vals.Count > 2 ? Convert.ToDouble(vals[2].Value) : (double?)null,
                        M = vals.Count > 3 ? Convert.ToDouble(vals[3].Value) : (double?)null
                    };
                    return true;
                }
            }
            result = null;
            return false;
        }

        private bool TryParseCoordinateArray(JArray coordinates, out CoordinateInfo[] result)
        {
            result = null;
            if (coordinates == null)
                return false;

            var valid = coordinates.All(x => x is JArray);
            if (!valid)
                return false;

            var tempResult = new CoordinateInfo[coordinates.Count];
            for (var index = 0; index < coordinates.Count; index++)
            {
                if (!TryParseCoordinate((JArray)coordinates[index], out tempResult[index]))
                    return false;
            }
            result = tempResult;
            return true;
        }

        private bool TryParseCoordinateArrayArray(JArray coordinates, out CoordinateInfo[][] result)
        {
            result = null;
            if (coordinates == null)
                return false;

            var valid = coordinates.All(x => x is JArray);
            if (!valid)
                return false;

            var tempResult = new CoordinateInfo[coordinates.Count][];
            for (var index = 0; index < coordinates.Count; index++)
            {
                if (!TryParseCoordinateArray((JArray)coordinates[index], out tempResult[index]))
                    return false;
            }
            result = tempResult;
            return true;
        }

        private bool TryParseCoordinateArrayArrayArray(JArray coordinates, out CoordinateInfo[][][] result)
        {
            result = null;
            if (coordinates == null)
                return false;

            var valid = coordinates.All(x => x is JArray);
            if (!valid)
                return false;

            var tempResult = new CoordinateInfo[coordinates.Count][][];
            for (var index = 0; index < coordinates.Count; index++)
            {
                if (!TryParseCoordinateArrayArray((JArray)coordinates[index], out tempResult[index]))
                    return false;
            }
            result = tempResult;
            return true;
        }

        private object SantizeJObjects(object obj)
        {
            var JArray = obj as JArray;
            if (JArray != null)
                return JArray.Select(SantizeJObjects).ToArray();

            var jObject = obj as JObject;
            if (jObject != null)
                return Enumerable.ToDictionary<KeyValuePair<string,JToken>,string,object>(jObject, x => x.Key, x => SantizeJObjects(x));

            return obj;
        }
    }
}