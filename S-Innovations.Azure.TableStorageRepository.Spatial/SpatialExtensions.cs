using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json.Linq;
using SInnovations.Azure.TableStorageRepository.Spatial;
using SInnovations.Azure.TableStorageRepository.TableRepositories;

namespace SInnovations.Azure.TableStorageRepository
{
    public class TileSystem
    {
        private const double EarthRadius = 6378137;
        private const double MinLatitude = -85.05112878;
        private const double MaxLatitude = 85.05112878;
        private const double MinLongitude = -180;
        private const double MaxLongitude = 180;

        private int _tileSize;
        private double _initialResolution;
        private const double OriginShift = 2 * Math.PI * EarthRadius / 2.0;

        public TileSystem(int tileSize = 256)
        {
            _tileSize = tileSize;
            _initialResolution = 2 * Math.PI * EarthRadius / _tileSize;
        }

        /// <summary>
        /// Clips a number to the specified minimum and maximum values.
        /// </summary>
        /// <param name="n">The number to clip.</param>
        /// <param name="minValue">Minimum allowable value.</param>
        /// <param name="maxValue">Maximum allowable value.</param>
        /// <returns>The clipped value.</returns>
        private static double Clip(double n, double minValue, double maxValue)
        {
            return Math.Min(Math.Max(n, minValue), maxValue);
        }



        /// <summary>
        /// Determines the map width and height (in pixels) at a specified level
        /// of detail.
        /// </summary>
        /// <param name="levelOfDetail">Level of detail, from 1 (lowest detail)
        /// to 23 (highest detail).</param>
        /// <returns>The map width and height in pixels.</returns>
        public static uint MapSize(int levelOfDetail)
        {
            return (uint)256 << levelOfDetail;
        }



        /// <summary>
        /// Determines the ground resolution (in meters per pixel) at a specified
        /// latitude and level of detail.
        /// </summary>
        /// <param name="latitude">Latitude (in degrees) at which to measure the
        /// ground resolution.</param>
        /// <param name="levelOfDetail">Level of detail, from 1 (lowest detail)
        /// to 23 (highest detail).</param>
        /// <returns>The ground resolution, in meters per pixel.</returns>
        public static double GroundResolution(double latitude, int levelOfDetail)
        {
            latitude = Clip(latitude, MinLatitude, MaxLatitude);
            return Math.Cos(latitude * Math.PI / 180) * 2 * Math.PI * EarthRadius / MapSize(levelOfDetail);
        }



        /// <summary>
        /// Determines the map scale at a specified latitude, level of detail,
        /// and screen resolution.
        /// </summary>
        /// <param name="latitude">Latitude (in degrees) at which to measure the
        /// map scale.</param>
        /// <param name="levelOfDetail">Level of detail, from 1 (lowest detail)
        /// to 23 (highest detail).</param>
        /// <param name="screenDpi">Resolution of the screen, in dots per inch.</param>
        /// <returns>The map scale, expressed as the denominator N of the ratio 1 : N.</returns>
        public static double MapScale(double latitude, int levelOfDetail, int screenDpi)
        {
            return GroundResolution(latitude, levelOfDetail) * screenDpi / 0.0254;
        }



        /// <summary>
        /// Converts a point from latitude/longitude WGS-84 coordinates (in degrees)
        /// into pixel XY coordinates at a specified level of detail.
        /// </summary>
        /// <param name="latitude">Latitude of the point, in degrees.</param>
        /// <param name="longitude">Longitude of the point, in degrees.</param>
        /// <param name="levelOfDetail">Level of detail, from 1 (lowest detail)
        /// to 23 (highest detail).</param>
        /// <param name="pixelX">Output parameter receiving the X coordinate in pixels.</param>
        /// <param name="pixelY">Output parameter receiving the Y coordinate in pixels.</param>
        public static void LatLongToPixelXY(double latitude, double longitude, int levelOfDetail, out int pixelX, out int pixelY)
        {
            latitude = Clip(latitude, MinLatitude, MaxLatitude);
            longitude = Clip(longitude, MinLongitude, MaxLongitude);

            double x = (longitude + 180) / 360;
            double sinLatitude = Math.Sin(latitude * Math.PI / 180);
            double y = 0.5 - Math.Log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI);

            uint mapSize = MapSize(levelOfDetail);
            pixelX = (int)Clip(x * mapSize + 0.5, 0, mapSize - 1);
            pixelY = (int)Clip(y * mapSize + 0.5, 0, mapSize - 1);
        }



        /// <summary>
        /// Converts a pixel from pixel XY coordinates at a specified level of detail
        /// into latitude/longitude WGS-84 coordinates (in degrees).
        /// </summary>
        /// <param name="pixelX">X coordinate of the point, in pixels.</param>
        /// <param name="pixelY">Y coordinates of the point, in pixels.</param>
        /// <param name="levelOfDetail">Level of detail, from 1 (lowest detail)
        /// to 23 (highest detail).</param>
        /// <param name="latitude">Output parameter receiving the latitude in degrees.</param>
        /// <param name="longitude">Output parameter receiving the longitude in degrees.</param>
        public static void PixelXYToLatLong(int pixelX, int pixelY, int levelOfDetail, out double latitude, out double longitude)
        {
            double mapSize = MapSize(levelOfDetail);
            double x = (Clip(pixelX, 0, mapSize - 1) / mapSize) - 0.5;
            double y = 0.5 - (Clip(pixelY, 0, mapSize - 1) / mapSize);

            latitude = 90 - 360 * Math.Atan(Math.Exp(-y * 2 * Math.PI)) / Math.PI;
            longitude = 360 * x;
        }
        public double Resolution(int levelOfDefault)
        {
            return _initialResolution / (1 << levelOfDefault);
        }
        public void PixelsToMeters(int pixelX, int pixelY, int levelOfDetail, out double mx, out double my)
        {
            var res = Resolution(levelOfDetail);
            mx = pixelX * res - OriginShift;
            my = pixelY * res - OriginShift;
            // return mx, my
        }



        /// <summary>
        /// Converts pixel XY coordinates into tile XY coordinates of the tile containing
        /// the specified pixel.
        /// </summary>
        /// <param name="pixelX">Pixel X coordinate.</param>
        /// <param name="pixelY">Pixel Y coordinate.</param>
        /// <param name="tileX">Output parameter receiving the tile X coordinate.</param>
        /// <param name="tileY">Output parameter receiving the tile Y coordinate.</param>
        public static void PixelXYToTileXY(int pixelX, int pixelY, out int tileX, out int tileY)
        {
            tileX = pixelX / 256;
            tileY = pixelY / 256;
        }



        /// <summary>
        /// Converts tile XY coordinates into pixel XY coordinates of the upper-left pixel
        /// of the specified tile.
        /// </summary>
        /// <param name="tileX">Tile X coordinate.</param>
        /// <param name="tileY">Tile Y coordinate.</param>
        /// <param name="pixelX">Output parameter receiving the pixel X coordinate.</param>
        /// <param name="pixelY">Output parameter receiving the pixel Y coordinate.</param>
        public void TileXYToPixelXY(int tileX, int tileY, out int pixelX, out int pixelY)
        {
            pixelX = tileX * _tileSize;
            pixelY = tileY * _tileSize;
        }



        /// <summary>
        /// Converts tile XY coordinates into a QuadKey at a specified level of detail.
        /// </summary>
        /// <param name="tileX">Tile X coordinate.</param>
        /// <param name="tileY">Tile Y coordinate.</param>
        /// <param name="levelOfDetail">Level of detail, from 1 (lowest detail)
        /// to 23 (highest detail).</param>
        /// <returns>A string containing the QuadKey.</returns>
        public static string TileXYToQuadKey(int tileX, int tileY, int levelOfDetail)
        {
            StringBuilder quadKey = new StringBuilder();
            for (int i = levelOfDetail; i > 0; i--)
            {
                char digit = '0';
                int mask = 1 << (i - 1);
                if ((tileX & mask) != 0)
                {
                    digit++;
                }
                if ((tileY & mask) != 0)
                {
                    digit++;
                    digit++;
                }
                quadKey.Append(digit);
            }
            return quadKey.ToString();
        }



        /// <summary>
        /// Converts a QuadKey into tile XY coordinates.
        /// </summary>
        /// <param name="quadKey">QuadKey of the tile.</param>
        /// <param name="tileX">Output parameter receiving the tile X coordinate.</param>
        /// <param name="tileY">Output parameter receiving the tile Y coordinate.</param>
        /// <param name="levelOfDetail">Output parameter receiving the level of detail.</param>
        public static void QuadKeyToTileXY(string quadKey, out int tileX, out int tileY, out int levelOfDetail)
        {
            tileX = tileY = 0;
            levelOfDetail = quadKey.Length;
            for (int i = levelOfDetail; i > 0; i--)
            {
                int mask = 1 << (i - 1);
                switch (quadKey[levelOfDetail - i])
                {
                    case '0':
                        break;

                    case '1':
                        tileX |= mask;
                        break;

                    case '2':
                        tileY |= mask;
                        break;

                    case '3':
                        tileX |= mask;
                        tileY |= mask;
                        break;

                    default:
                        throw new ArgumentException("Invalid QuadKey digit sequence.");
                }
            }
        }
    }

    public class Index<TEntityType>
    {
        private ISpatialExtentProvider<TEntityType> extentProvider;
        public Index(ISpatialExtentProvider<TEntityType> extentProvider)
        {
            this.extentProvider = extentProvider;
        }

        public string GetKey(TEntityType entitty)
        {
            var extent = this.extentProvider.GetExtent(entitty);
            int minx, miny,maxx,maxy;
            int tilex=0, tiley=0, lvl=1;
            for (; lvl <= 23; lvl++)
            {
                TileSystem.LatLongToPixelXY(extent[1], extent[0], lvl, out minx, out miny);
                TileSystem.LatLongToPixelXY(extent[3], extent[2], lvl, out maxx, out maxy);
                TileSystem.PixelXYToTileXY(minx, miny, out minx, out miny);
                TileSystem.PixelXYToTileXY(maxx,maxy, out maxx, out maxy);

                if (minx != maxx || miny != maxy)
                    break;

                tilex = minx;
                tiley = miny;

            }


            return TileXYToQuadKey(tilex,tiley,Math.Max(1, lvl-1)).PadRight(30,TableStorageContext.KeySeparator.First());// "000000";// extentProvider.GetExtent(entitty).ToString();
        }

        /// <summary>
        /// Converts tile XY coordinates into a QuadKey at a specified level of detail.
        /// </summary>
        /// <param name="tileX">Tile X coordinate.</param>
        /// <param name="tileY">Tile Y coordinate.</param>
        /// <param name="levelOfDetail">Level of detail, from 1 (lowest detail)
        /// to 23 (highest detail).</param>
        /// <returns>A string containing the QuadKey.</returns>
        public static string TileXYToQuadKey(int tileX, int tileY, int levelOfDetail)
        {
            StringBuilder quadKey = new StringBuilder();
            for (int i = levelOfDetail; i > 0; i--)
            {
                char digit = '0';
                int mask = 1 << (i - 1);
                if ((tileX & mask) != 0)
                {
                    digit++;
                }
                if ((tileY & mask) != 0)
                {
                    digit++;
                    digit++;
                }
                quadKey.Append(digit);
            }
            return quadKey.ToString();
        }
    }
    public static class SpatialExtensions
    {
        public static Task<EntityProperty> GeometryEncode(JObject plainText)
        {
            return Task.FromResult(new EntityProperty(Encoding.UTF8.GetBytes(plainText.ToString())));
        }

        public static Task<JObject> GeometryDecode(EntityProperty prop)
        {
            return Task.FromResult(JObject.Parse(Encoding.UTF8.GetString(prop.BinaryValue)));
        }
        public static EntityTypeConfiguration<TEntityType> WithGeometry<TEntityType>(this EntityTypeConfiguration<TEntityType> config, Expression<Func<TEntityType, JObject>> expression)
        {
            return config.WithPropertyOf(expression, GeometryDecode, GeometryEncode);
        }

        public static string QuadKeyFromExtent(double[] extent)
        {
            int minx, miny, maxx, maxy;
            int tilex = 0, tiley = 0, lvl = 1;
            for (; lvl <= 23; lvl++)
            {
                TileSystem.LatLongToPixelXY(extent[1], extent[0], lvl, out minx, out miny);
                TileSystem.LatLongToPixelXY(extent[3], extent[2], lvl, out maxx, out maxy);
                TileSystem.PixelXYToTileXY(minx, miny, out minx, out miny);
                TileSystem.PixelXYToTileXY(maxx, maxy, out maxx, out maxy);

                if (minx != maxx || miny != maxy)
                    break;

                tilex = minx;
                tiley = miny;

            }


            return TileXYToQuadKey(tilex, tiley, Math.Max(1, lvl - 1));


        }
        /// <summary>
        /// Converts tile XY coordinates into a QuadKey at a specified level of detail.
        /// </summary>
        /// <param name="tileX">Tile X coordinate.</param>
        /// <param name="tileY">Tile Y coordinate.</param>
        /// <param name="levelOfDetail">Level of detail, from 1 (lowest detail)
        /// to 23 (highest detail).</param>
        /// <returns>A string containing the QuadKey.</returns>
        public static string TileXYToQuadKey(int tileX, int tileY, int levelOfDetail)
        {
            StringBuilder quadKey = new StringBuilder();
            for (int i = levelOfDetail; i > 0; i--)
            {
                char digit = '0';
                int mask = 1 << (i - 1);
                if ((tileX & mask) != 0)
                {
                    digit++;
                }
                if ((tileY & mask) != 0)
                {
                    digit++;
                    digit++;
                }
                quadKey.Append(digit);
            }
            return quadKey.ToString();
        }
        public static async Task<IEnumerable<TEntity>> SpatialIntersectAsync<TEntity,TIntersectEntity>(this ITableRepository<TEntity> table, TIntersectEntity test, ISpatialExtentProvider<TIntersectEntity> extentProvider, ISpatialIntersectService<TEntity, TIntersectEntity> intersectingService)
        {
            var spatial = table.Configuration.Indexes["spatialindex"];
            if (spatial == null)
                throw new NotSupportedException("The table is not configured for spatialindex");

            var extent = extentProvider.GetExtent(test);
            var quadKey = QuadKeyFromExtent(extent);

           var tableClient =  table.Context.GetTable(spatial.TableName);

            var tableQuery = new TableQuery<EntityAdapter<TEntity>>();
            var lastChar = quadKey.Last();
            var lessThan = quadKey.Substring(0, quadKey.Length - 1) + ++lastChar;
            tableQuery.FilterString = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual, quadKey);
            tableQuery.FilterString = TableQuery.CombineFilters(tableQuery.FilterString,TableOperators.And, TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, lessThan));

            var entities = await tableClient.ExecuteQueryAsync(tableQuery);         
          

            return entities.Select(k=>k.InnerObject).Where(o=>intersectingService.IsIntersectingAsync(o,test).Result).ToArray();
        }

        public static EntityTypeConfiguration<TEntityType> WithSpatialIndex<TEntityType,TEntityGeometry, IndexKeyType>(
            this EntityTypeConfiguration<TEntityType> config, 
            Expression<Func<TEntityType, TEntityGeometry>> expression,
            Expression<Func<TEntityType, IndexKeyType>> IndexKeyExpression,
            ISpatialExtentProvider<TEntityType> extentProvider,
            string TableName = null)
        {



            //string key = "";

            var indx = new Index<TEntityType>(extentProvider);
            string key = "";


            var entityToKeyProperty = config.ConvertToStringKey(IndexKeyExpression, out key);
            

            //var entityToKeyProperty = ConvertToStringKey(IndexKeyExpression, out key);
            config.Indexes.Add("spatialindex", new IndexConfiguration<TEntityType>
            {
                PartitionKeyProvider = indx.GetKey,
                RowKeyProvider = entityToKeyProperty,
                TableName = TableName ?? (string.IsNullOrWhiteSpace(config.TableName) ? null : config.TableName + "SpatialIndex"),
                TableNamePostFix = "SpatialIndex",
                CopyAllProperties = true,
            });

            return config;
        }
    }
}
