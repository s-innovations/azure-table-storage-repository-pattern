using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DotSpatial.Topology;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace SInnovations.Azure.TableStorageRepository.Spatial.DotSpatial
{
    public class SpatialIntersectService<TEntityType, TTargetEntityType> : ISpatialIntersectService<TEntityType, TTargetEntityType>
    {
        JsonSerializer serialier = JsonSerializer.Create(
          new JsonSerializerSettings
          {
              Converters = new List<JsonConverter> { new GeoJsonConverter() }
          });

        protected Func<TEntityType, Task<JObject>> EntityReader { get; set; }

        protected Func<TTargetEntityType, Task<JObject>> TestReader { get; set; }

        public SpatialIntersectService(Func<TEntityType,Task<JObject>> EntityReader, Func<TTargetEntityType, Task<JObject>> TestReader)
        {
            this.EntityReader = EntityReader;
            this.TestReader = TestReader;
        }
        public async Task<bool> IsIntersectingAsync(TEntityType entity, TTargetEntityType testEntity)
        {
            var a = await EntityReader(entity);
            var b = await TestReader(testEntity);
            var geomA = a.ToObject<IGeometry>(serialier);
            var geomB = b.ToObject<IGeometry>(serialier);
    
            return Intersects(geomA, geomB);
            //     return a.ToObject<Polygon>(serialier).Intersects(b.ToObject<Polygon>(serialier));

        }

        private static bool Intersects(IGeometry A, IGeometry B)
        {
            var AGeoms = A as GeometryCollection;
            if (AGeoms != null)
            {
                foreach (var AGeom in AGeoms.Geometries)
                {
                    if (Intersects(AGeom, B))
                        return true;
                }

            }

            if (B is GeometryCollection)
            {
                return Intersects(B, A);
            }



            return (B.Intersects(A));

        }
    }
}
