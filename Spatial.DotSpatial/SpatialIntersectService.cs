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
            return geomA.Intersects(geomB);

       //     return a.ToObject<Polygon>(serialier).Intersects(b.ToObject<Polygon>(serialier));

        }
    }
}
