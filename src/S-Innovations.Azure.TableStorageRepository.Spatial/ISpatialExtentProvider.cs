using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.Spatial
{
    public interface ISpatialExtentProvider<TEntityType>
    {
        /// <summary>
        /// minX, minY, maxX, maxY
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        double[] GetExtent(TEntityType entity);
        bool SplitByExtent(TEntityType entity, double[] extentTest);
    }

    public interface ISpatialIntersectService<TEntityType,TTargetEntityType>
    {
      
        Task<bool> IsIntersectingAsync(TEntityType entity, TTargetEntityType testEntity);
    }
}
