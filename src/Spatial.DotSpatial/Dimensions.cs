using System;

namespace SInnovations.Azure.TableStorageRepository.Spatial.DotSpatial
{
    [Flags]
    internal enum Dimensions
    {
        XY = 0,
        Z = 1,
        M = 2,
    }
}