import rasterio
from rasterio.features import shapes
from shapely.geometry import shape
import geopandas as gpd
from pyproj import Transformer
from shapely.ops import transform as shp_transform

# 1. Define pixel areas
radd_pixel_size = 10.0                 # metres (RAAD TIFF resolution)
radd_pixel_area = radd_pixel_size**2   # 100 m² per RAAD pixel

insar_looks = (5, 5)                   # multi-looks in (range, azimuth)
insar_pixel_area = (insar_looks[0]*5) * (insar_looks[1]*5)
# if each look is ~5 m, this yields 25 m × 25 m = 625 m²

min_insar_pixels = 3 * 3               # 3×3 cluster → 9 pixels
min_area_m2      = min_insar_pixels * insar_pixel_area  # 9×625 = 5625 m²
min_radd_pixels  = min_area_m2 / radd_pixel_area         # 5625/100 = 56.25

print(f"Filtering for patches ≥ {min_radd_pixels:.1f} RAAD pixels (~{min_area_m2:.0f} m²)")

# 2. Open RAAD alert raster
src = rasterio.open("/home/colm-the-conjurer/VSCode/workspace/Borneo_Forest_Disturbance_Dataset/data/radd_alerts/radd_alerts_borneo_v2025-03-30.tif")
arr = src.read(1)
mask = arr > 0

# 3. Prepare transformer (RAAD CRS → metric CRS)
transformer = Transformer.from_crs(src.crs, "EPSG:32650", always_xy=True)

# 4. Vectorize + filter
records = []
for geom_dict, val in shapes(arr, mask=mask, transform=src.transform):
    # build Shapely geometry
    geom = shape(geom_dict)
    # project to metres and compute pixel count
    geom_m = shp_transform(transformer.transform, geom)
    pixel_count = geom_m.area / radd_pixel_area
    if pixel_count >= min_radd_pixels:
        records.append({
            "alert": int(val),
            "geometry": geom
        })

print(f"Kept {len(records)} disturbance patches ≥ 3×3 InSAR pixels")

# 5. Build GeoDataFrame and export
gdf = gpd.GeoDataFrame(records, crs=src.crs)
# decode alert code into year and day-of-year
gdf["year"] = gdf["alert"] // 1000 + 2000
gdf["doy"]  = gdf["alert"] % 1000

gdf.to_parquet(
    "/home/colm-the-conjurer/VSCode/workspace/InSAR_Forest_Disturbance_Dataset/data/filtered_radd_alerts.parquet",
    engine="pyarrow",
    index=False
)


