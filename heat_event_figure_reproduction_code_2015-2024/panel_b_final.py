import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm, BoundaryNorm, ListedColormap
import matplotlib.ticker as mticker
import numpy as np
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from scipy.ndimage import gaussian_filter 
import seaborn as sns
from shapely.geometry import MultiPolygon
import shapely.vectorized
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter

# --- 0. Global style settings ---
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = 'Arial'
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['savefig.dpi'] = 600
plt.rcParams['figure.dpi'] = 100

# --- 1. Data processing function ---
def load_and_grid_data(filepath, n_lat, n_lon):
    """Load data from a CSV file and aggregate it onto a grid."""
    print(f"Loading and processing data from file: {filepath}")
    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {filepath}. Make sure the file exists and the path is correct.")
    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        raise ValueError(f"Data file {filepath} is missing 'latitude' or 'longitude' columns.")
    df.dropna(subset=['latitude', 'longitude'], inplace=True)
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df.dropna(subset=['latitude', 'longitude'], inplace=True)
    df = df[(df['latitude'] >= -90) & (df['latitude'] <= 90) & (df['longitude'] >= -180) & (df['longitude'] <= 180)]
    if df.empty: return np.zeros((n_lat, n_lon))
    grid_frequencies = np.zeros((n_lat, n_lon))
    grid_lat_edges = np.linspace(-90, 90, n_lat + 1)
    grid_lon_edges = np.linspace(-180, 180, n_lon + 1)
    lat_indices = np.clip(np.digitize(df['latitude'], grid_lat_edges) - 1, 0, n_lat - 1)
    lon_indices = np.clip(np.digitize(df['longitude'], grid_lon_edges) - 1, 0, n_lon - 1)
    np.add.at(grid_frequencies, (lat_indices, lon_indices), 1)
    return grid_frequencies

# --- 2. Plotting function modified for no text, long ticks, and JPG output ---
def plot_smoothed_map(data_grid, lons, lats, cmap, sigma, filename, land_mask):
    """
    Color land areas only and keep oceans white; no text labels.
    """
    print(f"Generating land-only colored version (sigma={sigma}): {filename}")
    
    smoothed_data = gaussian_filter(data_grid, sigma=sigma)
    proj = ccrs.Robinson()
    fig, ax = plt.subplots(figsize=(6.18, 3.09), subplot_kw={'projection': proj})
    ax.set_global()
    
    # Use land_mask to mask all ocean areas
    plot_data = np.ma.masked_where(~land_mask, smoothed_data)

    # Keep the color segmentation logic unchanged.
    b1 = np.logspace(np.log10(1), np.log10(50), 10)
    b2 = np.logspace(np.log10(50), np.log10(600), 8)
    cap_value = 2000
    b3 = np.logspace(np.log10(600), np.log10(cap_value), 5)
    boundaries = np.concatenate([b1[:-1], b2[:-1], b3])
    num_colors = len(boundaries) - 1
    
    original_cmap = plt.get_cmap(cmap) 
    truncated_colors = original_cmap(np.linspace(0.2, 1.0, num_colors))
    discrete_cmap = ListedColormap(truncated_colors)
    discrete_norm = BoundaryNorm(boundaries, ncolors=num_colors, clip=True)

    mesh = ax.contourf(lons, lats, plot_data, levels=boundaries,
                         transform=ccrs.PlateCarree(),
                         cmap=discrete_cmap, 
                         norm=discrete_norm,
                         extend='max')
    
    # Map features
    ax.add_feature(cfeature.OCEAN, facecolor='white', zorder=-2)
    ax.add_feature(cfeature.LAND, facecolor="#dddddd", zorder=-1)
    border_color = '#dddddd'
    ax.coastlines(linewidth=0.5, color=border_color)
    ax.spines['geo'].set_edgecolor(border_color)
    ax.spines['geo'].set_linewidth(1.5)
    
    # Gridlines: draw_labels=False hides labels.
    # gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=False,
    #                   linewidth=0.5, color='#dddddd', alpha=0.5, linestyle='solid')

    # Colorbar settings
    # Keep the original tick-position logic
    ticks_to_show = [0, 10, 50, 200, 600, 1500]
    
    cbar = fig.colorbar(mesh, ax=ax, orientation='horizontal', shrink=0.5, pad=0.09, aspect=30,
                        ticks=ticks_to_show)
    
    # --- Change: hide text and keep long ticks ---
    cbar.ax.set_xticklabels([])    # Remove tick numbers
    cbar.set_label("")             # Remove title
    cbar.ax.tick_params(length=3, width=0.8) # Set tick length and width

    # Save files
    plt.savefig(f"{filename}.pdf", bbox_inches='tight')
    # Add JPG output (600 DPI)
    plt.savefig(f"{filename}.jpg", dpi=600, bbox_inches='tight', format='jpg', pil_kwargs={'quality': 95})
    
    plt.close(fig)
    print(f"Saved figures: {filename}.pdf / .jpg")


# --- 3. Main workflow ---
if __name__ == "__main__":
    n_lat, n_lon = 180, 360 # 1x1 degree resolution
    
    # --- Load and grid data ---
    news_csv_path = 'combined_news_events_locations.csv'
    news_grid_frequencies = load_and_grid_data(news_csv_path, n_lat, n_lon)
    
    # --- Creating land mask ---
    print("\nCreating the real land mask...")
    lats_centers_grid = np.linspace(-90 + 180.0/n_lat/2.0, 90 - 180.0/n_lat/2.0, n_lat)
    lons_centers_grid = np.linspace(-180 + 360.0/n_lon/2.0, 180 - 360.0/n_lon/2.0, n_lon)
    lon_mesh_centers, lat_mesh_centers = np.meshgrid(lons_centers_grid, lats_centers_grid)

    land_polygons_feature = cfeature.NaturalEarthFeature('physical', 'land', '110m')
    land_geoms = list(land_polygons_feature.geometries())

    flat_land_polygons = []
    for geom in land_geoms:
        if geom.geom_type == 'MultiPolygon':
            flat_land_polygons.extend(list(geom.geoms))
        else:
            flat_land_polygons.append(geom)
            
    land_mask = np.zeros_like(lon_mesh_centers, dtype=bool)
    points_x, points_y = lon_mesh_centers.ravel(), lat_mesh_centers.ravel()
    land_mask_flat = np.zeros(points_x.shape, dtype=bool)

    for poly in flat_land_polygons:
        if poly.is_valid:
            land_mask_flat = np.logical_or(land_mask_flat, shapely.vectorized.contains(poly, points_x, points_y))
            
    land_mask = land_mask_flat.reshape(lon_mesh_centers.shape)
    print("Real land mask created.")
    
    # Apply mask
    news_grid_frequencies[~land_mask] = 0
    print("Ocean frequencies set to zero.")

    # --- Create output directory ---
    output_dir = "final_output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Define longitude/latitude grids for plotting.
    lats_grid = np.linspace(-90 + 180.0/n_lat/2.0, 90 - 180.0/n_lat/2.0, n_lat)
    lons_grid = np.linspace(-180 + 360.0/n_lon/2.0, 180 - 360.0/n_lon/2.0, n_lon)
    
    # --- Call the new plotting function ---
    # Removed label and panel_letter arguments
    plot_smoothed_map(
        data_grid=news_grid_frequencies,
        lons=lons_grid,
        lats=lats_grid,
        cmap='Blues', 
        sigma=0.4,   
        filename=f"{output_dir}/panel_b",
        land_mask=land_mask
    )

    print("\nSmoothed news-data distribution map generated.")