import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm, BoundaryNorm, ListedColormap
import matplotlib.ticker as mticker
import numpy as np
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from scipy.ndimage import gaussian_filter  # Gaussian filter for smoothing
import seaborn as sns
# Ensure libraries needed for geospatial data processing are imported
from shapely.geometry import MultiPolygon
import shapely.vectorized
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter

# --- 0. Global style settings (same as before) ---
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = 'Arial'
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['savefig.dpi'] = 600
plt.rcParams['figure.dpi'] = 100
TITLE_FONTSIZE = 8
LABEL_FONTSIZE = 8
TICK_FONTSIZE = 8

# --- 1. Data processing function (same as before) ---
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

import matplotlib.ticker as mticker
import numpy as np
# ... (other imports remain unchanged) ...

# --- Final V4: color land only, leave ocean blank ---
# Change 1add the land_mask parameter
def plot_smoothed_map(data_grid, lons, lats, cmap, sigma, cbar_label, filename, land_mask, panel_letter=""):
    """
    Color land areas only and keep oceans white.
    """
    print(f"Generating land-only colored version (sigma={sigma}): {filename}")
    
    smoothed_data = gaussian_filter(data_grid, sigma=sigma)
    proj = ccrs.Robinson()
    fig, ax = plt.subplots(figsize=(6.18, 3.09), subplot_kw={'projection': proj})
    ax.set_global()
    
    # --- Change 2: use land_mask to mask all ocean areas ---
    # `~land_mask` selects all non-land cells, i.e. ocean cells
    plot_data = np.ma.masked_where(~land_mask, smoothed_data)
    # --- End change ---

    # (Boundary definitions remain unchanged)
    b1 = np.logspace(np.log10(1), np.log10(50), 10)
    b2 = np.logspace(np.log10(50), np.log10(600), 8)
    cap_value = 2000
    b3 = np.logspace(np.log10(600), np.log10(cap_value), 5)
    boundaries = np.concatenate([b1[:-1], b2[:-1], b3])
    num_colors = len(boundaries) - 1
     # --- New core change ---
    # 1. Get the original colormap object from the input cmap string, such as 'Blues'.
    original_cmap = plt.get_cmap(cmap) 
    
    # 2. Sample num_colors evenly from 20% to 100% of the original colormap
    #    This ensures the color count matches BoundaryNorm exactly
    truncated_colors = original_cmap(np.linspace(0.2, 1.0, num_colors))
    
    # 3. Create a new discrete colormap from the sampled colors
    discrete_cmap = ListedColormap(truncated_colors)
    # --- End change ---
    
    discrete_norm = BoundaryNorm(boundaries, ncolors=num_colors, clip=True)

    mesh = ax.contourf(lons, lats, plot_data, levels=boundaries,
                         transform=ccrs.PlateCarree(),
                         cmap=discrete_cmap, 
                         norm=discrete_norm,
                         extend='max')
    
    # These two lines matter again because oceans will be visible.
    ax.add_feature(cfeature.OCEAN, facecolor='white', zorder=-2)
    ax.add_feature(cfeature.LAND, facecolor="#dddddd", zorder=-1) # Use a light gray fallback in case some land cells are not covered
    border_color = '#dddddd'
    ax.coastlines(linewidth=0.5, color=border_color)
    ax.spines['geo'].set_edgecolor(border_color)
    ax.spines['geo'].set_linewidth(1.5)
    
    # --- New blockAdd longitude/latitude ticks ---
    
    # 1. Create a gridliner with draw_labels=True.
    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=dict(bottom="x", left="y"),
                      linewidth=0, linestyle='solid')
    gl.xformatter = LongitudeFormatter()
    gl.yformatter = LatitudeFormatter()
    gl.xlabel_style = {'size': TICK_FONTSIZE}
    gl.ylabel_style = {'size': TICK_FONTSIZE}
    # --- End new block ---

    # (Colorbar and label sections remain unchanged)
    ticks_to_show = [0, 10, 50, 200, 600, 1500]
    cbar = fig.colorbar(mesh, ax=ax, orientation='horizontal', shrink=0.5, pad=0.09, aspect=30,
                        ticks=ticks_to_show)
    cbar.ax.xaxis.set_major_formatter(mticker.FormatStrFormatter('%d'))
    cbar.set_label(cbar_label, fontsize=LABEL_FONTSIZE)
    cbar.ax.tick_params(labelsize=LABEL_FONTSIZE)
    
    # if panel_letter:
    #     ax.text(-0.05, 1.02, panel_letter, transform=ax.transAxes,
    #             fontsize=TITLE_FONTSIZE, fontweight='bold', va='bottom', ha='right')

    plt.savefig(f"{filename}.pdf", bbox_inches='tight')
    plt.savefig(f"{filename}.png", bbox_inches='tight')
    plt.close(fig)
    print(f"Saved figures: {filename}.pdf / .png")




# --- 3. Main workflow ---
if __name__ == "__main__":
    n_lat, n_lon = 180, 360 # 1x1 degree resolution
    
    # --- Load and grid data ---
    news_csv_path = 'combined_news_events_locations.csv'
    news_grid_frequencies = load_and_grid_data(news_csv_path, n_lat, n_lon)
    
    
    print("\nCreating the real land mask...")
    lats_centers_grid = np.linspace(-90 + 180.0/n_lat/2.0, 90 - 180.0/n_lat/2.0, n_lat)
    lons_centers_grid = np.linspace(-180 + 360.0/n_lon/2.0, 180 - 360.0/n_lon/2.0, n_lon)
    lon_mesh_centers, lat_mesh_centers = np.meshgrid(lons_centers_grid, lats_centers_grid)

    land_polygons_feature = cfeature.NaturalEarthFeature('physical', 'land', '110m')
    land_geoms = list(land_polygons_feature.geometries())

    # Fix and flatten polygons
    flat_land_polygons = []
    for geom in land_geoms:
        if geom.geom_type == 'MultiPolygon':
            flat_land_polygons.extend(list(geom.geoms))
        else:
            flat_land_polygons.append(geom)
            
    land_mask = np.zeros_like(lon_mesh_centers, dtype=bool)
    points_x, points_y = lon_mesh_centers.ravel(), lat_mesh_centers.ravel()
    land_mask_flat = np.zeros(points_x.shape, dtype=bool)

    # Import shapely
    from shapely.geometry import Point, MultiPolygon
    import shapely.vectorized

    for poly in flat_land_polygons:
        if poly.is_valid:
            # Use vectorized operations for speed
            land_mask_flat = np.logical_or(land_mask_flat, shapely.vectorized.contains(poly, points_x, points_y))
            
    land_mask = land_mask_flat.reshape(lon_mesh_centers.shape)
    print("Real land mask created.")
    
    
    # Apply the mask and zero out ocean frequencies
    news_grid_frequencies[~land_mask] = 0
    print("Ocean frequencies set to zero.")

    # --- Create output directory ---
    output_dir = "output_refined_maps"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Define longitude/latitude grids for plotting (cell centers)
    lats_grid = np.linspace(-90 + 180.0/n_lat/2.0, 90 - 180.0/n_lat/2.0, n_lat)
    lons_grid = np.linspace(-180 + 360.0/n_lon/2.0, 180 - 360.0/n_lon/2.0, n_lon)
    
    # --- Call the new plotting function ---
    # cm = sns.color_palette("vlag", as_cmap=True)
    plot_smoothed_map(
        data_grid=news_grid_frequencies,
        lons=lons_grid,
        lats=lats_grid,
        cmap='Blues', 
        sigma=0.4,   
        cbar_label='News Reports Count (2015-2024)',
        filename=f"{output_dir}/panel_a_final_custom_ticks",
        land_mask=land_mask,  # pass the land mask
        panel_letter='b'
    )

    print("\nSmoothed news-data distribution map generated.")

    