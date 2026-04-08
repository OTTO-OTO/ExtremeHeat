import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import BoundaryNorm
import numpy as np
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import shapely.vectorized
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
import seaborn as sns

# --- 0. Global style settings ---
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = 'Arial'
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['savefig.dpi'] = 600
plt.rcParams['figure.dpi'] = 100

# --- 1. Data processing function ---
def load_and_grid_data(filepath, n_lat, n_lon, convert_lon=False):
    """Load CSV data and convert it to a 2D grid at the requested resolution."""
    print(f"Loading and processing data from file: {filepath}")
    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {filepath}. Make sure the file exists and the path is correct.")
    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        raise ValueError(f"Data file {filepath} is missing 'latitude' or 'longitude' columns.")
    
    # Data cleaning
    df.dropna(subset=['latitude', 'longitude'], inplace=True)
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df.dropna(subset=['latitude', 'longitude'], inplace=True)

    # Convert longitude from 0-360 to -180-180.
    if convert_lon:
        df['longitude'] = np.where(df['longitude'] > 180, df['longitude'] - 360, df['longitude'])

    # Filter invalid latitude/longitude ranges.
    df = df[(df['latitude'] >= -90) & (df['latitude'] <= 90) & (df['longitude'] >= -180) & (df['longitude'] <= 180)]
    
    if df.empty:
        return np.zeros((n_lat, n_lon))

    # Create and populate the grid.
    grid_frequencies = np.zeros((n_lat, n_lon))
    grid_lat_edges = np.linspace(-90, 90, n_lat + 1)
    grid_lon_edges = np.linspace(-180, 180, n_lon + 1)
    lat_indices = np.clip(np.digitize(df['latitude'], grid_lat_edges) - 1, 0, n_lat - 1)
    lon_indices = np.clip(np.digitize(df['longitude'], grid_lon_edges) - 1, 0, n_lon - 1)
    
    if 'frequency' in df.columns:
        frequencies = df['frequency'].to_numpy()
        np.add.at(grid_frequencies, (lat_indices, lon_indices), frequencies)
    else:
        np.add.at(grid_frequencies, (lat_indices, lon_indices), 1)
        
    return grid_frequencies

# --- 2. General world-map plotting function with no text and long ticks ---
def create_world_map(data_grid, lons, lats, cmap, norm, filename, cbar_ticks=None):
    """Create a world-map subplot with curved projection edges, no text labels, and long ticks."""
    print(f"Generating map: {filename}")
    proj = ccrs.Robinson()
    fig, ax = plt.subplots(figsize=(6.18, 3.09), subplot_kw={'projection': proj})
    ax.set_global()
    
    plot_data = np.ma.masked_where(data_grid == 0, data_grid)

    mesh = ax.pcolormesh(lons, lats, plot_data, transform=ccrs.PlateCarree(),
                         cmap=cmap, norm=norm, shading='auto')
    
    ax.add_feature(cfeature.OCEAN, facecolor='white', zorder=-2)
    ax.add_feature(cfeature.LAND, facecolor="#f3f3f3", zorder=-1)
    
    border_color = '#dddddd'
    ax.coastlines(linewidth=0.5, color=border_color)
    ax.spines['geo'].set_edgecolor(border_color)
    ax.spines['geo'].set_linewidth(1.5)

    # hide longitude/latitude labels
    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=False,
                  linewidth=0, linestyle='solid')
    
    # Colorbar settings
    cbar = fig.colorbar(mesh, ax=ax, orientation='horizontal', shrink=0.5, pad=0.09, aspect=30)
    
    # Key change: set tick positions
    if cbar_ticks is not None: 
        cbar.set_ticks(cbar_ticks)
    
    # Key change:
    # 1. set_xticklabels([]) hides numbers
    # 2. set_label("") hides the title
    # 3. tick_params(length=6) shows tick marks and makes them longer; the default is about 3-4.
    cbar.ax.set_xticklabels([])  
    cbar.set_label("")           
    cbar.ax.tick_params(length=3, width=0.8) # length controls length, width controls width

    # Save files
    plt.savefig(f"{filename}.pdf", bbox_inches='tight')
    plt.savefig(f"{filename}.jpg", dpi=600, bbox_inches='tight', format='jpg', pil_kwargs={'quality': 95})
    
    plt.close(fig)
    print(f"Saved figures: {filename}.pdf / .jpg")

# --- 3. Main workflow ---
if __name__ == "__main__":
    n_lat, n_lon = 180, 360  # 1-degree resolution
    
    # --- Load and grid data ---
    calc_csv_path = 'calculated_heat_events_frequency.csv'
    calc_grid_frequencies = load_and_grid_data(calc_csv_path, n_lat, n_lon, convert_lon=True)
    
    # --- Creating land mask ---
    print("\nCreating land mask...")
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
    land_mask_flat = np.zeros(lon_mesh_centers.ravel().shape, dtype=bool)
    for poly in flat_land_polygons:
        if poly.is_valid:
            land_mask_flat = np.logical_or(land_mask_flat, shapely.vectorized.contains(poly, lon_mesh_centers.ravel(), lat_mesh_centers.ravel()))
    land_mask = land_mask_flat.reshape(lon_mesh_centers.shape)
    print("Land mask created.")
    
    # Apply land mask
    calc_grid_frequencies[~land_mask] = 0
    print("Ocean frequencies set to zero.")
    
    # --- Prepare plotting ---
    output_dir = "final_output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    lats_grid = np.linspace(-90, 90, n_lat)
    lons_grid = np.linspace(-180, 180, n_lon)
    
    # Define parameters for the calc map
    N_BINS_CALC = 20
    VMIN_CALC = 1000
    VMAX_CALC = 9000
    calc_bounds = np.linspace(VMIN_CALC, VMAX_CALC, N_BINS_CALC + 1)
    calc_colors = sns.color_palette("Reds", n_colors=N_BINS_CALC + 1)
    calc_cmap = plt.cm.colors.ListedColormap(calc_colors)
    calc_norm = BoundaryNorm(calc_bounds, calc_cmap.N, extend='max')
    calc_ticks = np.linspace(VMIN_CALC, VMAX_CALC, 5)
    
    # Call the plotting function.
    create_world_map(
        data_grid=calc_grid_frequencies, 
        lons=lons_grid, 
        lats=lats_grid, 
        cmap=calc_cmap, 
        norm=calc_norm, 
        filename=f"{output_dir}/panel_a", 
        cbar_ticks=calc_ticks
    )
    
    print("\ncalculated data map (calc map) generated successfully.")