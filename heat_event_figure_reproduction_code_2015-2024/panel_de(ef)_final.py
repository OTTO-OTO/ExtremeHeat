import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from shapely.geometry import Point, MultiPolygon
from shapely.prepared import prep
import shapely.vectorized
import seaborn as sns
from matplotlib.lines import Line2D

# --- 0. Global style settings ---
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = 'Arial'
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['savefig.dpi'] = 600
plt.rcParams['figure.dpi'] = 100

# --- 1. Data processing function ---
def load_and_grid_data(filepath, n_lat, n_lon, convert_lon=False):
    print(f"Loading and processing data from file: {filepath}")
    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {filepath}. Make sure the file exists and the path is correct.")

    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        raise ValueError(f"Data file {filepath} is missing 'latitude' or 'longitude' columns.")
    
    df = df.dropna(subset=['latitude', 'longitude'])
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df = df.dropna(subset=['latitude', 'longitude'])
    
    if convert_lon:
        df['longitude'] = np.where(df['longitude'] > 180, df['longitude'] - 360, df['longitude'])

    df = df[(df['latitude'] >= -90) & (df['latitude'] <= 90) & (df['longitude'] >= -180) & (df['longitude'] <= 180)]
    
    if df.empty:
        print(f"Warning: file {filepath} has no valid data points after loading.")
        return np.zeros((n_lat, n_lon))

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

    print(f"Data successfully aggregated to {n_lat}x{n_lon} grid.")
    return grid_frequencies

# --- 2. Plotting function modified for no text, long ticks, and JPG output ---

def plot_panel_e(calc_freq, news_freq, land_mask_grid, filename_base="panel_e"):
    """
    Plot Panel e: frequency distributions of calculated and news heat events (KDE plot).
    No text, no legend, keep long ticks.
    """
    print("Generating Panel e...")
    fig, ax = plt.subplots(figsize=(5, 5))

    calc_data_on_land = calc_freq[land_mask_grid & (calc_freq > 0)]
    news_data_on_land = news_freq[land_mask_grid & (news_freq > 0)]

    calc_color = sns.color_palette("Paired")[7]
    news_color = sns.color_palette("Paired")[1]
    
    sns.kdeplot(data=calc_data_on_land, ax=ax, color=calc_color,
                fill=True, alpha=0.5, log_scale=True, legend=False)
    sns.kdeplot(data=news_data_on_land, ax=ax, color=news_color,
                fill=True, alpha=0.5, log_scale=True, legend=False)

    # Remove labels
    ax.set_xlabel('')
    ax.set_ylabel('')
    
    # Remove tick labels and keep long ticks.
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    ax.tick_params(axis='both', which='both', length=3, width=0.8)
    
    sns.despine(ax=ax)

    plt.savefig(f"{filename_base}.pdf", bbox_inches='tight')
    plt.savefig(f"{filename_base}.jpg", dpi=600, bbox_inches='tight', format='jpg', pil_kwargs={'quality': 95})
    plt.close(fig)
    print(f"Saved Panel e figure: {filename_base}.pdf / .jpg")


def plot_panel_f(calc_freq, news_freq, land_mask_grid, filename_base="panel_f"):
    """
    Plot Panel f: reporting-bias distribution.
    No text, no legend, keep long ticks.
    """
    print("Generating Panel f...")
    fig, ax = plt.subplots(figsize=(3.185, 2.25))
    axis_line_color = "#cccccc"

    valid_mask = land_mask_grid 
    discrepancy_scores = calc_freq[valid_mask] - news_freq[valid_mask]
    kde_color = sns.color_palette("Paired")[7]
    
    sns.kdeplot(data=discrepancy_scores, ax=ax, fill=True, color=kde_color, linewidth=1.5, bw_adjust=0.5, legend=False)
    sns.rugplot(data=discrepancy_scores, ax=ax, height=0.05, color=sns.desaturate(kde_color, 0.3), legend=False)
    
    mean_val = np.mean(discrepancy_scores)
    median_val = np.median(discrepancy_scores)
    
    # Keep vertical lines but remove labels
    ax.axvline(mean_val, color='k', linestyle='--', linewidth=1)
    ax.axvline(median_val, color='k', linestyle=':', linewidth=1)
    ax.axvline(0, color='r', linestyle='-', linewidth=1, alpha=0.6)

    # Remove labels
    ax.set_xlabel('')
    ax.set_ylabel('')
    
    # Styling
    sns.despine(ax=ax)
    
    # Remove tick labels; keep long ticks and set color.
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    ax.tick_params(axis='both', color=axis_line_color, length=3, width=0.8)
    
    ax.spines['bottom'].set_color(axis_line_color)
    ax.spines['left'].set_color(axis_line_color)

    # Keep the original proportions; whitespace could be smaller without text, but these settings are safer.
    plt.subplots_adjust(left=0.20, right=0.95, bottom=0.21, top=0.90)

    plt.savefig(f"{filename_base}.pdf")
    plt.savefig(f"{filename_base}.jpg", dpi=600, format='jpg', pil_kwargs={'quality': 95})
    plt.close(fig)
    print(f"Saved Panel f figure: {filename_base}.pdf / .jpg")


def plot_panel_e_ecdf(calc_freq, news_freq, land_mask_grid, filename_base="panel_e_ecdf"):
    """
    Plot Panel e (ECDF version).
    No text, no legend, keep long ticks.
    """
    print("Generating Panel e (ECDF version)...")
    fig, ax = plt.subplots(figsize=(5, 5))

    calc_data_on_land = calc_freq[land_mask_grid]
    news_data_on_land = news_freq[land_mask_grid]

    paired_palette = sns.color_palette("Paired", 12)
    calc_color = paired_palette[4] 
    news_color = paired_palette[0] 

    sns.ecdfplot(data=calc_data_on_land, ax=ax, color=calc_color, linewidth=2, legend=False)
    sns.ecdfplot(data=news_data_on_land, ax=ax, color=news_color, linewidth=2, legend=False)

    ax.set_xscale('log') 
    
    # Remove labels
    ax.set_xlabel('')
    ax.set_ylabel('')
    
    # Median line
    calc_median = np.median(calc_data_on_land)
    news_median = np.median(news_data_on_land)
    ax.axhline(0.5, color='gray', linestyle=':', linewidth=1, zorder=0)
    ax.plot([news_median], [0.5], 'o', color=news_color, markersize=5)
    ax.plot([calc_median], [0.5], 'o', color=calc_color, markersize=5)
    
    # Remove tick labels and keep long ticks.
    ax.set_xticklabels([])
    ax.set_yticklabels([])
    ax.tick_params(axis='both', which='both', length=3, width=0.8)
    
    sns.despine(ax=ax)

    plt.savefig(f"{filename_base}.pdf", bbox_inches='tight')
    plt.savefig(f"{filename_base}.jpg", dpi=600, bbox_inches='tight', format='jpg', pil_kwargs={'quality': 95})
    plt.close(fig)
    print(f"Saved Panel e (ECDF version) figure: {filename_base}.pdf / .jpg")

def plot_panel_e_combined(calc_freq, news_freq, land_mask_grid, filename_base="panel_e_combined"):
    """
    Plot Panel e (combined KDE+ECDF).
    No text, no legend, keep long ticks.
    """
    print("Generating Panel e (combined KDE+ECDF)...")
    fig, ax1 = plt.subplots(figsize=(3.185, 2.5))

    deep_palette = sns.color_palette("deep")
    calc_color = deep_palette[3]
    news_color = deep_palette[0]
    axis_line_color = "#cccccc" 
    
    # Plotting
    calc_data_on_land = calc_freq[land_mask_grid & (calc_freq > 0)]
    news_data_on_land = news_freq[land_mask_grid & (news_freq > 0)]
    sns.kdeplot(data=calc_data_on_land, ax=ax1, color=calc_color, fill=True, alpha=0.3, log_scale=True, legend=False)
    sns.kdeplot(data=news_data_on_land, ax=ax1, color=news_color, fill=True, alpha=0.3, log_scale=True, legend=False)
    
    ax2 = ax1.twinx()
    sns.ecdfplot(data=calc_data_on_land, ax=ax2, color=calc_color, linestyle='--', linewidth=1, legend=False)
    sns.ecdfplot(data=news_data_on_land, ax=ax2, color=news_color, linestyle='--', linewidth=1, legend=False)
    
    # Remove labels
    ax1.set_xlabel('')
    ax1.set_ylabel('')
    ax2.set_ylabel('')
    ax2.grid(False)

    # Styling
    sns.despine(ax=ax1, top=True, right=True)
    sns.despine(ax=ax2, top=True, left=True)

    # Configure ax1 (left axis)
    ax1.set_xticklabels([])
    ax1.set_yticklabels([])
    ax1.tick_params(axis='both', color=axis_line_color, length=3, width=0.8) # long ticks
    ax1.spines['left'].set_color(axis_line_color)
    ax1.spines['bottom'].set_color(axis_line_color)
    
    # Configure ax2 (right axis)
    ax2.set_yticklabels([])
    ax2.spines['right'].set_visible(True)
    ax2.spines['right'].set_color(axis_line_color)
    ax2.spines['bottom'].set_color(axis_line_color) # ensure the bottom spine matches
    ax2.tick_params(axis='y', color=axis_line_color, length=3, width=0.8) # long ticks

    # Layout adjustment
    plt.subplots_adjust(left=0.15, right=0.84, bottom=0.23, top=0.82)
    
    plt.savefig(f"{filename_base}.pdf")
    plt.savefig(f"{filename_base}.jpg", dpi=600, format='jpg', pil_kwargs={'quality': 95})
    plt.close(fig)
    print(f"Saved Panel e (combined) figure: {filename_base}.pdf / .jpg")


# --- 3. Main workflow ---
if __name__ == "__main__":
    n_lat, n_lon = 180, 360
    
    # Specify input file paths
    news_csv_path = 'combined_news_events_locations.csv'
    calc_csv_path = 'calculated_heat_events_frequency.csv'

    # --- Load and grid data ---
    news_grid_frequencies = load_and_grid_data(news_csv_path, n_lat, n_lon, convert_lon=False)
    calc_grid_frequencies = load_and_grid_data(calc_csv_path, n_lat, n_lon, convert_lon=True)

    # --- Creating land mask ---
    lats_centers_grid = np.linspace(-90 + 180.0/n_lat/2.0, 90 - 180.0/n_lat/2.0, n_lat)
    lons_centers_grid = np.linspace(-180 + 360.0/n_lon/2.0, 180 - 360.0/n_lon/2.0, n_lon)
    lon_mesh_centers, lat_mesh_centers = np.meshgrid(lons_centers_grid, lats_centers_grid)
    land_polygons_feature = cfeature.NaturalEarthFeature('physical', 'land', '110m')
    land_geoms = list(land_polygons_feature.geometries())
    flat_land_polygons = []
    for geom in land_geoms:
        if geom.geom_type == 'MultiPolygon': flat_land_polygons.extend(list(geom.geoms))
        else: flat_land_polygons.append(geom)
    prepared_land_polygons = [prep(geom) for geom in flat_land_polygons]
    land_mask = np.zeros_like(lon_mesh_centers, dtype=bool)
    points_x, points_y = lon_mesh_centers.ravel(), lat_mesh_centers.ravel()
    land_mask_flat = np.zeros(points_x.shape, dtype=bool)
    for poly_idx, original_poly in enumerate(flat_land_polygons):
        if original_poly.is_valid:
            try:
                current_mask_for_poly = shapely.vectorized.contains(original_poly, points_x, points_y)
                land_mask_flat = np.logical_or(land_mask_flat, current_mask_for_poly)
            except Exception: pass
    land_mask = land_mask_flat.reshape(lon_mesh_centers.shape)
    
    # --- Create output directory ---
    output_dir = "final_output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # --- Call the plotting function ---
    # plot_panel_e(calc_grid_frequencies, news_grid_frequencies, land_mask,
    #              filename_base=f"{output_dir}/panel_e_distributions")

    plot_panel_f(calc_grid_frequencies, news_grid_frequencies, land_mask,
                 filename_base=f"{output_dir}/panel_e")
    
    # plot_panel_e_ecdf(calc_grid_frequencies, news_grid_frequencies, land_mask,
    #              filename_base=f"{output_dir}/panel_e_ecdf_discrepancy")
    
    plot_panel_e_combined(calc_grid_frequencies, news_grid_frequencies, land_mask,
                      filename_base=f"{output_dir}/panel_d")

    print("\nPanels e and f generated (no text, no legend, long ticks, JPG 600 DPI).")