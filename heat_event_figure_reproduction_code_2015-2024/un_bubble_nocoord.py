import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import BoundaryNorm
import numpy as np
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.ticker as mticker
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter

# --- 0. Global style settings ---
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = 'Arial'
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['savefig.dpi'] = 600
plt.rcParams['figure.dpi'] = 100
TITLE_FONTSIZE = 8
LABEL_FONTSIZE = 8


# --- 1. New data processing function for rectangle centroids ---
def load_and_calculate_centroids(filepath):
    """
    Load rectangular bounding-box data, calculate centroids, and prepare a DataFrame for plotting.
    """
    print(f"Loading bounding-box data and calculating centroids: {filepath}")
    df = pd.read_csv(filepath)
    
    # Assume columns are 'lon_min', 'lat_min', 'lon_max', 'lat_max', and 'Count'.
    required_cols = ['lon_min', 'lat_min', 'lon_max', 'lat_max', 'Count']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Data file is missing required bounding-box columns: {required_cols}.")

    # Data cleaning
    df.dropna(subset=required_cols, inplace=True)
    for col in required_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(subset=required_cols, inplace=True)

    # Calculate centroid longitude/latitude
    df['lon_center'] = (df['lon_min'] + df['lon_max']) / 2
    df['lat_center'] = (df['lat_min'] + df['lat_max']) / 2

    # Convert calculated centroid longitude from 0-360 to -180-180.
    # Rename final columns for compatibility with the plotting function.
    df['lon_start__180'] = df['lon_center'].apply(lambda x: x - 360 if x > 180 else x)
    df['lat_start'] = df['lat_center']
    
    return df

# --- 2. Plotting function (bubble-map version, no changes needed) ---
# The plot_bubble_map function created earlier only needs a DataFrame containing
# 'lon_start__180', 'lat_start', and 'Count' columns.
def plot_bubble_map(df, cmap, norm, ticks_to_show, cbar_label, filename, panel_letter=""):
    """
    Plot data with a scatter plot (bubble map) while preserving the rest of the map styling.
    """
    print(f"Generating bubble map: {filename}")
    
    proj = ccrs.Robinson()
    fig, ax = plt.subplots(figsize=(6.18, 3.09), subplot_kw={'projection': proj})
    ax.set_global()
    
    ax.add_feature(cfeature.OCEAN, facecolor='white', zorder=-2)
    ax.add_feature(cfeature.LAND, facecolor="#dddddd", zorder=-1)
    border_color = '#dddddd'
    ax.coastlines(linewidth=0.5, color=border_color)
    ax.spines['geo'].set_edgecolor(border_color)
    ax.spines['geo'].set_linewidth(1.5)
    
    size_factor = 15
    
    scatter_plot = ax.scatter(df['lon_start__180'], df['lat_start'], # use prepared centroid coordinates
                              s=df['Count'] * size_factor,
                              c=df['Count'],
                              cmap=cmap,
                              norm=norm,
                              alpha=0.8,
                              edgecolors='white',
                              linewidth=0.5,
                              transform=ccrs.PlateCarree(),
                              zorder=10)
    
    # --- ChangeApply the gridline settings and hide coordinate labels ---
    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=False,
                      linewidth=0.5, color='#dddddd', alpha=0.5, linestyle='solid')
    # --- End change ---
    
    cbar = fig.colorbar(scatter_plot, ax=ax, orientation='horizontal', shrink=0.5, pad=0.09, aspect=30,
                        ticks=ticks_to_show)
    cbar.ax.xaxis.set_major_formatter(mticker.FormatStrFormatter('%g'))
    cbar.set_label(cbar_label, fontsize=LABEL_FONTSIZE)
    cbar.ax.tick_params(labelsize=LABEL_FONTSIZE)
    
    plt.savefig(f"{filename}.pdf", bbox_inches='tight')
    plt.savefig(f"{filename}.png", bbox_inches='tight')
    plt.close(fig)
    print(f"Saved figures: {filename}.pdf / .png")

# --- 3. Main workflow (updated) ---
if __name__ == "__main__":
    # 1. Use the new loader and provide the file containing bounding-box data
    events_csv_path = 'un_events_bounding_box.csv'  # File containing bounding-box data
    events_df = load_and_calculate_centroids(events_csv_path)
    
    # 2. Define colors and ticks (unchanged)
    cmap = 'autumn_r'
    boundaries = [0,3,6,9, 12,15]
    ticks_to_show = [0, 3, 6, 9, 12, 15]
    
    norm = BoundaryNorm(boundaries, ncolors=plt.get_cmap(cmap).N, clip=True)
    
    # 3. Call the plotting function.
    output_dir = "output_un_heat_events_map"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    plot_bubble_map(
        df=events_df,
        cmap=cmap,
        norm=norm,
        ticks_to_show=ticks_to_show,
        cbar_label='UN Heat Events Count (2015-2024)',
        filename=f"{output_dir}/un_heat_events_centroid_bubble_map_nocoord",
        panel_letter='c' # update panel letter
    )

    print("\nBubble map based on country rectangle centroids generated.")
