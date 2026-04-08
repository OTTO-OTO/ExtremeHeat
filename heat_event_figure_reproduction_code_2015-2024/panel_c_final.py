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

# --- 1. Data processing function ---
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

    # Longitude conversion (0-360 -> -180-180)
    df['lon_start__180'] = df['lon_center'].apply(lambda x: x - 360 if x > 180 else x)
    df['lat_start'] = df['lat_center']
    
    return df

# --- 2. Plotting function modified for no text, long ticks, and JPG output ---
def plot_bubble_map(df, cmap, norm, ticks_to_show, filename):
    """
    Plot data with a scatter plot (bubble map), no text labels, and long ticks.
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
    
    scatter_plot = ax.scatter(df['lon_start__180'], df['lat_start'],
                              s=df['Count'] * size_factor,
                              c=df['Count'],
                              cmap=cmap,
                              norm=norm,
                              alpha=0.8,
                              edgecolors='white',
                              linewidth=0.5,
                              transform=ccrs.PlateCarree(),
                              zorder=10)
    
    # Change 1: draw_labels=False hides longitude/latitude labels.
    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=False,
                      linewidth=0, linestyle='solid')
    
    # Change 2: colorbar settings.
    cbar = fig.colorbar(scatter_plot, ax=ax, orientation='horizontal', shrink=0.5, pad=0.09, aspect=30,
                        ticks=ticks_to_show)
    
    # Change 3: remove text and lengthen ticks.
    cbar.ax.set_xticklabels([])  # Remove numbers
    cbar.set_label("")           # Remove title
    cbar.ax.tick_params(length=3, width=0.8)  # Set long ticks.

    # Change 4: save PDF and JPG at 600 DPI.
    plt.savefig(f"{filename}.pdf", bbox_inches='tight')
    plt.savefig(f"{filename}.jpg", dpi=600, bbox_inches='tight', format='jpg', pil_kwargs={'quality': 95})
    
    plt.close(fig)
    print(f"Saved figures: {filename}.pdf / .jpg")


# --- 3. Main workflow ---
if __name__ == "__main__":
    # 1. Load data
    events_csv_path = 'un_events_bounding_box.csv' 
    events_df = load_and_calculate_centroids(events_csv_path)
    
    # 2. Define colors and ticks
    cmap = 'autumn_r'
    boundaries = [0,3,6,9, 12,15]
    ticks_to_show = [0, 3, 6, 9, 12, 15]
    
    norm = BoundaryNorm(boundaries, ncolors=plt.get_cmap(cmap).N, clip=True)
    
    # 3. Create output directory
    output_dir = "final_output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 4. Call the plotting function without label or panel_letter.
    plot_bubble_map(
        df=events_df,
        cmap=cmap,
        norm=norm,
        ticks_to_show=ticks_to_show,
        filename=f"{output_dir}/panel_c"
    )

    print("\nBubble map based on country rectangle centroids generated.")