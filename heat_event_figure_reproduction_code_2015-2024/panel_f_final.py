# Script:plot_multi_panel_line_chart.py (modified: no text, no legend, long ticks, JPG output)
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import os
import seaborn as sns
from scipy import stats

def create_multi_panel_plot():
    """
    Load three annual datasets and create a figure with two vertically stacked subplots.
    No text labels, keep long ticks, and output JPG.
    """
    # --- 1. Load three datasets from CSV files ---
    calc_csv_filename = 'annual_mhw_counts_land_only.csv' 
    news_csv_filename = 'news_events_yearly_deduplicated.csv'
    un_csv_filename = 'un_events_yearly_counts.csv'           
    
    print(f"--- Starting multi-panel plotting script ---")
    try:
        df_calc = pd.read_csv(calc_csv_filename)
        df_news = pd.read_csv(news_csv_filename)
        df_un = pd.read_csv(un_csv_filename)
        df_un.rename(columns={'Year': 'year'}, inplace=True)
    except FileNotFoundError as e:
        print(f"Error: data file not found {e.filename}. Make sure all three CSV files exist.")
        return
        
    # Merge data
    df_merged = pd.merge(df_calc, df_news, on='year', how='outer')
    df = pd.merge(df_merged, df_un, on='year', how='outer').fillna(0)
    
    df.rename(columns={
        'event_count': 'calc_count', 
        'count': 'news_count',       
        'Count': 'un_count'          
    }, inplace=True)
        
    years = df['year'].values
    calc_counts = df['calc_count'].values
    news_counts = df['news_count'].values
    un_counts = df['un_count'].values
    print("Data loaded and merged successfully.")

    # --- 2. Calculate trend lines ---
    print("Calculating trend lines for the three datasets...")
    slope_calc, intercept_calc, _, _, _ = stats.linregress(years, calc_counts)
    trend_line_calc = slope_calc * years + intercept_calc
    
    slope_news, intercept_news, _, _, _ = stats.linregress(years, news_counts)
    trend_line_news = slope_news * years + intercept_news
    
    slope_un, intercept_un, _, _, _ = stats.linregress(years, un_counts)
    trend_line_un = slope_un * years + intercept_un

    # --- 3. Data visualization ---
    print("Starting plot...")
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = 'Arial'
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(3, 2.4), sharex=True, 
                                   gridspec_kw={'height_ratios': [2, 1]})

    deep_palette = sns.color_palette("muted")
    calc_color = deep_palette[3]
    news_color = deep_palette[0]
    un_color = deep_palette[1]

    # --- Plot the upper subplot (ax1) ---
    # Note: removed the label parameter because no legend is needed.
    ax1.plot(years, calc_counts, marker='o', linestyle='-', color=calc_color, markersize=4, linewidth=1.5)
    ax1.plot(years, trend_line_calc, linestyle='--', color=calc_color, linewidth=1.5, alpha=0.8)
    
    ax1.plot(years, news_counts, marker='s', linestyle='-', color=news_color, markersize=4, linewidth=1.5)
    ax1.plot(years, trend_line_news, linestyle='--', color=news_color, linewidth=1.5, alpha=0.8)

    ax1.set_ylim(0, 40000)

    # --- Plot the lower subplot (ax2) ---
    ax2.plot(years, un_counts, marker='^', linestyle='-', color=un_color, markersize=4, linewidth=1.5)
    ax2.plot(years, trend_line_un, linestyle='--', color=un_color, linewidth=1.5, alpha=0.8)
    
    ax2.set_ylim(0, max(un_counts) * 1.5 if max(un_counts) > 0 else 10) 

    # --- Apply shared styling to both subplots: no text and long ticks ---
    axis_line_color = "#cccccc"
    for ax in [ax1, ax2]:
        # Base styling
        ax.grid(True, linestyle='--', alpha=0.6, zorder=0)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color(axis_line_color)
        ax.spines['bottom'].set_color(axis_line_color)
        
        # Remove labels
        ax.set_xlabel('')
        ax.set_ylabel('')
        
        # Remove tick labels (Tick Labels)
        ax.set_xticklabels([])
        ax.set_yticklabels([])

        # Set tick style: long ticks with color
        ax.tick_params(axis='both', which='both', color=axis_line_color, length=3, width=0.8)
        
        # Set x-axis tick spacing using the existing logic.
        ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True, nbins=10))

    # --- Layout settings ---
    # Keep the original margins; even without a legend, consistent whitespace helps figure assembly
    plt.subplots_adjust(
        left=0.142,   
        right=0.98,  
        bottom=0.23, 
        top=0.75,    
        hspace=0.2   
    )

    # --- 4. Save figures ---
    output_dir = "final_output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_filename = "panel_f"
    
    # Save PDF
    plt.savefig(f"{output_dir}/{output_filename}.pdf", bbox_inches='tight')
    # Save JPG (600 DPI)
    plt.savefig(f"{output_dir}/{output_filename}.jpg", dpi=600, bbox_inches='tight', format='jpg', pil_kwargs={'quality': 95})
    
    print(f"Plot complete. Multi-panel comparison line chart saved as {output_filename}.pdf and .jpg")
    plt.close(fig)

if __name__ == "__main__":
    create_multi_panel_plot()
