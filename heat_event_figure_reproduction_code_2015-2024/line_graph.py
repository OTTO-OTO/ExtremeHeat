# Script:plot_multi_panel_line_chart.py (fixed KeyError)
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import os
import seaborn as sns
from scipy import stats

def create_multi_panel_plot():
    """
    Load three annual datasets and create a publication-ready figure with two vertically stacked subplots.
    The upper subplot contains two large-scale series; the lower subplot contains one small-scale series.
    """
    # --- Define font-size variables ---
    TITLE_FONTSIZE = 8
    LABEL_FONTSIZE = 8
    TICK_FONTSIZE = 8

    # --- 1. Load three datasets from CSV files ---
    calc_csv_filename = 'annual_mhw_counts_spatiotemporal.csv' # calculated heat events
    news_csv_filename = 'news_events_yearly_deduplicated.csv' # news events
    un_csv_filename = 'un_events_yearly_counts.csv'           # UN heat events
    
    print(f"--- Starting multi-panel plotting script ---")
    try:
        df_calc = pd.read_csv(calc_csv_filename)
        df_news = pd.read_csv(news_csv_filename)
        df_un = pd.read_csv(un_csv_filename)
        # Rename the UN 'Year' column to 'year' for merging.
        df_un.rename(columns={'Year': 'year'}, inplace=True)
    except FileNotFoundError as e:
        print(f"Error: data file not found {e.filename}. Make sure all three CSV files exist.")
        return
        
    # Use merge to combine the three data sources by 'year'
    df_merged = pd.merge(df_calc, df_news, on='year', how='outer')
    df = pd.merge(df_merged, df_un, on='year', how='outer').fillna(0)
    
    # --- Core fix---
    # Fix rename logic to use the correct original column name ('count')
    df.rename(columns={
        'event_count': 'calc_count', # from df_calc
        'count': 'news_count',       # from df_news (fixed here)
        'Count': 'un_count'          # from df_un
    }, inplace=True)
        
    years = df['year'].values
    calc_counts = df['calc_count'].values
    news_counts = df['news_count'].values
    un_counts = df['un_count'].values
    print("Data loaded and merged successfully.")

    # --- 2. Calculate statistics for each of the three datasets ---
    print("Calculating trend lines for the three datasets...")
    slope_calc, intercept_calc, r_calc, p_calc, _ = stats.linregress(years, calc_counts)
    trend_line_calc = slope_calc * years + intercept_calc
    
    slope_news, intercept_news, r_news, p_news, _ = stats.linregress(years, news_counts)
    trend_line_news = slope_news * years + intercept_news
    
    slope_un, intercept_un, r_un, p_un, _ = stats.linregress(years, un_counts)
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
    ax1.plot(years, calc_counts, marker='o', linestyle='-', color=calc_color, markersize=4, linewidth=1.5, 
             label=f'Heat Events')
    ax1.plot(years, trend_line_calc, linestyle='--', color=calc_color, linewidth=1.5, alpha=0.8)
    
    ax1.plot(years, news_counts, marker='s', linestyle='-', color=news_color, markersize=4, linewidth=1.5, 
             label=f'News Reports')
    ax1.plot(years, trend_line_news, linestyle='--', color=news_color, linewidth=1.5, alpha=0.8)

    ax1.set_ylabel('Annual Number of Events', fontsize=LABEL_FONTSIZE, labelpad=5,  y=0.2)
    ax1.set_ylim(0, 40000)
    ax1.ticklabel_format(style='plain', axis='y')
    # ax1.legend(loc='upper left', fontsize=TICK_FONTSIZE)
    # ax1.text(-0.16, 1.05, 'f', transform=ax1.transAxes, fontsize=TITLE_FONTSIZE, fontweight='bold')

    # --- Plot the lower subplot (ax2) ---
    ax2.plot(years, un_counts, marker='^', linestyle='-', color=un_color, markersize=4, linewidth=1.5, 
             label=f'UN Heat Events')
    ax2.plot(years, trend_line_un, linestyle='--', color=un_color, linewidth=1.5, alpha=0.8)
    
    ax2.set_xlabel('Year', fontsize=LABEL_FONTSIZE)
    # ax2.set_ylabel('Annual Count', fontsize=LABEL_FONTSIZE)
    ax2.set_ylim(0, max(un_counts) * 1.5 if max(un_counts) > 0 else 10)  # Avoid errors when the maximum value is 0.
    # ax2.legend(loc='upper left', fontsize=TICK_FONTSIZE)
    # ax2.text(-0.16, 1.05, 'e', transform=ax2.transAxes, fontsize=TITLE_FONTSIZE, fontweight='bold')

    # --- Apply shared styling to both subplots ---
    axis_line_color = "#cccccc"
    for ax in [ax1, ax2]:
        ax.grid(True, linestyle='--', alpha=0.6, zorder=0)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color(axis_line_color)
        ax.spines['bottom'].set_color(axis_line_color)
        ax.tick_params(axis='y', color=axis_line_color, labelcolor='black', labelsize=TICK_FONTSIZE)
        ax.tick_params(axis='x', color=axis_line_color, labelcolor='black', labelsize=TICK_FONTSIZE)
        ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True, nbins=10))

    # plt.tight_layout(pad=2.0)
    plt.subplots_adjust(
    left=0.142,   # left margin as a share of the canvas width
    right=0.98,  # Leave margin on the right side.
    bottom=0.23, # bottom margin
    top=0.75,    # Leave room for the shared top legend.
    hspace=0.2   # adjust vertical spacing between subplots
    )

    # --- New blockCreate a shared top legend ---

    # 1. Get legend handles and labels from both subplots
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()

    # 2. Combine them into one list
    #    Keep all handles and labels; trend lines are unlabeled.
    #    Slice this list if trend lines should be excluded after adding labels.
    handles = h1 + h2 
    labels = l1 + l2

    # 3. Create a figure-level legend
    fig.legend(handles, labels,
           loc='upper center',      # Anchor the legend at the upper center.
           bbox_to_anchor=(0.5, 0.99), # Place the anchor at the top center of the figure
           ncol=1,                  # Use one legend column.
           frameon=False,           
           fontsize=TICK_FONTSIZE)
    
    plt.xticks(rotation=45, ha='right')
    # --- 4. Save figures ---
    output_dir = "output_line_graph"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_filename = "annual_events_multi_panel_comparison"
    plt.savefig(f"{output_dir}/{output_filename}.pdf", bbox_inches='tight')
    plt.savefig(f"{output_dir}/{output_filename}.png", dpi=600, bbox_inches='tight')
    
    print(f"Plot complete. Multi-panel comparison line chart saved as {output_filename}.pdf and {output_filename}.png")
    plt.close(fig)

if __name__ == "__main__":
    create_multi_panel_plot()
