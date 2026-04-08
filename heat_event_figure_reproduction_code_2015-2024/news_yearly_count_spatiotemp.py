import pandas as pd
import numpy as np
import argparse

def deduplicate_news_events(input_path, output_path, resolution=1.0):
    """
    Read news data and use spatial gridding to handle small coordinate differences,
    merging duplicate reports from the same day and spatial area into a single event.
    """
    print(f"--- Starting news-data deduplication ---")
    print(f"Input file: {input_path}")
    print(f"Grid resolution: {resolution} degrees (about {int(resolution*111)} km)")

    # 1. Load data
    # Read only the columns needed for deduplication to save memory.
    use_cols = ['year', 'month', 'day', 'X', 'Y']
    try:
        df = pd.read_csv(input_path, usecols=use_cols)
    except ValueError as e:
        print(f"Error: failed to read columns. Check whether the CSV contains {use_cols}")
        return

    original_count = len(df)
    print(f"Original record count: {original_count}")

    # 2. Clean time columns.
    # Ensure there are no null values and convert to integers.
    df.dropna(subset=['year', 'month', 'day'], inplace=True)
    df['year'] = df['year'].astype(int)
    df['month'] = df['month'].astype(int)
    df['day'] = df['day'].astype(int)

    # 3. Core step: spatial gridding (binning/snapping).
    # Method: divide coordinates by resolution, round to integers, then multiply by resolution.
    # Effect: 116.30001 and 116.29999 both become 116.0 when resolution=1.0.
    print("Running coordinate grid snapping...")
    
    df['lat_bin'] = (df['Y'] / resolution).round(0) * resolution
    df['lon_bin'] = (df['X'] / resolution).round(0) * resolution

    # 4. Run deduplication
    # Define one event as same year, month, day, and spatial grid cell.
    dedup_cols = ['year', 'month', 'day', 'lat_bin', 'lon_bin']
    
    print("Removing duplicate events...")
    df_unique = df.drop_duplicates(subset=dedup_cols)
    
    final_count = len(df_unique)
    
    # Calculate summary metrics
    reduction_rate = (1 - final_count / original_count) * 100
    media_multiplier = original_count / final_count

    print(f"\n==========================================")
    print(f"             Processing summary")
    print(f"==========================================")
    print(f"Original media volume (Media Volume): {original_count}")
    print(f"Unique events (Unique Events): {final_count}")
    print(f"------------------------------------------")
    print(f"Duplicate redundancy rate: {reduction_rate:.2f}%")
    print(f"Media amplification multiplier: {media_multiplier:.2f}x") 
    print(f"(On average, each physical event generated {media_multiplier:.2f} reports)")
    print(f"==========================================\n")

    # 5. Generate annual statistics for plotting the blue line.
    yearly_counts = df_unique.groupby('year').size().reset_index(name='count')
    
    # Sort
    yearly_counts = yearly_counts.sort_values('year')

    # 6. Save results
    yearly_counts.to_csv(output_path, index=False)
    print(f"Final annual statistics saved to: {output_path}")
    print("Preview first five rows:")
    print(yearly_counts.head())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Deduplicate raw geocoded news records by date and spatial grid cell."
    )
    parser.add_argument("--input", default="raw_news_events.csv")
    parser.add_argument("--output", default="news_events_yearly_deduplicated.csv")
    parser.add_argument("--resolution", type=float, default=1.0)
    args = parser.parse_args()

    deduplicate_news_events(args.input, args.output, resolution=args.resolution)
