import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import plot_style

def analyze_polymarket():
    plot_style.apply_plot_style()
    csv_path = "data/polymarket/polymarket-price-data-07-09-2025-07-01-2026-1767785813564.csv"
    
    try:
        df = pd.read_csv(csv_path)
        df['Date'] = pd.to_datetime(df['Date (UTC)'], format='%m-%d-%Y %H:%M')
        
        start = "2025-12-25"
        end = "2026-01-10"
        mask = (df['Date'] >= start) & (df['Date'] <= end)
        event_df = df[mask].copy()
        
        if event_df.empty:
            print("No data in window.")
            return

        print("\n Polymarket Data (Jan 2026 Window) ")
        print(event_df[['Date', 'January 31', 'March 31']].head(10))
        
        jan2 = event_df[event_df['Date'].dt.date == pd.Timestamp("2026-01-02").date()]
        if not jan2.empty:
            prob = jan2['January 31'].mean()
            print(f"\nJan 2 Probability (Event by Jan 31): {prob:.3f}")
            
            plt.figure(figsize=(10, 5))
            plt.plot(event_df['Date'], event_df['January 31'], label='Polymarket: Maduro Out by Jan 31', color='#9467bd', linewidth=2)
            plt.axvline(pd.Timestamp("2026-01-02"), color='black', linestyle=':', label='The Leak (Jan 2)')
            
            plt.title("Public Prediction Market: No Leak Detected")
            plt.ylabel("Probability")
            plt.legend()
            
            # Format Date Axis
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
            plt.xticks(rotation=45)
            
            plt.tight_layout()
            plt.savefig("out/Polymarket_Chart.png")
            print("Chart saved to out/Polymarket_Chart.png")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze_polymarket()
