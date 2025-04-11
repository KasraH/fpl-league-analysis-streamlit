import streamlit as st
import pandas as pd
# Import session from fpl_api and the league fetching function
from utils.fpl_api import get_league_standings, session
from utils.calculations import calculate_adjusted_points_for_players
# Import the new analysis function
from utils.top_n_analysis import analyze_top_n_managers

st.set_page_config(layout="wide")  # Use wider layout for more space
st.title("FPL Mini League Analysis")

# --- Sidebar Inputs ---
league_id = st.sidebar.number_input(
    "Enter League ID:", min_value=1, step=1, value=None, placeholder="League ID...")
current_gw = st.sidebar.number_input(
    "Current Gameweek:", min_value=1, max_value=38, step=1, value=None, placeholder="Gameweek...")
top_n = st.sidebar.number_input(
    "Number of top managers (N) for detailed analysis:",
    min_value=1,
    value=10,
    max_value=200,  # Example: Limit N to 200
    step=1
)

# --- Helper function to display DataFrames nicely (defined once) ---


def display_df(title, dataframe):
    st.markdown(f"##### {title}")  # Use smaller heading
    if dataframe is not None and not dataframe.empty:
        # Reset index to get 0-based index, then add 1
        display_dataframe = dataframe.reset_index(drop=True)
        display_dataframe.index = display_dataframe.index + 1
        st.dataframe(display_dataframe)  # Display with 1-based index
    else:
        st.info("No data available for this statistic.")


# --- Main Button Logic ---
if st.sidebar.button("Run Analysis"):
    if not league_id or not current_gw:
        st.warning("Please enter both League ID and Current Gameweek.")
        st.stop()

    # --- Main Processing ---
    progress_bar = st.progress(0, text="Initializing...")
    progress_text = st.empty()  # Placeholder for detailed status

    df = pd.DataFrame()  # Initialize df as empty
    analysis_results = None  # Initialize analysis results

    try:
        progress_text.text("Fetching league standings...")
        progress_bar.progress(10, text="Fetching league standings...")
        with st.spinner("Fetching league standings..."):
            df = get_league_standings(league_id)  # Uses session internally

        if df.empty:
            st.error(
                f"Could not retrieve league standings for League ID {league_id}. Please check the ID and try again.")
            st.stop()

        progress_text.text("Calculating adjusted points...")
        progress_bar.progress(33, text="Calculating adjusted points...")
        with st.spinner("Calculating adjusted points (this may take a while)..."):
            # Pass the shared session to the calculation function
            df = calculate_adjusted_points_for_players(
                # Pass session and progress_text
                df, current_gw, _session=session, progress_text=progress_text
            )

        progress_text.text(f"Analyzing top {top_n} managers...")
        progress_bar.progress(66, text=f"Analyzing top {top_n} managers...")
        with st.spinner(f"Running detailed analysis for Top {top_n} managers..."):
            # Pass the main DataFrame, N, GW, and the shared session
            analysis_results = analyze_top_n_managers(
                df, top_n, current_gw, session)  # Pass session

        progress_bar.progress(100, text="Analysis Complete!")
        st.success("Analysis complete!")
        progress_bar.empty()  # Remove progress bar on completion
        progress_text.empty()  # Remove progress text

    except Exception as e:
        st.error(f"An error occurred during processing: {e}")
        # Optionally show more detailed traceback for debugging
        st.exception(e)
        progress_bar.empty()
        progress_text.empty()
        st.stop()

    # --- Display Results ---
    st.subheader("Overall Standings with Adjusted Points")
    # Select/reorder columns for the main table - ADD 'pct_rank_change' HERE
    display_cols_main = [
        'rank', 'player_name', 'entry_name', 'event_total',
        'adjusted_event_total', 'overall_rank', 'rank_change', 'pct_rank_change', 'total'
    ]
    # Ensure columns exist before trying to display them
    display_cols_main = [col for col in display_cols_main if col in df.columns]

    # Create a copy to avoid modifying the original df if needed elsewhere
    df_display = df[display_cols_main].copy()

    # Check if 'rank' column exists before setting it as index
    if 'rank' in df_display.columns:
        # Set 'rank' as the index for display purposes
        df_display.set_index('rank', inplace=True)
        # Remove the index name ('rank') from being displayed
        df_display.index.name = None

    # Display the DataFrame without the default 0-based index
    st.dataframe(df_display)

    st.markdown("---")  # Separator

    # --- Display General League Statistics ---
    st.subheader(f"General League Statistics (GW {current_gw})")
    col1, col2 = st.columns(2)
    with col1:
        # Top points of the week (based on event_total)
        if "event_total" in df.columns:
            top_points_week = df.loc[df["event_total"]
                                     == df["event_total"].max()]
            display_df("🏆 Top Points (Raw)", top_points_week[[
                       'player_name', 'entry_name', 'event_total']])

        # Top points of the week without using chips
        if "adjusted_event_total" in df.columns:
            top_points_week_without_chips = df.loc[df["adjusted_event_total"]
                                                   == df["adjusted_event_total"].max()]
            display_df("🏆 Top Points (Adjusted)", top_points_week_without_chips[[
                       'player_name', 'entry_name', 'adjusted_event_total']])

        # Most improved rank (absolute)
        if "rank_change" in df.columns:
            valid_rank_change = df["rank_change"].dropna()
            if not valid_rank_change.empty:
                most_improved = df.loc[df["rank_change"]
                                       == valid_rank_change.max()]
                display_df("📈 Most Improved Rank", most_improved[[
                           'player_name', 'entry_name', 'rank_change', 'rank']])
            else:
                # Show title even if empty
                display_df("📈 Most Improved Rank", pd.DataFrame())

    with col2:
        # Biggest rank drop (absolute)
        if "rank_change" in df.columns:
            # Recalculate or use from above
            valid_rank_change = df["rank_change"].dropna()
            if not valid_rank_change.empty:
                most_dropped = df.loc[df["rank_change"]
                                      == valid_rank_change.min()]
                display_df("📉 Biggest Rank Drop", most_dropped[[
                           'player_name', 'entry_name', 'rank_change', 'rank']])
            else:
                display_df("📉 Biggest Rank Drop", pd.DataFrame())

        # Most improved rank by percentage (requires pct_rank_change column)
        if "pct_rank_change" in df.columns:
            valid_pct_change = df["pct_rank_change"].dropna().replace(
                [float('inf'), -float('inf')], None).dropna()
            if not valid_pct_change.empty:
                most_improved_pct = df.loc[df["pct_rank_change"]
                                           == valid_pct_change.max()]
                display_df("📈 Most Improved Rank (%)", most_improved_pct[[
                           'player_name', 'entry_name', 'pct_rank_change', 'rank']])
            else:
                display_df("📈 Most Improved Rank (%)", pd.DataFrame())

            # Biggest rank drop by percentage
            if not valid_pct_change.empty:
                most_dropped_pct = df.loc[df["pct_rank_change"]
                                          == valid_pct_change.min()]
                display_df("📉 Biggest Rank Drop (%)", most_dropped_pct[[
                           'player_name', 'entry_name', 'pct_rank_change', 'rank']])
            else:
                display_df("📉 Biggest Rank Drop (%)", pd.DataFrame())

    # --- Display Top N Average Stats ---
    st.markdown("---")
    st.subheader(f"Average Stats for Top {top_n} Managers (GW {current_gw})")
    actual_n = min(top_n, len(df))
    if actual_n > 0:
        top_n_df = df.nsmallest(actual_n, 'rank')
        if not top_n_df.empty:
            avg_event_total_top_n = top_n_df['event_total'].mean(skipna=True)
            avg_adjusted_total_top_n = top_n_df['adjusted_event_total'].mean(
                skipna=True)
            avg_overall_rank_top_n = top_n_df['overall_rank'].mean(skipna=True)

            col_avg1, col_avg2, col_avg3 = st.columns(3)
            col_avg1.metric(label=f"Avg GW Points (Top {actual_n})", value=f"{avg_event_total_top_n:.2f}" if pd.notna(
                avg_event_total_top_n) else "N/A")
            col_avg2.metric(label=f"Avg Adjusted GW Points (Top {actual_n})", value=f"{avg_adjusted_total_top_n:.2f}" if pd.notna(
                avg_adjusted_total_top_n) else "N/A")
            if pd.notna(avg_overall_rank_top_n):
                col_avg3.metric(
                    label=f"Avg Overall Rank (Top {actual_n})", value=f"{avg_overall_rank_top_n:,.0f}")
            else:
                col_avg3.info(f"Avg Overall Rank N/A")
        else:
            st.warning("Could not calculate average stats for top N.")
    else:
        st.warning("N must be > 0.")

    # --- Display Detailed Top N Analysis Results ---
    if analysis_results:
        st.markdown("---")
        st.subheader(
            f"Detailed Analysis for Top {actual_n} Managers (GW {current_gw})")
        (df_captains, df_transfers_in, df_transfers_out,
         chip_counts, df_manager_picks, df_triple_captains) = analysis_results

        col_detail1, col_detail2 = st.columns(2)

        with col_detail1:
            # Keep this title as it's not using display_df
            st.markdown("#### Chip Usage")
            if chip_counts and sum(chip_counts.values()) > 0:
                # Filter out chips with 0 count before creating Series
                chip_data_filtered = {
                    k.upper(): v for k, v in chip_counts.items() if v > 0}
                if chip_data_filtered:  # Check if dict is not empty after filtering
                    st.dataframe(pd.Series(chip_data_filtered, name="Count"))
                else:
                    st.info("No chips were used by the top N managers.")
            else:
                st.info("No chips were used by the top N managers.")

            # This will display the title
            display_df("Top Captain Picks", df_captains)

            # Only show if TC chip was used AND there are results
            if chip_counts.get("3xc", 0) > 0:
                # This will display the title
                display_df("Triple Captain Picks", df_triple_captains)
            # else: Don't display section if TC wasn't used

        with col_detail2:
            # This will display the title
            display_df("Top Transfers In", df_transfers_in)

            # This will display the title
            display_df("Top Transfers Out", df_transfers_out)

            # Placeholder for Manager Picks if implemented
            # display_df("Top Manager Picks", df_manager_picks)

    else:
        st.warning(
            "Detailed analysis for top N managers could not be completed or returned no results.")
