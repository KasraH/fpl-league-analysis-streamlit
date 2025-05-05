import streamlit as st
import pandas as pd
# Import session from fpl_api and the league fetching function
from utils.fpl_api import get_league_standings, session
from utils.calculations import calculate_adjusted_points_for_players
# Import the new analysis function
from utils.top_n_analysis import analyze_top_n_managers

st.set_page_config(layout="wide")
st.title("FPL Mini League Analysis")

# --- Sidebar Inputs ---
league_id = st.sidebar.number_input(
    "Enter League ID:", min_value=1, step=1, value=None, placeholder="League ID...")
current_gw = st.sidebar.number_input(
    "Current Gameweek:", min_value=1, max_value=38, step=1, value=None, placeholder="Gameweek...")
top_n = st.sidebar.number_input(
    "Number of top managers (N) for detailed analysis:",
    min_value=1, value=10, max_value=200, step=1
)
# --- Analysis Mode Selection ---
analysis_mode = st.sidebar.radio(
    "Analysis Mode:",
    ["Quick Analysis (Top Managers Only)", "Full League Analysis"],
    help="""
    Quick Analysis: Only analyzes the top N managers. Best for quick insights into top performers.
    Full League Analysis: Analyzes all managers including chip-adjusted points. Takes longer but provides complete league overview.
    """
)
calculate_all_adjusted = analysis_mode == "Full League Analysis"

# --- Helper function to display DataFrames nicely ---


def display_df(title, dataframe):
    st.markdown(f"##### {title}")
    if dataframe is not None and not dataframe.empty:
        display_dataframe = dataframe.reset_index(drop=True)
        display_dataframe.index = display_dataframe.index + 1
        st.dataframe(display_dataframe)
    else:
        st.info("No data available for this statistic.")


# --- Main Button Logic ---
if st.sidebar.button("Run Analysis"):
    if not league_id or not current_gw:
        st.warning("Please enter both League ID and Current Gameweek.")
        st.stop()

    # --- Main Processing ---
    progress_bar = st.progress(0, text="Initializing...")
    progress_text = st.empty()

    df = pd.DataFrame()
    analysis_results = None
    adjusted_points_calculated = False  # Flag to track if calculation was done

    try:
        # --- Stage 1: Fetch League Standings ---
        progress_text.text("Fetching league standings...")
        progress_bar.progress(10, text="Fetching league standings...")

        # Only fetch required number of managers if user doesn't need full table
        fetch_limit = None if calculate_all_adjusted else top_n
        with st.spinner("Fetching league standings..."):
            df = get_league_standings(
                league_id, current_gw=current_gw, limit=fetch_limit, progress_text=progress_text)

        if df.empty:
            st.error(
                f"Could not retrieve league standings for League ID {league_id}.")
            st.stop()

        # --- Stage 2: Analyze Top N Managers ---
        actual_n = min(top_n, len(df))
        progress_text.text(f"Analyzing top {actual_n} managers...")
        # Adjust progress based on whether next step happens
        next_progress = 33 if calculate_all_adjusted else 50
        progress_bar.progress(
            next_progress, text=f"Analyzing top {actual_n} managers...")
        with st.spinner(f"Running detailed analysis for Top {actual_n} managers..."):
            analysis_results = analyze_top_n_managers(
                df, top_n, current_gw, session, progress_text=progress_text)

        # --- Stage 3: Calculate Adjusted Points (Optional) ---
        if calculate_all_adjusted:
            progress_text.text(
                "Calculating adjusted points for all managers...")
            progress_bar.progress(66, text="Calculating adjusted points...")
            with st.spinner("Calculating adjusted points (this may take a while)..."):
                df = calculate_adjusted_points_for_players(
                    df, current_gw, _session=session, progress_text=progress_text
                )
            adjusted_points_calculated = True  # Set flag
        else:
            progress_text.text("Skipping full adjusted points calculation.")
            # Ensure 'adjusted_event_total' column exists even if skipped, fill with NaN or copy raw
            if 'adjusted_event_total' not in df.columns:
                # Or df['event_total'] if you prefer
                df['adjusted_event_total'] = pd.NA

        progress_bar.progress(100, text="Analysis Complete!")
        st.success("Analysis complete!")
        progress_bar.empty()
        progress_text.empty()

    except Exception as e:
        st.error(f"An error occurred during processing: {e}")
        st.exception(e)
        progress_bar.empty()
        progress_text.empty()
        st.stop()

    # --- Display Results ---

    # Display Top N results first
    if analysis_results:
        st.markdown("---")
        st.subheader(
            f"Detailed Analysis for Top {actual_n} Managers (GW {current_gw})")
        (df_captains, df_transfers_in, df_transfers_out,
         chip_counts, df_manager_picks, df_triple_captains) = analysis_results
        col_detail1, col_detail2 = st.columns(2)
        with col_detail1:
            st.markdown("#### Chip Usage")
            if chip_counts and sum(chip_counts.values()) > 0:
                chip_data_filtered = {
                    k.upper(): v for k, v in chip_counts.items() if v > 0}
                if chip_data_filtered:
                    st.dataframe(pd.Series(chip_data_filtered, name="Count"))
                else:
                    st.info(
                        f"No chips were used by the top {actual_n} managers.")
            else:
                st.info(f"No chips were used by the top {actual_n} managers.")

            display_df("Top Captain Picks", df_captains)
            if chip_counts.get("3xc", 0) > 0:
                display_df("Triple Captain Picks", df_triple_captains)
            if chip_counts.get("manager", 0) > 0:
                display_df("Manager Chip Selections", df_manager_picks)
        with col_detail2:
            display_df("Top Transfers In", df_transfers_in)
            display_df("Top Transfers Out", df_transfers_out)
    else:
        st.warning("Detailed analysis for top N managers could not be completed.")

    # Display Overall Standings (Conditionally include adjusted points)
    st.markdown("---")
    st.subheader("League Standings")
    # Define base columns
    display_cols_main = [
        'rank', 'manager_name', 'team_name', 'manager_id', 'gw_points',
        'captain_name', 'vice_captain_name',
        'chip_used', 'transfer_penalty',
        'overall_rank', 'overall_rank_change', 'overall_rank_change_pct',
        'rank_change', 'pct_rank_change', 'total'
    ]
    # Add net points column if calculated
    if adjusted_points_calculated and 'net_points' in df.columns:
        # Insert after 'gw_points'
        event_total_index = display_cols_main.index('gw_points')
        display_cols_main.insert(event_total_index + 1, 'net_points')

    display_cols_main = [col for col in display_cols_main if col in df.columns]
    df_display = df[display_cols_main].copy()

    # Create user-friendly column names for display
    column_renames = {
        'manager_name': 'Manager',
        'team_name': 'Team',
        'manager_id': 'ID',
        'gw_points': 'GW Points',
        'net_points': 'Net Points',
        'captain_name': 'Captain',
        'vice_captain_name': 'Vice Captain',
        'chip_used': 'Chip Used',
        'transfer_penalty': 'Transfer Cost',
        'overall_rank': 'Overall Rank',
        'overall_rank_change': 'OR Change',
        'overall_rank_change_pct': 'OR Change %',
        'rank_change': 'Rank Change',
        'pct_rank_change': 'Rank Change %',
        'total': 'Total Points'
    }

    # Format chip names to be more readable
    if 'chip_used' in df_display.columns:
        chip_map = {
            '3xc': 'Triple Captain',
            'bboost': 'Bench Boost',
            'wildcard': 'Wildcard',
            'freehit': 'Free Hit',
            'manager': 'Manager'
        }
        df_display['chip_used'] = df_display['chip_used'].map(
            lambda x: chip_map.get(x, x))

    # Rename columns for display
    df_display.rename(columns=column_renames, inplace=True)

    if 'rank' in df_display.columns:
        df_display.set_index('rank', inplace=True)
        df_display.index.name = None
    st.dataframe(df_display)

    # Display General League Statistics (Conditionally include adjusted stats)
    st.markdown("---")
    st.subheader(f"General League Statistics (GW {current_gw})")
    col1, col2 = st.columns(2)
    with col1:
        if "gw_points" in df.columns:
            top_points_week = df.loc[df["gw_points"] == df["gw_points"].max()]
            display_cols = ['manager_name',
                            'team_name', 'manager_id', 'gw_points']
            top_points_week = top_points_week[display_cols].rename(columns={
                'manager_id': 'ID',
                'manager_name': 'Manager',
                'team_name': 'Team',
                'gw_points': 'GW Points'
            })
            display_df("🏆 Top Points (Raw)", top_points_week)

        if adjusted_points_calculated and "net_points" in df.columns:
            net_points_valid = df["net_points"].dropna()
            if not net_points_valid.empty:
                # Filter out managers who used the Free Hit chip
                no_chips_df = df[df["chip_used"] != "freehit"]
                top_points_week_without_chips = no_chips_df.loc[no_chips_df["net_points"] == no_chips_df["net_points"].max(
                )]
                display_cols = ['manager_name',
                                'team_name', 'manager_id', 'net_points']
                top_points_week_without_chips = top_points_week_without_chips[display_cols].rename(columns={
                    'manager_id': 'ID',
                    'manager_name': 'Manager',
                    'team_name': 'Team',
                    'net_points': 'Net Points'
                })
                display_df("🏆 Top Points (Without Chips)",
                           top_points_week_without_chips)
            else:
                st.info("Net points not calculated or available.")

        if "rank_change" in df.columns:
            valid_rank_change = df["rank_change"].dropna()
            if not valid_rank_change.empty:
                most_improved = df.loc[df["rank_change"]
                                       == valid_rank_change.max()]
                display_cols = ['manager_name', 'team_name',
                                'manager_id', 'rank_change', 'rank']
                most_improved = most_improved[display_cols].rename(columns={
                    'manager_id': 'ID',
                    'manager_name': 'Manager',
                    'team_name': 'Team',
                    'rank_change': 'Rank Change'
                })
                display_df("📈 Most Improved Rank", most_improved)
            else:
                display_df("📈 Most Improved Rank", pd.DataFrame())

    with col2:
        # Biggest rank drop (absolute)
        if "rank_change" in df.columns:
            valid_rank_change = df["rank_change"].dropna()
            if not valid_rank_change.empty:
                most_dropped = df.loc[df["rank_change"]
                                      == valid_rank_change.min()]
                display_cols = ['manager_name', 'team_name',
                                'manager_id', 'rank_change', 'rank']
                most_dropped = most_dropped[display_cols].rename(columns={
                    'manager_id': 'ID',
                    'manager_name': 'Manager',
                    'team_name': 'Team',
                    'rank_change': 'Rank Change'
                })
                display_df("📉 Biggest Rank Drop", most_dropped)
            else:
                display_df("📉 Biggest Rank Drop", pd.DataFrame())

        # Rank changes by percentage
        if "pct_rank_change" in df.columns:
            valid_pct_change = df["pct_rank_change"].dropna().replace(
                [float('inf'), -float('inf')], None).dropna()
            if not valid_pct_change.empty:
                most_improved_pct = df.loc[df["pct_rank_change"]
                                           == valid_pct_change.max()]
                display_cols = ['manager_name', 'team_name',
                                'manager_id', 'pct_rank_change', 'rank']
                most_improved_pct = most_improved_pct[display_cols].rename(columns={
                    'manager_id': 'ID',
                    'manager_name': 'Manager',
                    'team_name': 'Team',
                    'pct_rank_change': 'Rank Change %'
                })
                display_df("📈 Most Improved Rank (%)", most_improved_pct)

                most_dropped_pct = df.loc[df["pct_rank_change"]
                                          == valid_pct_change.min()]
                most_dropped_pct = most_dropped_pct[display_cols].rename(columns={
                    'manager_id': 'ID',
                    'manager_name': 'Manager',
                    'team_name': 'Team',
                    'pct_rank_change': 'Rank Change %'
                })
                display_df("📉 Biggest Rank Drop (%)", most_dropped_pct)
            else:
                display_df("📈 Most Improved Rank (%)", pd.DataFrame())
                display_df("📉 Biggest Rank Drop (%)", pd.DataFrame())

    # Display Overall Rank Change Statistics
    st.markdown("---")
    st.subheader(f"Overall Rank Change Statistics (GW {current_gw})")
    col1, col2 = st.columns(2)

    with col1:
        # Most improved overall rank (absolute)
        if "overall_rank_change" in df.columns:
            valid_rank_change = df["overall_rank_change"].dropna()
            if not valid_rank_change.empty:
                most_improved = df.loc[df["overall_rank_change"]
                                       == valid_rank_change.max()]
                display_cols = ['manager_name', 'team_name', 'manager_id',
                                'overall_rank', 'prev_overall_rank', 'overall_rank_change']
                most_improved = most_improved[display_cols].rename(columns={
                    'manager_id': 'ID',
                    'manager_name': 'Manager',
                    'team_name': 'Team',
                    'overall_rank_change': 'Rank Change'
                })
                display_df("📈 Most Improved Overall Rank", most_improved)
            else:
                st.info("Overall rank change data not available.")

        # Most improved overall rank by percentage
        if "overall_rank_change_pct" in df.columns:
            valid_pct_change = df["overall_rank_change_pct"].dropna().replace(
                [float('inf'), -float('inf')], None).dropna()
            if not valid_pct_change.empty:
                most_improved_pct = df.loc[df["overall_rank_change_pct"]
                                           == valid_pct_change.max()]
                display_cols = ['manager_name', 'team_name', 'manager_id',
                                'overall_rank', 'prev_overall_rank', 'overall_rank_change_pct']
                most_improved_pct = most_improved_pct[display_cols].rename(columns={
                    'manager_id': 'ID',
                    'manager_name': 'Manager',
                    'team_name': 'Team',
                    'overall_rank_change_pct': 'Rank Change %'
                })
                display_df("📈 Most Improved Overall Rank (%)",
                           most_improved_pct)

    with col2:
        # Biggest overall rank drop (absolute)
        if "overall_rank_change" in df.columns:
            valid_rank_change = df["overall_rank_change"].dropna()
            if not valid_rank_change.empty:
                most_dropped = df.loc[df["overall_rank_change"]
                                      == valid_rank_change.min()]
                display_cols = ['manager_name', 'team_name', 'manager_id',
                                'overall_rank', 'prev_overall_rank', 'overall_rank_change']
                most_dropped = most_dropped[display_cols].rename(columns={
                    'manager_id': 'ID',
                    'manager_name': 'Manager',
                    'team_name': 'Team',
                    'overall_rank_change': 'Rank Change'
                })
                display_df("📉 Biggest Overall Rank Drop", most_dropped)

        # Biggest overall rank drop by percentage
        if "overall_rank_change_pct" in df.columns:
            valid_pct_change = df["overall_rank_change_pct"].dropna().replace(
                [float('inf'), -float('inf')], None).dropna()
            if not valid_pct_change.empty:
                most_dropped_pct = df.loc[df["overall_rank_change_pct"]
                                          == valid_pct_change.min()]
                display_cols = ['manager_name', 'team_name', 'manager_id',
                                'overall_rank', 'prev_overall_rank', 'overall_rank_change_pct']
                most_dropped_pct = most_dropped_pct[display_cols].rename(columns={
                    'manager_id': 'ID',
                    'manager_name': 'Manager',
                    'team_name': 'Team',
                    'overall_rank_change_pct': 'Rank Change %'
                })
                display_df("� Biggest Overall Rank Drop (%)", most_dropped_pct)

    # Display Top N Average Stats (Conditionally include adjusted avg)
    st.markdown("---")
    st.subheader(
        f"Average Stats for Top {actual_n} Managers (GW {current_gw})")
    if actual_n > 0:
        top_n_df = df.nsmallest(actual_n, 'rank')
        if not top_n_df.empty:
            avg_gw_points_top_n = top_n_df['gw_points'].mean(skipna=True)
            avg_overall_rank_top_n = top_n_df['overall_rank'].mean(skipna=True)

            # Determine number of columns needed
            num_cols = 3 if adjusted_points_calculated else 2
            cols = st.columns(num_cols)

            cols[0].metric(label=f"Avg GW Points (Top {actual_n})", value=f"{avg_gw_points_top_n:.2f}" if pd.notna(
                avg_gw_points_top_n) else "N/A")

            # --- Conditional Display ---
            if adjusted_points_calculated:
                avg_net_points_top_n = top_n_df['net_points'].mean(
                    skipna=True)
                cols[1].metric(label=f"Avg Net Points (Top {actual_n})", value=f"{avg_net_points_top_n:.2f}" if pd.notna(
                    avg_net_points_top_n) else "N/A")
                rank_col_index = 2
            else:
                rank_col_index = 1  # If only 2 columns, rank is the second one

            if pd.notna(avg_overall_rank_top_n):
                cols[rank_col_index].metric(
                    label=f"Avg Overall Rank (Top {actual_n})", value=f"{avg_overall_rank_top_n:,.0f}")
            else:
                cols[rank_col_index].info(f"Avg Overall Rank N/A")
        else:
            st.warning("Could not calculate average stats for top N.")
    else:
        st.warning("N must be > 0.")
