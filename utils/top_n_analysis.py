import requests
import pandas as pd
import concurrent.futures
from collections import Counter
import streamlit as st  # Import streamlit for caching

# --- Caching for Player Data ---


@st.cache_data(show_spinner=False)
def load_player_data(_session):  # Pass session as an argument
    """Load player data (id, web_name) with caching."""
    print("Fetching bootstrap-static data...")  # Add print statement to see when it runs
    try:
        req = _session.get(
            "https://fantasy.premierleague.com/api/bootstrap-static/", timeout=10)
        if req.status_code == 200:
            data = req.json()["elements"]
            df_players = pd.DataFrame(data)[["id", "web_name"]]
            print("Bootstrap-static data fetched successfully.")
            return df_players
        else:
            st.error(f"Failed to fetch players' data: {req.status_code}")
            return pd.DataFrame(columns=["id", "web_name"])
    except requests.exceptions.RequestException as e:
        st.error(f"Error loading player data: {str(e)}")
        return pd.DataFrame(columns=["id", "web_name"])

# --- Helper Function to Fetch Manager Data ---


def fetch_data_for_manager(manager_id, current_gw, _session):
    """Fetch both picks and transfers for a manager using the provided session."""
    results = {}
    picks_url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{current_gw}/picks/"
    transfers_url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/transfers/"

    try:
        # Fetch picks data
        picks_response = _session.get(picks_url, timeout=5)
        if picks_response.status_code == 200:
            results['picks'] = picks_response.json()
        else:
            # Log quietly, main function will handle overall progress/errors
            print(
                f"Failed picks fetch for {manager_id}: {picks_response.status_code}")

        # Fetch transfers data
        transfers_response = _session.get(transfers_url, timeout=5)
        if transfers_response.status_code == 200:
            results['transfers'] = transfers_response.json()
        else:
            print(
                f"Failed transfers fetch for {manager_id}: {transfers_response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Request error for manager {manager_id}: {str(e)}")
        # Return empty results on error, allows processing to continue
        return manager_id, {}

    return manager_id, results

# --- Main Analysis Function ---


def analyze_top_n_managers(df, top_n, current_gw, _session, max_workers=20):
    """
    Analyzes picks, transfers, and chip usage for the top N managers.
    Correctly identifies Triple Captain picks.

    Args:
        df (pd.DataFrame): DataFrame containing league standings including 'entry' and 'rank'.
        top_n (int): The number of top managers to analyze.
        current_gw (int): The gameweek to analyze.
        _session (requests.Session): The requests session object.
        max_workers (int): Max workers for ThreadPoolExecutor.

    Returns:
        tuple: Contains DataFrames/dict for captains, transfers in/out, chips, manager picks, triple captains.
               Returns None for all if basic data fetching fails.
    """
    # Filter the DataFrame to include only the top N players
    actual_n = min(top_n, len(df))
    if actual_n <= 0:
        st.warning("N must be greater than 0.")
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}, pd.DataFrame(), pd.DataFrame())

    df_top_players = df.nsmallest(actual_n, "rank")
    manager_ids = df_top_players["entry"].tolist()

    # Initialize data structures
    player_stats = {}  # Using dict for easier updates: {player_id: {counts}}
    chip_counts = {"wildcard": 0, "3xc": 0, "bboost": 0,
                   "freehit": 0, "manager": 0}  # Added manager chip
    # --- Counter for Triple Captain and Manager picks ---
    triple_captain_picks = Counter()
    manager_picks = Counter()  # NEW: Counter for manager picks

    # Fetch player data (uses caching)
    df_players = load_player_data(_session)
    if df_players.empty:
        st.error("Could not load player names, aborting detailed analysis.")
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}, pd.DataFrame(), pd.DataFrame())

    # --- Parallel fetch data for all managers ---
    manager_data = {}
    print(f"Fetching detailed data for top {actual_n} managers...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_manager = {
            executor.submit(fetch_data_for_manager, manager_id, current_gw, _session): manager_id
            for manager_id in manager_ids
        }

        # Process results as they complete
        for i, future in enumerate(concurrent.futures.as_completed(future_to_manager)):
            manager_id, results = future.result()
            if results:  # Only store if we got some data back
                manager_data[manager_id] = results
            # Optionally update a progress bar here if passed from app.py

    print(f"Processing collected data for {len(manager_data)} managers...")
    # --- Process all the collected data ---
    for manager_id, data in manager_data.items():
        # Process picks data
        # Check if picks data exists and is not None
        if 'picks' in data and data['picks']:
            picks_data = data['picks']
            active_chip = picks_data.get("active_chip")  # Can be None

            # Count chip usage
            if active_chip and active_chip in chip_counts:
                chip_counts[active_chip] += 1
            elif active_chip:
                print(
                    f"Warning: Unknown chip '{active_chip}' used by manager {manager_id}")

            # Process individual picks for general stats and specific chip stats
            captain_found_for_tc = False  # Flag for TC check
            manager_found = False  # Flag for manager chip
            for pick in picks_data.get("picks", []):
                player_id = pick.get("element")
                position = pick.get("position")
                is_captain = pick.get("is_captain", False)
                is_vice_captain = pick.get("is_vice_captain", False)

                if player_id not in player_stats:
                    player_stats[player_id] = {
                        "captain_count": 0, "vice_captain_count": 0,
                        "transfer_in_count": 0, "transfer_out_count": 0
                    }

                # Increment general captain/vice-captain counts
                if is_captain:
                    player_stats[player_id]["captain_count"] += 1
                if is_vice_captain:
                    player_stats[player_id]["vice_captain_count"] += 1

                # Check for Triple Captain
                if active_chip == '3xc' and is_captain and not captain_found_for_tc:
                    triple_captain_picks[player_id] += 1
                    captain_found_for_tc = True

                # Check for Manager Chip selection (position 16)
                if active_chip == 'manager' and position == 16 and not manager_found:
                    manager_picks[player_id] += 1
                    manager_found = True

        # Process transfers data
        # Check if transfers data exists
        if 'transfers' in data and data['transfers']:
            transfers = data['transfers']
            for transfer in transfers:
                # Ensure the transfer happened *for the current gameweek*
                if transfer.get("event") == current_gw:
                    element_in = transfer.get("element_in")
                    element_out = transfer.get("element_out")

                    # Ensure player entries exist in stats dict before incrementing
                    if element_in not in player_stats:
                        player_stats[element_in] = {
                            "captain_count": 0, "vice_captain_count": 0, "transfer_in_count": 0, "transfer_out_count": 0}
                    if element_out not in player_stats:
                        player_stats[element_out] = {
                            "captain_count": 0, "vice_captain_count": 0, "transfer_in_count": 0, "transfer_out_count": 0}

                    if element_in:  # Check if element_in is not None or 0
                        player_stats[element_in]["transfer_in_count"] += 1
                    if element_out:  # Check if element_out is not None or 0
                        player_stats[element_out]["transfer_out_count"] += 1

    print("Creating results dataframes...")
    # --- Convert statistics to DataFrames ---
    if not player_stats and not triple_captain_picks:  # Check both general stats and TC picks
        st.warning("No player statistics collected for the top N managers.")
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), chip_counts, pd.DataFrame(), pd.DataFrame())

    # --- General Stats DataFrame (Captains, Transfers) ---
    df_stats = pd.DataFrame.from_dict(player_stats, orient='index')
    df_stats.index.name = 'Player_ID'
    df_stats = df_stats.merge(
        df_players, left_index=True, right_on="id", how="left")  # Add player names

    # Filter out players with no name (shouldn't happen if df_players loaded)
    df_stats = df_stats.dropna(subset=['web_name'])

    # Captains (Top 10 - All captains)
    df_captains = df_stats[df_stats["captain_count"] > 0].sort_values(
        by="captain_count", ascending=False).head(10)
    df_captains = df_captains[["web_name", "captain_count"]]

    # Transfers In (Top 10)
    df_transfers_in = df_stats[df_stats["transfer_in_count"] > 0].sort_values(
        by="transfer_in_count", ascending=False).head(10)
    df_transfers_in = df_transfers_in[["web_name", "transfer_in_count"]]

    # Transfers Out (Top 10)
    df_transfers_out = df_stats[df_stats["transfer_out_count"] > 0].sort_values(
        by="transfer_out_count", ascending=False).head(10)
    df_transfers_out = df_transfers_out[["web_name", "transfer_out_count"]]

    # --- Manager Picks DataFrame ---
    df_manager_picks = pd.DataFrame()  # Default empty
    if manager_picks:  # Check if the counter has any entries
        # Convert the Counter to a DataFrame
        df_mp = pd.DataFrame(manager_picks.items(), columns=[
                             'Player_ID', 'manager_pick_count'])
        # Merge with player names
        df_mp = df_mp.merge(df_players, left_on='Player_ID',
                            right_on='id', how='left')
        # Select columns and sort
        df_manager_picks = df_mp[['web_name', 'manager_pick_count']].sort_values(
            by='manager_pick_count', ascending=False)
        # Drop rows where merge might have failed
        df_manager_picks = df_manager_picks.dropna(subset=['web_name'])

    # --- Triple Captains DataFrame (from specific counter) ---
    df_triple_captains = pd.DataFrame()  # Default empty
    if triple_captain_picks:  # Check if the counter has any entries
        # Convert the Counter to a DataFrame
        df_tc = pd.DataFrame(triple_captain_picks.items(), columns=[
                             'Player_ID', 'triple_captain_count'])
        # Merge with player names
        df_tc = df_tc.merge(df_players, left_on='Player_ID',
                            right_on='id', how='left')
        # Select columns and sort
        df_triple_captains = df_tc[['web_name', 'triple_captain_count']].sort_values(
            by='triple_captain_count', ascending=False)
        # Drop rows where merge might have failed (though unlikely if df_players is good)
        df_triple_captains = df_triple_captains.dropna(subset=['web_name'])

    print("Top N analysis complete.")
    return df_captains, df_transfers_in, df_transfers_out, chip_counts, df_manager_picks, df_triple_captains
