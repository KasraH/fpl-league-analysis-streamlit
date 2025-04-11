import time
import requests
import concurrent.futures
import streamlit as st
import pandas as pd  # Added missing import

# Create a session object for reuse in this module
session = requests.Session()

# --- Caching for Player Points ---


@st.cache_data(show_spinner=False)
def get_player_points(player_id, gw, _session):  # Accept session
    """Fetches points for a specific player in a specific gameweek using the provided session."""
    cache_key = f"{player_id}_{gw}"  # Cache key might not be needed with st.cache_data
    url = f"https://fantasy.premierleague.com/api/element-summary/{player_id}/"
    try:
        # Use the passed session object
        response = _session.get(url, timeout=5)
    except requests.exceptions.RequestException as e:
        print(f"Request error fetching player points for {player_id}: {e}")
        return 0  # Return 0 on error

    if response.status_code != 200:
        print(
            f"Error fetching player points for {player_id} GW {gw}: {response.status_code}")
        return 0  # Return 0 on non-200 status

    player_data = response.json().get("history", [])
    # Find the specific gameweek data
    for record in player_data:
        if record.get("round") == gw:
            return record.get("total_points", 0)

    return 0  # Return 0 if gameweek not found


# --- Get Manager's GW Points and Chip Info ---
def get_manager_gw_points(manager_id, gw, _session):  # Accept session
    """Fetches manager's points, chip used, and transfer cost for a gameweek using the provided session."""
    url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{gw}/picks/"
    try:
        # Use the passed session object
        response = _session.get(url, timeout=5)
    except requests.exceptions.RequestException as e:
        print(f"Request error fetching manager picks for {manager_id}: {e}")
        return None  # Indicate failure

    if response.status_code != 200:
        print(
            f"Error fetching manager picks for {manager_id} GW {gw}: {response.status_code}")
        return None  # Indicate failure

    data = response.json()
    entry_history = data.get("entry_history", {})
    # Raw points for the GW (includes chip effects)
    points = entry_history.get("points", 0)
    # Chip used (e.g., 'bboost', '3xc', None)
    active_chip = data.get("active_chip")
    transfers_cost = entry_history.get(
        "event_transfers_cost", 0)  # Cost of transfers

    # --- Calculate Chip Point Effects (to subtract them) ---
    chip_point_effect = 0
    picks = data.get("picks", [])

    if active_chip == "bboost":
        # Sum points of players who were on the bench (position > 11)
        for pick in picks:
            # Bench players have multiplier 0 unless bboost is active, then it's 1
            # A simpler check might be position > 11
            if pick.get("position", 0) > 11:
                # Pass session to get_player_points
                chip_point_effect += get_player_points(
                    pick['element'], gw, _session)
    elif active_chip == "3xc":
        # Find the captain and add their points once more (since raw points include 3x already)
        for pick in picks:
            if pick.get("is_captain", False):
                # Pass session to get_player_points
                chip_point_effect += get_player_points(
                    pick['element'], gw, _session)
                break  # Only one captain

    # Adjusted points = Raw Points - Transfer Costs - Chip Point Effect
    adjusted_points = points - transfers_cost - chip_point_effect
    return adjusted_points


# --- Process a Single Manager Row ---
def process_manager(row, gw, _session):  # Accept session
    """Fetches and calculates adjusted points for a single manager row using the provided session."""
    manager_id = row["entry"]
    original_event_total = row["event_total"]  # Keep original as fallback

    # Pass session to the function that makes the API call
    adjusted_points = get_manager_gw_points(manager_id, gw, _session)

    # If fetching/calculation failed, return the original event total (or pd.NA)
    return adjusted_points if adjusted_points is not None else original_event_total


# --- Calculate Adjusted Points for the Entire DataFrame ---
def calculate_adjusted_points_for_players(df, gw, _session, progress_text=None, max_workers=20, batch_size=50):
    """Updates DataFrame by calculating adjusted points with parallelization, batching, and optional progress updates, using the provided session."""
    all_results = []
    total_managers = len(df)
    results_dict = {}  # Use dict to store results by index

    if progress_text:
        progress_text.text(
            f"Calculating adjusted points for {total_managers} managers...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create futures, passing the session to process_manager
        future_to_index = {
            # Pass session here
            executor.submit(process_manager, row, gw, _session): index
            for index, row in df.iterrows()
        }

        for i, future in enumerate(concurrent.futures.as_completed(future_to_index)):
            original_index = future_to_index[future]
            try:
                result = future.result()
                results_dict[original_index] = result
            except Exception as exc:
                manager_id = df.loc[original_index, 'entry']
                print(f"Manager ID {manager_id} generated an exception: {exc}")
                # Fallback to original event total or NaN
                results_dict[original_index] = df.loc[original_index,
                                                      'event_total']  # Or pd.NA

            # Update progress text periodically
            if progress_text and (i + 1) % 10 == 0:  # Update every 10 managers
                progress_text.text(
                    f"Calculated adjusted points for {i+1}/{total_managers} managers...")

    # Reconstruct results in the original order using the index
    all_results = [results_dict[index] for index in df.index]

    df["adjusted_event_total"] = all_results
    # Ensure the column is numeric, coercing errors to NaN
    df["adjusted_event_total"] = pd.to_numeric(
        df["adjusted_event_total"], errors='coerce')

    if progress_text:
        progress_text.text(
            "Adjusted points calculation complete.")  # Final update

    return df
