import time
import requests
import concurrent.futures
import streamlit as st
import pandas as pd  # Added missing import

# Create a session object for reuse in this module
session = requests.Session()

# --- In-memory cache for manager history data ---
# This allows single-GW requests to reuse data fetched during multi-GW analysis


@st.cache_data(show_spinner=False, ttl=300)  # Cache for 5 minutes
def get_manager_history(manager_id, _session):
    """Fetch and cache manager's complete history data."""
    url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/history/"
    try:
        response = _session.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error fetching history for {manager_id}: {e}")
    return None


@st.cache_data(show_spinner=False, ttl=300)  # Cache for 5 minutes
def get_manager_transfers(manager_id, _session):
    """Fetch and cache manager's complete transfer history."""
    url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/transfers/"
    try:
        response = _session.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error fetching transfers for {manager_id}: {e}")
    return []


@st.cache_data(show_spinner=False, ttl=300)  # Cache for 5 minutes
def get_manager_picks(manager_id, gw, _session):
    """Fetch and cache manager's picks for a specific gameweek."""
    url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{gw}/picks/"
    try:
        response = _session.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error fetching picks for {manager_id} GW {gw}: {e}")
    return None


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

    # Sum points for all matches in the gameweek (handles double gameweeks)
    total_points = 0
    for record in player_data:
        if record.get("round") == gw:
            total_points += record.get("total_points", 0)

    return total_points  # Return sum of all points in the gameweek


# --- Calculate Transfer Points Gain/Loss ---
def get_transfer_points_difference(manager_id, gw, _session):
    """Calculates the net points gained/lost from transfers in a specific gameweek.
    Uses cached transfer data for better performance.
    """
    # Use cached transfers fetch
    transfers_data = get_manager_transfers(manager_id, _session)

    if not transfers_data:
        return 0

    # Filter transfers made for this specific gameweek
    gw_transfers = [t for t in transfers_data if t.get("event") == gw]

    if not gw_transfers:
        return 0  # No transfers made in this gameweek

    # Calculate points for transferred-in players
    points_in = 0
    for transfer in gw_transfers:
        player_in_id = transfer.get("element_in")
        if player_in_id:
            points_in += get_player_points(player_in_id, gw, _session)

    # Calculate points for transferred-out players (what they would have scored)
    points_out = 0
    for transfer in gw_transfers:
        player_out_id = transfer.get("element_out")
        if player_out_id:
            points_out += get_player_points(player_out_id, gw, _session)

    # Net gain/loss from transfers
    transfer_difference = points_in - points_out

    return transfer_difference


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
    elif active_chip == "manager":
        # Handle manager chip - find player in position 16 (the manager position)
        for pick in picks:
            if pick.get("position") == 16:
                # Get the manager's points
                chip_point_effect += get_player_points(
                    pick['element'], gw, _session)
                break  # Only one manager

    # Transfer-adjusted points = Raw Points - Transfer Costs only (no chip deduction)
    transfer_adjusted_points = points - transfers_cost

    # Net points = Raw Points - Transfer Costs - Chip Point Effect
    net_points = points - transfers_cost - chip_point_effect

    return {
        'net_points': net_points,
        'transfer_adjusted_points': transfer_adjusted_points,
        'raw_points': points,
        'transfer_cost': transfers_cost,
        'chip_effect': chip_point_effect
    }


# --- Process a Single Manager Row ---
def process_manager(row, gw, _session):  # Accept session
    """Fetches and calculates adjusted points for a single manager row using the provided session."""
    manager_id = row["manager_id"]
    original_gw_points = row["gw_points"]  # Keep original as fallback
    captain_id = row.get("captain_id")  # Get captain ID from DataFrame

    # Pass session to the function that makes the API call
    points_data = get_manager_gw_points(manager_id, gw, _session)

    # Calculate transfer points difference
    transfer_gain = get_transfer_points_difference(manager_id, gw, _session)

    # Calculate captain points using cached get_player_points (no extra API call)
    captain_points = 0
    if captain_id:
        captain_points = get_player_points(captain_id, gw, _session)

    # If fetching/calculation failed, return fallback values
    if points_data is not None:
        points_data['transfer_gain'] = transfer_gain
        points_data['captain_points'] = captain_points
        return points_data
    else:
        return {
            'net_points': original_gw_points,
            'transfer_adjusted_points': original_gw_points,
            'raw_points': original_gw_points,
            'transfer_cost': 0,
            'chip_effect': 0,
            'transfer_gain': transfer_gain,
            'captain_points': captain_points
        }


# --- Calculate Adjusted Points for the Entire DataFrame ---
def calculate_adjusted_points_for_players(df, gw, _session, progress_text=None, max_workers=20, batch_size=50):
    """Updates DataFrame by calculating net points (points after deducting chip effects and transfer penalties) with parallelization."""
    all_results = []
    total_managers = len(df)
    results_dict = {}  # Use dict to store results by index

    if progress_text:
        progress_text.text(
            f"Calculating net points for {total_managers} managers...")

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
                manager_id = df.loc[original_index, 'manager_id']
                print(f"Manager ID {manager_id} generated an exception: {exc}")
                # Fallback to original event total or NaN
                results_dict[original_index] = df.loc[original_index,
                                                      'gw_points']  # Or pd.NA

            # Update progress text periodically
            if progress_text and (i + 1) % 10 == 0:  # Update every 10 managers
                progress_text.text(
                    f"Calculated net points for {i+1}/{total_managers} managers...")

    # Reconstruct results in the original order using the index
    all_results = [results_dict[index] for index in df.index]

    # Extract data for each column
    df["net_points"] = [result['net_points'] for result in all_results]
    df["transfer_adjusted_points"] = [result['transfer_adjusted_points']
                                      for result in all_results]
    df["transfer_cost"] = [result['transfer_cost'] for result in all_results]
    df["chip_effect"] = [result['chip_effect'] for result in all_results]
    df["transfer_gain"] = [result['transfer_gain'] for result in all_results]
    df["captain_points"] = [result['captain_points'] for result in all_results]

    # Ensure the columns are numeric, coercing errors to NaN
    df["net_points"] = pd.to_numeric(df["net_points"], errors='coerce')
    df["transfer_adjusted_points"] = pd.to_numeric(
        df["transfer_adjusted_points"], errors='coerce')
    df["transfer_cost"] = pd.to_numeric(df["transfer_cost"], errors='coerce')
    df["chip_effect"] = pd.to_numeric(df["chip_effect"], errors='coerce')
    df["transfer_gain"] = pd.to_numeric(df["transfer_gain"], errors='coerce')
    df["captain_points"] = pd.to_numeric(df["captain_points"], errors='coerce')

    if progress_text:
        progress_text.text(
            "Points calculation complete.")  # Final update

    return df


# --- Calculate Multi-Gameweek Aggregated Data (Optimized) ---
def get_multi_gw_manager_data(manager_id, gameweeks, _session, calculate_transfer_gain=False):
    """Fetches and aggregates manager data across multiple gameweeks - optimized version.

    Uses cached history endpoint which is much faster than per-GW API calls.
    Transfer gain calculation is optional as it requires additional API calls per player.
    """
    total_gw_points = 0
    total_transfer_cost = 0
    total_captain_points = 0
    total_points_on_bench = 0
    chips_used = []

    # Use cached history fetch (1 API call, cached for 5 min)
    history_data = get_manager_history(manager_id, _session)

    if history_data:
        current_history = {
            h['event']: h for h in history_data.get('current', [])}
        chips_history = history_data.get('chips', [])

        # Process each gameweek from cached history (no additional API calls)
        for gw in gameweeks:
            if gw in current_history:
                gw_data = current_history[gw]
                total_gw_points += gw_data.get('points', 0)
                total_transfer_cost += gw_data.get('event_transfers_cost', 0)

            # Get captain points and bench points for this GW using cached picks data
            picks_data = get_manager_picks(manager_id, gw, _session)
            if picks_data:
                picks = picks_data.get('picks', [])
                # Get captain points
                for pick in picks:
                    if pick.get('is_captain', False):
                        captain_id = pick.get('element')
                        if captain_id:
                            total_captain_points += get_player_points(
                                captain_id, gw, _session)
                        break

                # Get bench points from entry_history
                entry_history = picks_data.get('entry_history', {})
                total_points_on_bench += entry_history.get(
                    'points_on_bench', 0)

        # Get chips used in the gameweek range
        for chip in chips_history:
            chip_event = chip.get('event')
            if chip_event in gameweeks:
                chip_name = chip.get('name', 'unknown')
                chips_used.append(f"{chip_name}(GW{chip_event})")
    else:
        # Fallback if history fetch failed
        for gw in gameweeks:
            points_data = get_manager_gw_points(manager_id, gw, _session)
            if points_data:
                total_gw_points += points_data.get('raw_points', 0)
                total_transfer_cost += points_data.get('transfer_cost', 0)

    # Transfer gain calculation (OPTIONAL - significantly slower due to player API calls)
    total_transfer_gain = 0
    if calculate_transfer_gain:
        # Use cached transfers fetch
        all_transfers = get_manager_transfers(manager_id, _session)
        if all_transfers:
            for gw in gameweeks:
                gw_transfers = [
                    t for t in all_transfers if t.get("event") == gw]
                for transfer in gw_transfers:
                    player_in_id = transfer.get("element_in")
                    player_out_id = transfer.get("element_out")
                    if player_in_id:
                        total_transfer_gain += get_player_points(
                            player_in_id, gw, _session)
                    if player_out_id:
                        total_transfer_gain -= get_player_points(
                            player_out_id, gw, _session)

    # Calculate aggregated values
    net_points = total_gw_points - total_transfer_cost
    transfer_adjusted_points = total_gw_points - total_transfer_cost

    return {
        'gw_points': total_gw_points,
        'net_points': net_points,
        'transfer_adjusted_points': transfer_adjusted_points,
        'transfer_cost': total_transfer_cost,
        'chip_effect': 0,
        'transfer_gain': total_transfer_gain,
        'captain_points': total_captain_points,
        'points_on_bench': total_points_on_bench,
        'chips_used': ', '.join(chips_used) if chips_used else None
    }


def process_manager_multi_gw(row, gameweeks, _session, calculate_transfer_gain=False):
    """Process a single manager for multi-gameweek analysis."""
    manager_id = row["manager_id"]
    return get_multi_gw_manager_data(manager_id, gameweeks, _session, calculate_transfer_gain)


def calculate_multi_gw_points(df, start_gw, end_gw, _session, progress_text=None, max_workers=20, calculate_transfer_gain=False):
    """Calculate aggregated points across multiple gameweeks for all managers in DataFrame.

    Args:
        calculate_transfer_gain: If False (default), skips transfer gain calculation for speed.
                                 Set to True if you need transfer gain data (slower).
    """
    gameweeks = list(range(start_gw, end_gw + 1))
    total_managers = len(df)
    results_dict = {}

    mode_text = "(with transfer gain)" if calculate_transfer_gain else "(fast mode)"
    if progress_text:
        progress_text.text(
            f"Calculating points for GW {start_gw}-{end_gw} {mode_text}...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {
            executor.submit(process_manager_multi_gw, row, gameweeks, _session, calculate_transfer_gain): index
            for index, row in df.iterrows()
        }

        for i, future in enumerate(concurrent.futures.as_completed(future_to_index)):
            original_index = future_to_index[future]
            try:
                result = future.result()
                results_dict[original_index] = result
            except Exception as exc:
                manager_id = df.loc[original_index, 'manager_id']
                print(f"Manager ID {manager_id} generated an exception: {exc}")
                results_dict[original_index] = {
                    'gw_points': 0,
                    'net_points': 0,
                    'transfer_adjusted_points': 0,
                    'transfer_cost': 0,
                    'chip_effect': 0,
                    'transfer_gain': 0,
                    'captain_points': 0,
                    'points_on_bench': 0,
                    'chips_used': None
                }

            if progress_text and (i + 1) % 10 == 0:
                progress_text.text(
                    f"Processed {i+1}/{total_managers} managers for multi-GW analysis...")

    # Reconstruct results in original order
    all_results = [results_dict[index] for index in df.index]

    # Update DataFrame with aggregated columns
    df["gw_points"] = [result['gw_points'] for result in all_results]
    df["net_points"] = [result['net_points'] for result in all_results]
    df["transfer_adjusted_points"] = [result['transfer_adjusted_points']
                                      for result in all_results]
    df["transfer_cost"] = [result['transfer_cost'] for result in all_results]
    df["chip_effect"] = [result['chip_effect'] for result in all_results]
    df["transfer_gain"] = [result['transfer_gain'] for result in all_results]
    df["captain_points"] = [result['captain_points'] for result in all_results]
    df["points_on_bench"] = [result['points_on_bench']
                             for result in all_results]
    df["chips_used"] = [result['chips_used'] for result in all_results]

    # Ensure numeric columns
    for col in ["gw_points", "net_points", "transfer_adjusted_points", "transfer_cost", "chip_effect", "transfer_gain", "captain_points", "points_on_bench"]:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    if progress_text:
        progress_text.text(
            f"Multi-GW analysis complete (GW {start_gw}-{end_gw}).")

    return df
