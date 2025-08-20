import requests
import pandas as pd
from tqdm import tqdm  # Keep if you want terminal progress for long fetches
import concurrent.futures
import time  # Keep for potential rate limiting if needed
import streamlit as st  # For caching

# --- Create a shared session object for reuse across the app ---
session = requests.Session()
# Optional: Add headers if needed (e.g., User-Agent)
session.headers.update({'User-Agent': 'FPL League Analysis App'})

# --- Global cache for overall rank (in-memory for a single run) ---
# Consider using Streamlit's caching for persistence across reruns if needed,
# but be mindful of stale data. In-memory is fine for a single calculation.
player_cache = {}  # Cache for player data


@st.cache_data(show_spinner=False)
def get_bootstrap_static():
    """Fetch and cache bootstrap static data (players, teams, etc.)"""
    try:
        bootstrap_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
        response = session.get(bootstrap_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            # Create a lookup dict for player names by ID
            players_dict = {p["id"]: p["web_name"]
                            for p in data.get("elements", [])}
            return players_dict
        else:
            print(f"Error fetching bootstrap data: {response.status_code}")
            return {}
    except Exception as e:
        print(f"Exception fetching bootstrap data: {e}")
        return {}


@st.cache_data(show_spinner=False)
def get_overall_rank(entry):
    """
    Fetch the overall rank (summary_overall_rank) for the given entry.
    Uses the shared session and Streamlit's caching.
    """
    url = f"https://fantasy.premierleague.com/api/entry/{entry}/"
    try:
        # Use the shared session
        response = session.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            overall_rank = data.get("summary_overall_rank")
            return overall_rank
        elif response.status_code == 404:
            print(f"Info: Entry {entry} not found (404).")
            return None  # Or pd.NA
        else:
            print(
                f"Error: Failed to fetch overall rank for entry {entry}. Status code: {response.status_code}")
            return None  # Return None or pd.NA on error
    except requests.exceptions.RequestException as e:
        print(f"Request error for overall rank entry {entry}: {e}")
        return None  # Return None or pd.NA on error


def get_manager_basic_data(entry_id):
    """Fetch basic manager data including total points and last event points."""
    try:
        url = f"https://fantasy.premierleague.com/api/entry/{entry_id}/"
        response = session.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            return {
                "summary_overall_points": data.get("summary_overall_points", 0),
                "summary_event_points": data.get("summary_event_points", 0)
            }
        else:
            print(f"Failed to fetch manager data for entry {entry_id}")
            return {"summary_overall_points": 0, "summary_event_points": 0}
    except Exception as e:
        print(f"Error fetching manager data for entry {entry_id}: {e}")
        return {"summary_overall_points": 0, "summary_event_points": 0}


def fetch_league_page(league_id, page, use_new_entries_pagination=False):
    """
    Fetch a single page of league standings with proper pagination support.
    """
    url = f"https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/"

    # Use appropriate pagination parameter based on league format
    if use_new_entries_pagination:
        params = {"page_new_entries": page}
    else:
        params = {"page_standings": page}

    try:
        # Use the shared session object for the request
        response = session.get(url, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(
                f"Error: Failed to fetch league data page {page}. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request error on league page {page}: {e}")
        return None


def process_page_data(page_data, current_gw=None, max_workers=10):
    """
    Process data from a single page: extract player info and fetch overall ranks in parallel.

    Args:
        page_data: The JSON data for a single page of league standings
        current_gw: Current gameweek number (needed for rank changes)
        max_workers: Max workers for parallel processing
    """
    players_data_list = []
    if not page_data or "standings" not in page_data:
        print("Warning: Invalid or empty page data received.")
        return players_data_list, False  # Return empty list and has_next=False

    standings = page_data.get("standings", {})
    results = standings.get("results", [])
    has_next = standings.get("has_next", False)

    if not results:
        # It's possible to have standings but no results on an empty page
        return players_data_list, has_next

    # --- Fetch manager data in parallel (history contains overall ranks) ---
    entry_ids = [player["entry"] for player in results]
    manager_data = {}  # Dictionary to store results: {entry_id: data}

    # Use ThreadPoolExecutor for I/O-bound tasks (API calls)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create futures that fetch both overall rank and history in one call
        future_to_entry = {}

        # Always fetch history data to get captain and chip information
        if current_gw:
            future_to_entry = {
                executor.submit(get_manager_history, entry_id, current_gw): entry_id
                for entry_id in entry_ids
            }
        else:
            # If no current_gw provided, just get basic overall rank
            future_to_entry = {
                executor.submit(get_overall_rank, entry_id): entry_id
                for entry_id in entry_ids
            }

        for future in concurrent.futures.as_completed(future_to_entry):
            entry_id = future_to_entry[future]
            try:
                data_result = future.result()
                if data_result:
                    manager_data[entry_id] = data_result
            except Exception as exc:
                # Log exception during the parallel fetch
                print(
                    f'Entry {entry_id} generated an exception during data fetch: {exc}')
                # Assign None if fetching failed
                manager_data[entry_id] = None

    # --- Process player data using fetched data ---
    for player in results:
        entry_id = player.get("entry")
        last_rank = player.get("last_rank", 0)
        current_rank = player.get("rank", 0)

        # Calculate rank change
        rank_change = None if last_rank == 0 else last_rank - current_rank

        # Calculate percentage rank change (avoid division by zero)
        pct_rank_change = None
        if last_rank > 0:
            pct_rank_change = ((last_rank - current_rank) / last_rank) * 100

        # Initialize overall rank data
        overall_rank = None
        prev_overall_rank = None
        overall_rank_change = None
        overall_rank_change_pct = None
        chip_used = None
        transfer_cost = 0
        captain_name = None
        vice_captain_name = None
        points_on_bench = 0

        # Get manager data from our fetched results
        manager_info = manager_data.get(entry_id)

        # Process data based on what was fetched
        if manager_info:
            if isinstance(manager_info, dict) and "current" in manager_info and "previous" in manager_info:
                # We have history data with current and previous GW info
                if manager_info["current"]:
                    overall_rank = manager_info["current"].get("overall_rank")
                if manager_info["previous"]:
                    prev_overall_rank = manager_info["previous"].get(
                        "overall_rank")

                # Calculate overall rank changes if we have both values
                if overall_rank is not None and prev_overall_rank is not None:
                    overall_rank_change = prev_overall_rank - overall_rank
                    if prev_overall_rank > 0:  # Avoid division by zero
                        overall_rank_change_pct = (
                            overall_rank_change / prev_overall_rank) * 100

                # Extract chip used and transfer cost information
                chip_used = manager_info.get("chip_used")
                transfer_cost = manager_info.get("transfer_cost", 0)

                # Extract captain and vice captain names
                captain_name = manager_info.get("captain_name")
                vice_captain_name = manager_info.get("vice_captain_name")

                # Extract points on bench
                points_on_bench = manager_info.get("points_on_bench", 0)
            else:
                # We only have basic overall rank
                overall_rank = manager_info

        player_data = {
            "manager_name": player.get("player_name", "N/A"),
            "rank": current_rank,
            "last_rank": last_rank,
            "rank_change": rank_change,
            "pct_rank_change": pct_rank_change,
            "total": player.get("total", 0),
            "team_name": player.get("entry_name", "N/A"),
            "manager_id": entry_id,
            "gw_points": player.get("event_total", 0),
            "overall_rank": overall_rank,  # Add the fetched overall rank
            "chip_used": chip_used,        # Add chip used information
            "transfer_penalty": transfer_cost,  # Add transfer cost information
            "captain_name": captain_name,  # Add captain name
            "vice_captain_name": vice_captain_name,  # Add vice captain name
            "points_on_bench": points_on_bench  # Add points on bench
        }

        # Add overall rank change data if available
        if prev_overall_rank is not None:
            player_data["prev_overall_rank"] = prev_overall_rank
        if overall_rank_change is not None:
            player_data["overall_rank_change"] = overall_rank_change
        if overall_rank_change_pct is not None:
            player_data["overall_rank_change_pct"] = overall_rank_change_pct

        players_data_list.append(player_data)

    return players_data_list, has_next


@st.cache_data(show_spinner=False)
def get_manager_history(entry, current_gw):
    """
    Fetch manager's gameweek history including overall rank for current and previous GWs.
    Used to calculate overall rank change.
    """
    # Check cache first (we could add this for optimization)
    url = f"https://fantasy.premierleague.com/api/entry/{entry}/history/"
    try:
        # Use the shared session
        response = session.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            current_data = data.get("current", [])

            # Get current GW's overall rank
            current_gw_data = None
            prev_gw_data = None

            # Find current GW data
            for gw_data in current_data:
                if gw_data.get("event") == current_gw:
                    current_gw_data = gw_data
                elif gw_data.get("event") == current_gw - 1:
                    prev_gw_data = gw_data

            # Get chip info from picks API
            chip_used = None
            transfer_cost = 0
            captain_id = None
            vice_captain_id = None
            captain_name = None
            vice_captain_name = None
            points_on_bench = 0

            if current_gw_data:
                transfer_cost = current_gw_data.get("event_transfers_cost", 0)

                # Get chip and captain information from picks endpoint
                picks_url = f"https://fantasy.premierleague.com/api/entry/{entry}/event/{current_gw}/picks/"
                try:
                    picks_response = session.get(picks_url, timeout=5)
                    if picks_response.status_code == 200:
                        picks_data = picks_response.json()
                        chip_used = picks_data.get("active_chip")

                        # Extract points on bench from entry_history
                        entry_history = picks_data.get("entry_history", {})
                        points_on_bench = entry_history.get(
                            "points_on_bench", 0)

                        # Extract captain and vice captain information
                        picks = picks_data.get("picks", [])
                        for pick in picks:
                            if pick.get("is_captain", False):
                                captain_id = pick.get("element")
                            if pick.get("is_vice_captain", False):
                                vice_captain_id = pick.get("element")

                        # Get player names if IDs are found
                        if captain_id or vice_captain_id:
                            try:
                                players_dict = get_bootstrap_static()

                                if captain_id and captain_id in players_dict:
                                    captain_name = players_dict[captain_id]
                                if vice_captain_id and vice_captain_id in players_dict:
                                    vice_captain_name = players_dict[vice_captain_id]
                            except:
                                # Continue without player names if bootstrap fetch fails
                                pass
                except:
                    # If picks request fails, continue without chip data
                    pass

            # Return both current and previous GW data
            return {
                "current": current_gw_data,
                "previous": prev_gw_data,
                "chip_used": chip_used,
                "transfer_cost": transfer_cost,
                "captain_id": captain_id,
                "vice_captain_id": vice_captain_id,
                "captain_name": captain_name,
                "vice_captain_name": vice_captain_name,
                "points_on_bench": points_on_bench
            }
        else:
            print(
                f"Error: Failed to fetch history for entry {entry}. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Request error for history data entry {entry}: {e}")
        return None


def calculate_overall_rank_changes(players_data, current_gw):
    """
    Calculate overall rank changes for each player by fetching history data.

    Args:
        players_data (list): List of player data dictionaries
        current_gw (int): The current gameweek being analyzed

    Returns:
        dict: Dictionary mapping entry IDs to their overall rank changes
    """
    rank_changes = {}

    # Use ThreadPoolExecutor for parallel fetching
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        # Create a mapping of future to entry_id
        future_to_entry = {
            executor.submit(get_manager_history, player["entry"], current_gw): player["entry"]
            for player in players_data
        }

        for future in concurrent.futures.as_completed(future_to_entry):
            entry_id = future_to_entry[future]
            try:
                history_data = future.result()
                if history_data and history_data["current"] and history_data["previous"]:
                    current_rank = history_data["current"].get("overall_rank")
                    prev_rank = history_data["previous"].get("overall_rank")

                    if current_rank is not None and prev_rank is not None:
                        # Calculate rank change (positive means improvement)
                        rank_change = prev_rank - current_rank

                        # Calculate percentage change (avoid division by zero)
                        if prev_rank > 0:
                            pct_change = (rank_change / prev_rank) * 100
                        else:
                            pct_change = None

                        rank_changes[entry_id] = {
                            "current_rank": current_rank,
                            "prev_rank": prev_rank,
                            "rank_change": rank_change,
                            "pct_change": pct_change
                        }
            except Exception as exc:
                print(
                    f'Error calculating rank change for entry {entry_id}: {exc}')

    return rank_changes


def get_league_standings(league_id, current_gw=None, max_workers_overall_rank=10, limit=None, progress_text=None):
    """
    Enhanced function to fetch from both standings and new_entries.
    Uses parallel fetching for overall ranks and history data within each page.

    Args:
        league_id: The ID of the league to fetch
        current_gw: Current gameweek (needed for overall rank change calculations)
        max_workers_overall_rank: Maximum number of workers for parallel data fetching
        limit: Optional limit on number of managers to fetch
        progress_text: Optional streamlit text element for progress updates
    """
    all_players = []

    print(f"Fetching league {league_id} standings...")

    # Phase 1: Fetch all pages from standings
    standings_players = []
    page = 1
    has_next_standings = True

    print("Fetching from standings...")
    with tqdm(desc="Standings pages", unit="page") as pbar:
        while has_next_standings:
            if progress_text:
                progress_text.text(
                    f"Fetching standings data... {len(standings_players)} managers retrieved")

            page_data = fetch_league_page(
                league_id, page, use_new_entries_pagination=False)
            if not page_data:
                break

            standings = page_data.get("standings", {})
            has_next_standings = standings.get("has_next", False)

            # Process standings results using existing parallel processing
            players_page_data, _ = process_page_data(
                page_data, current_gw, max_workers=max_workers_overall_rank)
            standings_players.extend(players_page_data)

            pbar.update(1)
            page += 1
            time.sleep(0.1)

    # Phase 2: Fetch all pages from new_entries
    new_entries_players = []
    page = 1
    has_next_new_entries = True

    print("Fetching from new_entries...")
    with tqdm(desc="New entries pages", unit="page") as pbar:
        while has_next_new_entries:
            if progress_text:
                progress_text.text(
                    f"Fetching new entries... {len(new_entries_players)} managers retrieved")

            page_data = fetch_league_page(
                league_id, page, use_new_entries_pagination=True)
            if not page_data:
                break

            new_entries = page_data.get("new_entries", {})
            new_entries_results = new_entries.get("results", [])
            has_next_new_entries = new_entries.get("has_next", False)

            # Process new_entries results with parallel processing
            entry_ids = [entry["entry"] for entry in new_entries_results]

            # Use parallel processing to fetch manager data
            manager_data_dict = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers_overall_rank) as executor:
                # Submit tasks for manager basic data
                future_to_entry = {
                    executor.submit(get_manager_basic_data, entry_id): entry_id
                    for entry_id in entry_ids
                }

                # Collect results
                for future in concurrent.futures.as_completed(future_to_entry):
                    entry_id = future_to_entry[future]
                    try:
                        manager_data_dict[entry_id] = future.result()
                    except Exception as exc:
                        print(
                            f'Entry {entry_id} basic data fetch failed: {exc}')
                        manager_data_dict[entry_id] = {
                            "summary_overall_points": 0, "summary_event_points": 0}

            # Get history data in parallel if current_gw is provided
            history_data_dict = {}
            if current_gw:
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers_overall_rank) as executor:
                    future_to_entry = {
                        executor.submit(get_manager_history, entry_id, current_gw): entry_id
                        for entry_id in entry_ids
                    }

                    for future in concurrent.futures.as_completed(future_to_entry):
                        entry_id = future_to_entry[future]
                        try:
                            history_data_dict[entry_id] = future.result()
                        except Exception as exc:
                            print(
                                f'Entry {entry_id} history data fetch failed: {exc}')
                            history_data_dict[entry_id] = None

            # Process each entry using the fetched data
            for entry in new_entries_results:
                entry_id = entry["entry"]

                # Get manager data from parallel fetch
                manager_data = manager_data_dict.get(
                    entry_id, {"summary_overall_points": 0, "summary_event_points": 0})

                # Initialize data
                chip_used = None
                transfer_cost = 0
                captain_name = None
                vice_captain_name = None
                overall_rank = None
                points_on_bench = 0

                # Get history data if available
                if current_gw and entry_id in history_data_dict:
                    manager_history = history_data_dict[entry_id]
                    if manager_history:
                        chip_used = manager_history.get("chip_used")
                        transfer_cost = manager_history.get("transfer_cost", 0)
                        captain_name = manager_history.get("captain_name")
                        vice_captain_name = manager_history.get(
                            "vice_captain_name")
                        points_on_bench = manager_history.get(
                            "points_on_bench", 0)

                        # Get overall rank data
                        current_data = manager_history.get("current")
                        overall_rank = current_data.get(
                            "overall_rank") if current_data else None

                player_data = {
                    "manager_name": f"{entry['player_first_name']} {entry['player_last_name']}",
                    "rank": None,  # No ranking yet for new entries
                    "last_rank": 0,
                    "rank_change": None,
                    "pct_rank_change": None,
                    # ✅ Get actual total points
                    "total": manager_data["summary_overall_points"],
                    "team_name": entry["entry_name"],
                    "manager_id": entry_id,
                    # ✅ Get actual event points
                    "gw_points": manager_data["summary_event_points"],
                    "overall_rank": overall_rank,
                    "overall_rank_change": None,
                    "overall_rank_change_pct": None,
                    "chip_used": chip_used,
                    "transfer_penalty": transfer_cost,
                    "captain_name": captain_name,
                    "vice_captain_name": vice_captain_name,
                    "points_on_bench": points_on_bench
                }
                new_entries_players.append(player_data)

            pbar.update(1)
            page += 1
            time.sleep(0.1)

    # Combine all players
    all_players = standings_players + new_entries_players

    print(f"Found {len(standings_players)} players in standings")
    print(f"Found {len(new_entries_players)} players in new_entries")

    if not all_players:
        print("No players found for this league.")
        return pd.DataFrame()

    if progress_text:
        progress_text.text(
            f"Processing data for {len(all_players)} managers...")

    # Apply limit if specified
    if limit and len(all_players) > limit:
        all_players = all_players[:limit]

    # Create DataFrame from all collected players
    df = pd.DataFrame(all_players)

    # Convert types after collecting all data for efficiency
    df["rank"] = pd.to_numeric(df["rank"], errors='coerce').astype("Int64")
    df["last_rank"] = pd.to_numeric(
        df["last_rank"], errors='coerce').astype("Int64")
    df["rank_change"] = pd.to_numeric(
        df["rank_change"], errors='coerce').astype("Int64")
    df["total"] = pd.to_numeric(df["total"], errors='coerce').astype("Int64")
    df["manager_id"] = pd.to_numeric(
        df["manager_id"], errors='coerce').astype("Int64")
    df["gw_points"] = pd.to_numeric(
        df["gw_points"], errors='coerce').astype("Int64")
    df["overall_rank"] = pd.to_numeric(
        df["overall_rank"], errors='coerce').astype("Int64")
    df["pct_rank_change"] = pd.to_numeric(
        df["pct_rank_change"], errors='coerce').round(2)

    # Convert overall rank change columns if they exist
    if "prev_overall_rank" in df.columns:
        df["prev_overall_rank"] = pd.to_numeric(
            df["prev_overall_rank"], errors='coerce').astype("Int64")
    if "overall_rank_change" in df.columns:
        df["overall_rank_change"] = pd.to_numeric(
            df["overall_rank_change"], errors='coerce').astype("Int64")
    if "overall_rank_change_pct" in df.columns:
        df["overall_rank_change_pct"] = pd.to_numeric(
            df["overall_rank_change_pct"], errors='coerce').round(2)

    # Convert new chip and transfer cost columns if they exist
    if "transfer_penalty" in df.columns:
        df["transfer_penalty"] = pd.to_numeric(
            df["transfer_penalty"], errors='coerce').astype("Int64")

    # Convert points on bench column if it exists
    if "points_on_bench" in df.columns:
        df["points_on_bench"] = pd.to_numeric(
            df["points_on_bench"], errors='coerce').astype("Int64")

    # If no ranks exist, assign them based on total points for new leagues
    if df["rank"].isna().all():
        # Sort by total points (highest first) and assign ranks
        df = df.sort_values("total", ascending=False).reset_index(drop=True)
        df["rank"] = range(1, len(df) + 1)

    print(f"Total players retrieved and processed: {len(df)}")
    return df
