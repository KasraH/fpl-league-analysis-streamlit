import requests
import pandas as pd
from tqdm import tqdm  # Keep if you want terminal progress for long fetches
import concurrent.futures
import time  # Keep for potential rate limiting if needed

# --- Create a shared session object for reuse across the app ---
session = requests.Session()
# Optional: Add headers if needed (e.g., User-Agent)
session.headers.update({'User-Agent': 'FPL League Analysis App'})

# --- Global cache for overall rank (in-memory for a single run) ---
# Consider using Streamlit's caching for persistence across reruns if needed,
# but be mindful of stale data. In-memory is fine for a single calculation.
overall_rank_cache = {}


def get_overall_rank(entry):
    """
    Fetch the overall rank (summary_overall_rank) for the given entry.
    Uses the shared session and in-memory cache.
    """
    # Check cache first
    if entry in overall_rank_cache:
        return overall_rank_cache[entry]

    url = f"https://fantasy.premierleague.com/api/entry/{entry}/"
    try:
        # Use the shared session
        response = session.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            overall_rank = data.get("summary_overall_rank")
            overall_rank_cache[entry] = overall_rank  # Cache the result
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


def fetch_league_page(league_id, page):
    """
    Fetch a single page of league standings using the shared session.
    """
    url = f"https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/"
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


def process_page_data(page_data, max_workers=10):
    """
    Process data from a single page: extract player info and fetch overall ranks in parallel.
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

    # --- Fetch overall ranks in parallel ---
    entry_ids = [player["entry"] for player in results]
    overall_ranks = {}  # Dictionary to store results: {entry_id: rank}

    # Use ThreadPoolExecutor for I/O-bound tasks (API calls)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a mapping of future to entry_id
        future_to_entry = {
            executor.submit(get_overall_rank, entry_id): entry_id
            for entry_id in entry_ids
        }

        for future in concurrent.futures.as_completed(future_to_entry):
            entry_id = future_to_entry[future]
            try:
                rank_result = future.result()
                overall_ranks[entry_id] = rank_result
            except Exception as exc:
                # Log exception during the parallel fetch
                print(
                    f'Entry {entry_id} generated an exception during overall rank fetch: {exc}')
                # Assign None or pd.NA if fetching failed
                overall_ranks[entry_id] = None

    # --- Process player data using fetched ranks ---
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

        # Get the pre-fetched overall rank using .get() for safety
        # Default to None if not found
        overall_rank = overall_ranks.get(entry_id, None)

        player_data = {
            "player_name": player.get("player_name", "N/A"),
            "rank": current_rank,
            "last_rank": last_rank,
            "rank_change": rank_change,
            "pct_rank_change": pct_rank_change,
            "total": player.get("total", 0),
            "entry_name": player.get("entry_name", "N/A"),
            "entry": entry_id,
            "event_total": player.get("event_total", 0),
            "overall_rank": overall_rank  # Add the fetched overall rank
        }
        players_data_list.append(player_data)

    return players_data_list, has_next


def get_league_standings(league_id, max_workers_overall_rank=10, limit=None):
    """
    Fetch league standings and return a DataFrame.
    Uses parallel fetching for overall ranks within each page.

    Args:
        league_id: The ID of the league to fetch
        max_workers_overall_rank: Maximum number of workers for parallel overall rank fetching
        limit: Optional limit on number of managers to fetch. If provided, only fetches
              enough pages to get this many managers (50 managers per page)
    """
    all_players = []
    page = 1
    has_next = True
    managers_per_page = 50  # FPL API returns 50 managers per page

    # Calculate how many pages we need if limit is provided
    max_pages = None
    if limit:
        max_pages = (limit + managers_per_page - 1) // managers_per_page

    print(f"Fetching league {league_id} standings...")
    with tqdm(desc="Fetching pages", unit="page") as pbar:
        while has_next:
            page_data = fetch_league_page(league_id, page)

            if page_data is None:
                print(f"Warning: Failed to fetch page {page}. Stopping.")
                has_next = False
                break

            players, current_has_next = process_page_data(
                page_data, max_workers=max_workers_overall_rank)
            all_players.extend(players)
            has_next = current_has_next

            pbar.update(1)
            pbar.set_description(f"Fetched {len(all_players)} players")

            # Stop if we've reached the limit
            if limit and len(all_players) >= limit:
                all_players = all_players[:limit]  # Trim to exact limit
                break

            # Stop if we've fetched all needed pages
            if max_pages and page >= max_pages:
                break

            if not has_next:
                break

            page += 1

    if not all_players:
        print("No players found for this league.")
        return pd.DataFrame()

    # Create DataFrame from the collected list of dictionaries
    df = pd.DataFrame(all_players)

    # Convert types after collecting all data for efficiency
    df["rank"] = pd.to_numeric(df["rank"], errors='coerce').astype("Int64")
    df["last_rank"] = pd.to_numeric(
        df["last_rank"], errors='coerce').astype("Int64")
    df["rank_change"] = pd.to_numeric(
        df["rank_change"], errors='coerce').astype("Int64")
    df["total"] = pd.to_numeric(df["total"], errors='coerce').astype("Int64")
    df["entry"] = pd.to_numeric(df["entry"], errors='coerce').astype("Int64")
    df["event_total"] = pd.to_numeric(
        df["event_total"], errors='coerce').astype("Int64")
    df["overall_rank"] = pd.to_numeric(
        df["overall_rank"], errors='coerce').astype("Int64")
    df["pct_rank_change"] = pd.to_numeric(
        df["pct_rank_change"], errors='coerce').round(2)

    print(f"Total players retrieved and processed: {len(df)}")
    return df
