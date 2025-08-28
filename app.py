import streamlit as st
import pandas as pd
# Import session from fpl_api and the league fetching function
from utils.fpl_api import get_league_standings, session
from utils.calculations import calculate_adjusted_points_for_players
# Import the new analysis function
from utils.top_n_analysis import analyze_top_n_managers

# Sub-league configuration for specific league 2246
SUB_LEAGUES_2246 = {
    "Premier League": [5050980, 8590, 7411, 1675130, 286512, 168631, 476344, 27408, 65456, 380654, 1150237, 6847475, 26387, 382027, 1522561, 1791917, 19389, 260068, 1509543, 13764, 3271367, 3260, 31616, 61450, 91378, 4884, 16269, 251332, 24972, 131106, 6469],
    "Championship": [2016757, 10891, 393811, 678161, 4128, 434367, 13488, 18080, 1174782, 157958, 25165, 276736, 5211, 10851, 17258, 10253, 58600, 1195578, 383674, 9749, 5316708, 2767081, 4620970, 1548, 7236, 15100, 20430, 65441, 445801, 2188, 15922, 2049, 116664, 1412485, 237731, 5344, 28790, 155795, 13750, 756688, 1221578, 10294, 161384, 319885, 22742, 13539, 9799, 15920, 2898, 5693, 476277, 46807, 11102, 288626, 403588, 33499, 9507],
    "League One": [35441, 256201, 3126, 163183, 9216, 670694, 4878918, 421595, 74279, 208669, 408504, 468291, 5126, 11376, 944488, 94630, 100271, 141633, 4639438, 14243, 15254, 330432, 109528, 1237425, 7453, 46847, 5566, 265813, 20051, 113059, 565657, 151118, 16205, 57613, 16768, 9601, 792874, 2010351, 14285, 10335, 125174, 666736, 888415, 20699, 395195, 79499],
    "League Two": [6861293, 4070060, 13554, 447144, 65176, 376665, 14369, 361276, 276477, 85085, 1236368, 17812, 101782, 298858, 4347, 311875, 65991, 570437, 14589, 65963, 362557, 997, 29344, 68452, 2586908, 33636, 6306961, 461788, 1212, 11476, 777908, 18719, 97954, 559574, 1170, 16053, 51725, 4210, 15989, 7977, 49434, 1558072, 235129, 22860, 4703, 201397, 32247, 386672, 4872, 683048, 298175, 391367, 16671, 43917, 68211, 645094, 24459, 33371, 50873, 25099, 6199],
    "National League": [3131, 7663, 221582, 1211665, 318548, 3204, 204642, 4762, 13159, 171403, 319156, 520440, 22348, 3734338, 2553285, 73643, 10700, 37336, 60934, 46365, 3176, 4809, 53034, 1254869, 58623, 1778814, 62106, 539806, 2002715, 159588, 49602, 1170054, 97886, 128544, 3565986, 222847, 1274782, 8697, 17795, 3866025, 231476, 1627595, 3163338, 3464492, 12919, 15347, 372714, 3557420, 9942, 12443, 19876, 9402, 251361, 908068, 2716, 52537, 52541, 362924, 497597, 3332733, 1836763, 7348, 45740, 125431, 355961, 7643254, 11562, 1884595, 7555, 12450, 55428, 83372, 1058442, 3613054, 243945, 27808, 31455, 290536, 1155111, 3310508, 18091, 141089, 23078, 2887629, 48067, 2851950, 3149877, 12311, 22532, 1382618, 1437245, 2208878, 2326697, 1186, 1046512, 15144, 23652, 337301, 1907093, 42001, 88941, 1960713, 109954, 1076691, 1603824, 2112252, 10635, 4612574, 4706854, 6010272, 6827879, 1448170, 99356, 907514, 20741, 40380, 89566, 711677, 1421082, 8215, 21011, 23748, 32104, 68712, 173846, 205850, 206281, 436, 70648, 914866, 3578266, 9586, 60380, 85011, 233028, 2973882, 4197679, 2009, 6872, 1127009, 4410, 50762, 54599, 1802207, 4340303, 28941, 84552, 100814, 426522, 4546877, 39354, 152587, 340423, 1504330, 215837, 5008, 8934, 52055, 57137, 141390, 603624, 1703396, 3627, 28537, 45866, 2012819, 3379475, 8414147, 5740, 17984, 85388, 90285, 1167658, 6356117, 545266, 828905, 1084497, 1527644, 13253, 13869, 18351, 27839, 103175, 255602, 2894521, 66559, 117127, 306162, 949628, 2667437, 16051, 19378, 150529, 200860, 439767, 510841, 736204, 1035361, 1610937, 1860194, 2508271, 27177, 97646, 111004, 469711, 9011, 16181, 74981, 101565, 372354, 2731530, 9409, 44613, 84440, 130951, 99265, 5397, 9317, 146647, 1907881, 3173450, 91652, 781817, 15786, 23844, 24012, 360117, 460985, 1950084, 2132583, 8876766, 727801, 975296, 39347, 98183, 253380, 278096, 306265, 125932, 169689, 413375, 4766, 5528, 19904, 383788, 4476314, 97488, 747882, 12079, 32589, 10975, 675298, 22102, 107794, 129809, 851936, 1506291, 1982034, 6054488, 1063, 8309, 8610, 79438, 156892, 228025, 2672754, 5356166, 3794626, 17441, 9481, 174879, 37341, 37835, 53998, 291237, 261620, 20474, 30064, 31826, 47303, 117517, 3290944, 600387, 30488, 35852, 1081640, 1447016, 4178359, 50581, 50714, 504369, 820110, 7280, 38043, 884110, 26410, 2517516, 3278859, 6051, 15432, 415698, 1117026, 23975, 2757157, 51084, 2059893, 137068, 21451, 30646, 345969, 172433, 15342, 290718, 8460, 700583, 3198431, 25112, 13137, 5480434, 10807, 71330, 1536817, 132548, 1413318, 2654849, 576779, 5498163, 332499, 874140, 1650690, 1975060, 9711, 4619476, 626169, 90770, 194886, 14130, 37694, 1422672, 2524, 759736, 77210],
    "HEX Purgatory": [14001, 748426, 1599540, 52224, 8125104, 8868341, 393962, 331686, 105173, 3976197, 603365, 1893465, 492027, 803167, 959276, 3108602, 5615955, 3230918, 4116, 4601124, 259337, 4766500, 458472, 773785, 9085580, 43255, 545796, 4384239, 604408, 36025, 6582917, 9683, 188607, 1183016, 6130226, 547812, 4588510, 10846, 16060, 4971461, 13002, 1011511, 67018, 3590461, 361287, 298857, 209667, 47142, 82178, 2327072, 2776152, 11279, 8845847, 351545, 581637, 1108816, 2758177, 4920321, 767859, 1205527, 3489943, 5072763, 5704556, 20999, 460552, 2856908, 3301541, 6385094, 281361, 1787203, 3603, 14924, 40816, 152609, 331551, 347446, 402182, 409453, 88704, 3009765, 3638230, 7153057, 50224, 201778, 225866, 800861, 1147873, 8296696, 334057, 683796, 1423435, 11762, 52708, 80616, 123316, 180807, 317220, 14217, 1314066, 7081, 7996, 31329, 114287, 140984, 141007, 288234, 738476, 2164637, 3543783, 8322033, 231718, 6990671, 10898, 2872906, 4262541, 5899724, 141540, 702754, 1216495, 2511944, 2978884, 8298979, 32809, 143700, 158705, 792288, 1717472, 2461868, 114214, 5601239, 6479036, 8942628, 15594, 83086, 313953, 322606, 8849, 192696, 395215, 1403912, 1975287, 8345480, 579586, 5287411, 2517958, 7735, 12948, 13604, 32169, 256982, 326512, 920469, 5057662, 8476236, 118059, 591747, 1179352, 1251644, 1406195, 110486, 117291, 305163, 1227189, 1709202, 3435675, 3655251, 4597959, 6529590, 6649018, 127027, 186233, 196489, 265111, 786577, 1386860, 3063864, 17833, 52955, 2432, 19739, 305867, 7921473, 3474085, 12232, 25045, 114567, 150323, 205100, 258922, 275517, 542451, 3095057, 7529949, 9337637, 1096942, 2331101, 37357, 73715, 395133, 815942, 1407521, 6834883, 202366, 2520047, 4794776, 9585, 15219, 32484, 41047, 46318, 71018, 673753, 977685, 3209135, 4743991, 41846, 88984, 1177156, 3861170, 39404, 62857, 65889, 516866, 909150, 2196910, 35380, 902136, 1046292, 25794, 31887, 68353, 133888, 1367384, 1736752, 7811566, 9156900, 1247996, 2441570, 2603696, 2653400, 6466853, 625824, 1287525, 1917784, 3094774, 3197625, 6313, 267701, 3261692, 7414934, 8280954, 9361308, 22227, 164669, 801396, 1146825, 5780, 10984, 45255, 514029, 1178612, 8060036, 21894, 316844, 3290647, 166379, 1616666, 7105482, 7197079, 4729422, 121548, 17502, 132569, 467854, 803358, 1042958, 2421529, 5484843, 540124, 23406, 38452, 82623, 694581, 876759, 6624022, 5117, 364056, 502375, 584506, 1583039, 2897490, 3575078, 122495, 2847567, 52636, 1018867, 2183758, 3147125, 9173178, 1644260, 3456639, 1110, 111231, 210654, 311454, 1381302, 2307597, 5143790, 7109593, 8726323, 175996, 1741821, 37263, 397978, 445087, 6538868, 1844111, 2324716, 9410154, 1078699, 7891, 33876, 6691988, 7765513, 7689, 118214, 1574634, 284408, 453981, 895695, 186073, 49343, 81967, 2499287, 4567597, 16431, 2950937, 1012789, 1243359, 5010046, 131619, 2587790, 781420, 3293864, 415, 245767, 41390, 48122, 24857, 7206657, 9794, 1833421, 6836709, 7623492, 7405, 1095378, 551836, 5297122, 1476356, 34528, 129628, 138289, 171222, 2580160, 7765262, 54616, 8548118, 221441, 14642, 8446705, 1924619, 6556109, 21983, 449415, 1949705, 2757860, 3281301, 102588, 4367502, 962766, 121039, 2368887, 1972906, 9671, 4169096, 5163631, 4703281, 1780214, 8599438, 1112732, 10654221, 10255633, 10761510, 10645145]
}

st.set_page_config(layout="wide")
st.title("FPL Mini League Analysis")

# --- Sidebar Inputs ---
league_id = st.sidebar.number_input(
    "Enter League ID:", min_value=1, step=1, value=None, placeholder="League ID...")

# Sub-league selector (only for league 2246)
selected_division = None
if league_id == 2246:
    # Add custom CSS to fix cursor for selectbox
    st.markdown("""
    <style>
    .stSelectbox > div > div {
        cursor: pointer !important;
    }
    .stSelectbox > div > div > div {
        cursor: pointer !important;
    }
    [data-testid="stSelectbox"] {
        cursor: pointer !important;
    }
    [data-testid="stSelectbox"] > div {
        cursor: pointer !important;
    }
    [data-testid="stSelectbox"] * {
        cursor: pointer !important;
    }
    </style>
    """, unsafe_allow_html=True)

    selected_division = st.sidebar.selectbox(
        "Select Division:",
        ["All Divisions"] + list(SUB_LEAGUES_2246.keys()),
        help="Select a specific division to analyze or view all divisions together"
    )

current_gw = st.sidebar.number_input(
    "Current Gameweek:", min_value=1, max_value=38, step=1, value=None, placeholder="Gameweek...")
top_n = st.sidebar.number_input(
    "Number of top managers (N) for detailed analysis:",
    min_value=1, value=10, step=1
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


def display_df(title, dataframe, current_gw=None):
    st.markdown(f"##### {title}")
    if dataframe is not None and not dataframe.empty:
        display_dataframe = dataframe.reset_index(drop=True)
        display_dataframe.index = display_dataframe.index + 1

        # Add compact link column if conditions are met
        column_config = {}
        if current_gw and 'team_name' in display_dataframe.columns and 'manager_id' in display_dataframe.columns:
            # Add a compact link column
            display_dataframe['view_team'] = display_dataframe.apply(
                lambda row: f"https://fantasy.premierleague.com/entry/{row['manager_id']}/event/{current_gw}",
                axis=1
            )

            # Reorder columns to put link after team_name
            cols = list(display_dataframe.columns)
            team_name_idx = cols.index('team_name')
            cols.insert(team_name_idx + 1, cols.pop(cols.index('view_team')))
            display_dataframe = display_dataframe[cols]

            # Rename the link column to just an icon
            display_dataframe = display_dataframe.rename(
                columns={'view_team': '🔗'})

            # Configure the link column to be compact
            column_config = {
                "🔗": st.column_config.LinkColumn(
                    "🔗",
                    help="View team on FPL",
                    width="small"
                )
            }

        st.dataframe(display_dataframe, column_config=column_config,
                     use_container_width=True)
    else:
        st.info("No data available for this statistic.")


# --- Helper function to filter dataframe by division ---
def filter_by_division(dataframe, selected_division):
    """Filter dataframe to only include managers from selected division"""
    if selected_division == "All Divisions" or selected_division is None:
        return dataframe

    if selected_division in SUB_LEAGUES_2246:
        manager_ids = SUB_LEAGUES_2246[selected_division]
        filtered_df = dataframe[dataframe['manager_id'].isin(
            manager_ids)].copy()

        # Reset rank based on filtered data (sorted by total points descending)
        filtered_df = filtered_df.sort_values(
            'total', ascending=False).reset_index(drop=True)
        filtered_df['rank'] = range(1, len(filtered_df) + 1)

        return filtered_df

    return dataframe


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

    # Determine specific manager IDs if division filter is applied
    specific_manager_ids = None
    if league_id == 2246 and selected_division and selected_division != "All Divisions":
        specific_manager_ids = SUB_LEAGUES_2246[selected_division]

    try:
        # --- Stage 1: Fetch League Standings ---
        progress_text.text("Fetching league standings...")
        progress_bar.progress(10, text="Fetching league standings...")

        if specific_manager_ids:
            st.info(
                f"Fetching data for {selected_division}: {len(specific_manager_ids)} managers")

        # Only fetch required number of managers if user doesn't need full table
        fetch_limit = None if calculate_all_adjusted else top_n
        with st.spinner("Fetching league standings..."):
            df = get_league_standings(
                league_id, current_gw=current_gw, limit=fetch_limit, progress_text=progress_text,
                specific_manager_ids=specific_manager_ids)

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
            with st.spinner("Calculating adjusted points..."):
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

    # --- Apply Division Filtering (only for league 2246) ---
    # Note: If specific_manager_ids was used, filtering is already applied during fetch
    if league_id == 2246 and selected_division and selected_division != "All Divisions" and specific_manager_ids is None:
        # This fallback is for cases where specific fetching wasn't used
        original_count = len(df)
        df = filter_by_division(df, selected_division)
        filtered_count = len(df)
        st.info(
            f"Showing {selected_division}: {filtered_count} managers (filtered from {original_count} total)")

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

            display_df("Top Captain Picks", df_captains, current_gw)
            if chip_counts.get("3xc", 0) > 0:
                display_df("Triple Captain Picks",
                           df_triple_captains, current_gw)
            if chip_counts.get("manager", 0) > 0:
                display_df("Manager Chip Selections",
                           df_manager_picks, current_gw)
        with col_detail2:
            display_df("Top Transfers In", df_transfers_in, current_gw)
            display_df("Top Transfers Out", df_transfers_out, current_gw)
    else:
        st.warning("Detailed analysis for top N managers could not be completed.")

    # Display Overall Standings (Conditionally include adjusted points)
    st.markdown("---")
    # Dynamic title based on division selection
    if league_id == 2246 and selected_division and selected_division != "All Divisions":
        st.subheader(f"{selected_division} Standings")
    else:
        st.subheader("League Standings")
    # Define base columns
    display_cols_main = [
        'rank', 'manager_name', 'team_name', 'manager_id', 'gw_points',
        'captain_name', 'vice_captain_name',
        'chip_used', 'transfer_penalty', 'points_on_bench',
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
        'points_on_bench': 'Bench Points',
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

    # Add a compact clickable link column
    if 'team_name' in df_display.columns and 'manager_id' in df_display.columns:
        # Add a compact link column
        df_display['view_team'] = df_display.apply(
            lambda row: f"https://fantasy.premierleague.com/entry/{row['manager_id']}/event/{current_gw}",
            axis=1
        )

        # Reorder columns to put link right after team_name
        cols = list(df_display.columns)
        team_name_idx = cols.index('team_name')
        cols.insert(team_name_idx + 1, cols.pop(cols.index('view_team')))
        df_display = df_display[cols]

    # Rename columns for display
    column_renames['view_team'] = '🔗'
    df_display.rename(columns=column_renames, inplace=True)

    # Configure the link column to be compact
    column_config = {}
    if '🔗' in df_display.columns:
        column_config = {
            "🔗": st.column_config.LinkColumn(
                "🔗",
                help="View team on FPL",
                width="small"
            )
        }

    if 'rank' in df_display.columns:
        df_display.set_index('rank', inplace=True)
        df_display.index.name = None

    # Display interactive dataframe
    st.dataframe(df_display, column_config=column_config,
                 use_container_width=True)

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
            display_df("🏆 Top Points (Raw)", top_points_week, current_gw)

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
                           top_points_week_without_chips, current_gw)
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
                display_df("📈 Most Improved Rank", most_improved, current_gw)
            else:
                display_df("📈 Most Improved Rank", pd.DataFrame(), current_gw)

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
                display_df("📉 Biggest Rank Drop", most_dropped, current_gw)
            else:
                display_df("📉 Biggest Rank Drop", pd.DataFrame(), current_gw)

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
                display_df("📈 Most Improved Rank (%)",
                           most_improved_pct, current_gw)

                most_dropped_pct = df.loc[df["pct_rank_change"]
                                          == valid_pct_change.min()]
                most_dropped_pct = most_dropped_pct[display_cols].rename(columns={
                    'manager_id': 'ID',
                    'manager_name': 'Manager',
                    'team_name': 'Team',
                    'pct_rank_change': 'Rank Change %'
                })
                display_df("📉 Biggest Rank Drop (%)",
                           most_dropped_pct, current_gw)
            else:
                display_df("📈 Most Improved Rank (%)",
                           pd.DataFrame(), current_gw)
                display_df("📉 Biggest Rank Drop (%)",
                           pd.DataFrame(), current_gw)

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
                display_df("📈 Most Improved Overall Rank",
                           most_improved, current_gw)
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
                           most_improved_pct, current_gw)

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
                display_df("📉 Biggest Overall Rank Drop",
                           most_dropped, current_gw)

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
