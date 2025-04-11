# FPL Mini-League Analysis Tool

A Streamlit application designed to analyze Fantasy Premier League (FPL) mini-leagues. It calculates chip-adjusted points for the current gameweek, fetches overall ranks, and provides detailed statistics for the top N managers in the league.

## Features

- **Adjusted Gameweek Points:** Calculates each manager's gameweek score excluding points gained from chips (Bench Boost, Triple Captain). Transfer costs are also factored in.
- **Overall Rank Display:** Fetches and displays the current overall FPL rank for each manager in the league.
- **League Standings:** Shows the classic league standings table with raw and adjusted points, rank changes, and overall rank.
- **General League Statistics:** Displays key stats for the gameweek, including:
  - Top raw points scorer.
  - Top adjusted points scorer.
  - Most improved rank (absolute and percentage).
  - Biggest rank drop (absolute and percentage).
- **Top N Manager Analysis:** Provides detailed insights into the top N managers (configurable) for the selected gameweek:
  - Average raw and adjusted points.
  - Average overall rank.
  - Chip usage breakdown (Wildcard, Free Hit, Bench Boost, Triple Captain).
  - Most captained players.
  - Most triple-captained players (if applicable).
  - Most transferred-in players.
  - Most transferred-out players.

## Demo

_[Link to your deployed Streamlit app will go here once deployed]_

## Setup (Local Development)

1.  **Clone the repository (Optional - if you put it on GitHub):**
    ```bash
    git clone <your-repository-url>
    cd fpl-streamlit-app
    ```
2.  **Create a virtual environment (Recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate # On Windows use `venv\Scripts\activate`
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Run the Streamlit app:**
    ```bash
    streamlit run app.py
    ```
    The app should open in your default web browser.

## Usage

1.  Navigate to the app (either locally or the deployed version).
2.  Enter your FPL **League ID** in the sidebar.
3.  Enter the **Current Gameweek** number you want to analyze.
4.  Specify the **Number of top managers (N)** you want detailed analysis for.
5.  Click the "**Run Analysis**" button.
6.  Wait for the data to be fetched and processed. Results will be displayed on the main page.

## File Structure

```
fpl-streamlit-app/
├── app.py             # Main Streamlit application script
├── requirements.txt   # Python package dependencies
├── utils/             # Utility functions folder
│   ├── __init__.py
│   ├── fpl_api.py     # Functions for interacting with the FPL API (league standings, overall rank)
│   ├── calculations.py# Functions for calculating adjusted points
│   └── top_n_analysis.py # Functions for detailed analysis of top N managers
└── README.md          # This file
```

## Dependencies

All required Python packages are listed in `requirements.txt`.
