import json
from enum import Enum

import polars as pl
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


# team enum
class BREFTeams(Enum):
    ANGELS = "ANA"
    DIAMONDBACKS = "ARI"
    BRAVES = "ATL"
    ORIOLES = "BAL"
    RED_SOX = "BOS"
    CUBS = "CHC"
    WHITE_SOX = "CHW"
    REDS = "CIN"
    GUARDIANS = "CLE"
    ROCKIES = "COL"
    TIGERS = "DET"
    MARLINS = "FLA"
    ASTROS = "HOU"
    ROYALS = "KCR"
    DODGERS = "LAD"
    BREWERS = "MIL"
    TWINS = "MIN"
    METS = "NYM"
    YANKEES = "NYY"
    ATHLETICS = "OAK"
    PHILLIES = "PHI"
    PIRATES = "PIT"
    PADRES = "SDP"
    MARINERS = "SEA"
    GIANTS = "SFG"
    CARDINALS = "STL"
    RAYS = "TBD"
    RANGERS = "TEX"
    BLUE_JAYS = "TOR"
    NATIONALS = "WSN"

    @classmethod
    def show_options(cls):
        return "\n".join([f"{team.name}: {team.value}" for team in cls])


BREF_TEAM_RECORD_URL = "https://www.baseball-reference.com/teams/{team_code}/"


def fetch_page_html(url: str) -> str:
    """
    Fetches the full HTML of a Baseball Reference page using Playwright
    (bypasses Cloudflare JavaScript challenge).
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)
        # Wait until network is idle (all JS/XHRs done)
        page.wait_for_load_state("networkidle")
        html = page.content()
        browser.close()
        return html


def _extract_table(table):
    trs = table.tbody.find_all("tr")
    row_data = {}
    for tr in trs:
        if tr.has_attr("class") and "thead" in tr["class"]:
            continue
        tds = tr.find_all("th")
        tds.extend(tr.find_all("td"))
        if len(tds) == 0:
            continue
        for td in tds:
            data_stat = td.attrs["data-stat"]
            if data_stat not in row_data:
                row_data[data_stat] = []
            if td.find("a"):
                row_data[data_stat].append(td.find("a").text)
            elif td.find("span"):
                row_data[data_stat].append(td.find("span").string)
            elif td.find("strong"):
                row_data[data_stat].append(td.find("strong").string)
            else:
                row_data[data_stat].append(td.string)
    return row_data


def bref_teams_yearly_history(
    team: BREFTeams,
    start_season: int = None,
    end_season: int = None,
    # return_pandas: bool = False,
) -> pl.DataFrame:
    if team is None:
        raise ValueError("Must provide a team")
    html = fetch_page_html(BREF_TEAM_RECORD_URL.format(team_code=team.value))
    soup = BeautifulSoup(html, "html.parser")
    franch_history_table = soup.find("table", {"id": "franchise_years"})
    df = pl.DataFrame(_extract_table(franch_history_table))
    df = df.with_columns(
        [
            pl.col(
                [
                    "G",
                    "W",
                    "L",
                    "ties",
                    "R",
                    "RA",
                    "batters_used",
                    "pitchers_used",
                    "year_ID",
                ]
            ).cast(pl.Int16),
            pl.col(
                [
                    "win_loss_perc",
                    "win_loss_perc_pythag",
                    "age_bat",
                    "age_pit",
                ]
            ).cast(pl.Float32),
        ]
    )
    df = df.with_columns(pl.col("games_back").str.replace("--", "0").cast(pl.Float32))
    if start_season:
        df = df.filter(pl.col("year_ID") >= start_season)
    if end_season:
        df = df.filter(pl.col("year_ID") <= end_season)
    return df


if __name__ == "__main__":
    win_pct_map = {}
    for team in BREFTeams:
        print(f"Fetching data for {team.name} ({team.value})")
        df = bref_teams_yearly_history(team, start_season=1998, end_season=2024)
        avg_win_pct = df.select(pl.col("win_loss_perc").mean()).item()
        win_pct_map[team.name] = avg_win_pct
        print(f"{team.name}  Average Win %: {avg_win_pct:.3f}")
    json.dump(win_pct_map, open("data/team_win_pct.json", "w"), indent=4)
