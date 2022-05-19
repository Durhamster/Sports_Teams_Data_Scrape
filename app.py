import glob
import os
import pandas as pd
from rich import print
from rich.console import Console
from rich.progress import Progress
from team_ids import mlb_team_ids, nfl_team_ids, nhl_team_ids
from time import sleep


def getLeague(subdir, start_year, end_year, league, subleague):
    """Fetches historical team standing data for the user selected league

    Args:
        subdir (string): name of the subdirectory the csv file(s) will be outputted to.
        start_year (int): Earliest year to scrape for.
        end_year (int): Latest completed season to scrape for.
        league (string): League the user has selected.
        subleague (string): Specific Conference or historical name for a league.
    """

    if league == "NCAAF":
        if not os.path.exists(f"{cwd}/scraped_data/{league}/{subleague}"):
            os.makedirs(f"{cwd}/scraped_data/{league}/{subleague}")

    if league == "MLB" or league == "NFL" or league == "NHL":
        print(
            f"Fetching standings data for team history of all current active franchises..."
        )
    else:
        print(
            f"Fetching standings/table data for [bold green]{subleague.upper()}[/bold green] ({start_year} - {end_year - 1})..."
        )

    seasons = [*range(start_year, end_year, 1)]

    with Progress() as progress:
        if league == "MLB":
            task = progress.add_task("[cyan]Scraping...", total=(30))
        elif league == "NFL" or league == "NHL":
            task = progress.add_task("[cyan]Scraping...", total=(32))
        else:
            task = progress.add_task("[cyan]Scraping...", total=(end_year - start_year))

        while not progress.finished:
            for year in seasons:
                if league == "big5":
                    df = pd.read_html(
                        f"https://fbref.com/en/comps/Big5/{year - 1}-{year}/{year - 1}-{year}-Big-5-European-Leagues-Stats"
                    )

                elif league == "MLB":
                    for team in mlb_team_ids:
                        df = pd.read_html(
                            f"https://www.baseball-reference.com/teams/{team}/#all_franchise_years"
                        )
                        df = df[0].to_csv(
                            f"{cwd}/scraped_data/{subdir}/{team}.csv", index=False
                        )

                        progress.update(task, advance=1)
                        sleep(1)

                elif league == "NFL":

                    for team in nfl_team_ids:
                        df = pd.read_html(
                            f"https://www.pro-football-reference.com/teams/{team}/"
                        )
                        df = df[0].to_csv(
                            f"{cwd}/scraped_data/{subdir}/{team}.csv", index=False
                        )

                        progress.update(task, advance=1)
                        sleep(1)

                elif league == "NHL":

                    for team in nhl_team_ids:
                        df = pd.read_html(
                            f"https://www.hockey-reference.com/teams/{team}/history.html"
                        )
                        df = df[0].to_csv(
                            f"{cwd}/scraped_data/{subdir}/{team}.csv", index=False
                        )

                        progress.update(task, advance=1)
                        sleep(1)

                elif league == "NBA":
                    df = pd.read_html(
                        f"https://www.basketball-reference.com/leagues/{subleague}_{year}.html"
                    )

                elif league == "NCAAF":
                    df = pd.read_html(
                        f"https://www.sports-reference.com/cfb/conferences/{subleague}/{year}.html"
                    )

                # Add Date col to all files
                if league == "big5":
                    df = df[0].to_csv(
                        f"{cwd}/scraped_data/{subleague}/{subleague}-{year - 1}-{year}.csv",
                        index=False,
                    )

                    year_df = pd.read_csv(
                        f"{cwd}/scraped_data/{subdir}/{subleague}-{year - 1}-{year}.csv"
                    )
                    year_df["yearId"] = year
                    year_df.to_csv(
                        f"{cwd}/scraped_data/{subdir}/{subleague}-{year - 1}-{year}.csv"
                    )

                    progress.update(task, advance=1)
                    sleep(1)

                elif league == "MLB" or league == "NFL" or league == "NHL":
                    return None

                elif league == "NCAAF":
                    df = df[0].to_csv(
                        f"{cwd}/scraped_data/{subdir}/{subleague}/{subleague}-{year}.csv",
                        index=False,
                    )

                    # Add Date col to all files
                    year_df = pd.read_csv(
                        f"{cwd}/scraped_data/{subdir}/{subleague}/{subleague}-{year}.csv"
                    )
                    year_df["yearId"] = year
                    year_df.to_csv(
                        f"{cwd}/scraped_data/{subdir}/{subleague}/{subleague}-{year}.csv"
                    )

                    progress.update(task, advance=1)
                    sleep(1)

                else:
                    df = df[0].to_csv(
                        f"{cwd}/scraped_data/{subdir}/{subleague}-{year}.csv",
                        index=False,
                    )

                    # Add Date col to all files
                    year_df = pd.read_csv(
                        f"{cwd}/scraped_data/{subdir}/{subleague}-{year}.csv"
                    )
                    year_df["yearId"] = year
                    year_df.to_csv(
                        f"{cwd}/scraped_data/{subdir}/{subleague}-{year}.csv"
                    )

                    progress.update(task, advance=1)
                    sleep(1)


def combine_data(subdir, subleague):
    """Combines all the csv files for a subdir into one csv file

    Args:
        subdir (string): League the user has selected.
        subleague (string): Specific Conference or historical name for a league.
    """
    # Combine the Data
    if subdir == "big5":
        print(f"...done! Combining data into {subleague}-ALL.csv...\n")
        os.chdir(f"{cwd}/scraped_data/{subdir}/")
        all_csv_files = [i for i in glob.glob("*.{}".format("csv"))]
        combined_dfs = pd.concat([pd.read_csv(f) for f in all_csv_files])
        combined_dfs.to_csv(f"{subleague}-ALL.csv", index=False, encoding="uft-8")
        os.chdir(cwd)

    elif subdir == "NCAAF":
        print(f"...done! Combining data into {subleague}-ALL.csv...\n")
        os.chdir(f"{cwd}/scraped_data/{subdir}/{subleague}/")
        all_csv_files = [i for i in glob.glob("*.{}".format("csv"))]
        combined_dfs = pd.concat([pd.read_csv(f) for f in all_csv_files])
        combined_dfs.to_csv(f"{subleague}-ALL.csv", index=False, encoding="utf-8")
        # Removes extra headers
        cleaned_df = pd.read_csv(f"{subleague}-ALL.csv", header=[1])
        cleaned_df = cleaned_df[cleaned_df.W != "W"]
        cleaned_df.to_csv(f"{subleague}-ALL.csv", index=False)
        os.chdir(cwd)

    else:
        print(f"...done! Combining data into {subleague}-ALL.csv...\n")
        os.chdir(f"{cwd}/scraped_data/{subdir}/")
        all_csv_files = [i for i in glob.glob("*.{}".format("csv"))]
        combined_dfs = pd.concat([pd.read_csv(f) for f in all_csv_files])
        combined_dfs.to_csv(f"{subleague}-ALL.csv", index=False, encoding="utf-8")
        os.chdir(cwd)


if __name__ == "__main__":

    cwd = os.getcwd()

    console = Console()

    # Must be one greater than the actual last season (i.e. 2022 if the latest season is latest_season)
    # This is set as a var so it will be easier to update later.
    latest_season = 2022

    if not os.path.exists(f"{cwd}/scraped_data"):
        os.makedirs(f"{cwd}/scraped_data")

    league = int(
        console.input(
            "\nWhich league(s) do you want to scrape standings/table data for?:\n"
            "1) [green]Major League Baseball[/green]\n"
            "2) National Basketball Association\n"
            "3) [green]National Football League[/green]\n"
            "4) National Hockey League\n"
            "5) [green]NCAA Football Division 1[/green]\n"
            "6) The Big 5 European Leagues (Bundesliga, La Liga, Ligue 1, Premier, Serie A)\n"
        )
    )

    if league == 1:
        if not os.path.exists(f"{cwd}/scraped_data/MLB"):
            os.makedirs(f"{cwd}/scraped_data/MLB")

        getLeague("MLB", 2020, latest_season, "MLB", "MLB")

    elif league == 2:
        if not os.path.exists(f"{cwd}/scraped_data/NBA"):
            os.makedirs(f"{cwd}/scraped_data/NBA")

        getLeague("NBA", 1947, 1950, "NBA", "BAA")
        combine_data("NBA", "BAA")
        getLeague("NBA", 1950, latest_season, "NBA", "NBA")
        combine_data("NBA", "NBA")

    elif league == 3:
        if not os.path.exists(f"{cwd}/scraped_data/NFL"):
            os.makedirs(f"{cwd}/scraped_data/NFL")

        getLeague("NFL", 1918, latest_season, "NFL", "NFL")

    elif league == 4:
        if not os.path.exists(f"{cwd}/scraped_data/NHL"):
            os.makedirs(f"{cwd}/scraped_data/NHL")

        getLeague("NHL", 1918, latest_season, "NHL", "NHL")

    elif league == 5:
        if not os.path.exists(f"{cwd}/scraped_data/NCAAF"):
            os.makedirs(f"{cwd}/scraped_data/NCAAF")

        getLeague("NCAAF", 2013, latest_season, "NCAAF", "american")
        combine_data("NCAAF", "american")
        getLeague("NCAAF", 1991, 2013, "NCAAF", "big-east")
        combine_data("NCAAF", "big-east")
        getLeague("NCAAF", 1953, latest_season, "NCAAF", "acc")
        combine_data("NCAAF", "acc")
        getLeague("NCAAF", 1996, latest_season, "NCAAF", "big-12")
        combine_data("NCAAF", "big-12")
        getLeague("NCAAF", 1988, 2001, "NCAAF", "big-west")
        combine_data("NCAAF", "big-west")
        getLeague("NCAAF", 1969, 1987, "NCAAF", "pcaa")
        combine_data("NCAAF", "pcaa")
        getLeague("NCAAF", 1953, latest_season, "NCAAF", "big-ten")
        combine_data("NCAAF", "big-ten")
        getLeague("NCAAF", 1996, latest_season, "NCAAF", "cusa")
        combine_data("NCAAF", "cusa")
        getLeague("NCAAF", 1900, latest_season, "NCAAF", "independent")
        combine_data("NCAAF", "independent")
        getLeague("NCAAF", 1962, latest_season, "NCAAF", "mac")
        combine_data("NCAAF", "mac")
        getLeague("NCAAF", 2011, latest_season, "NCAAF", "pac-12")
        combine_data("NCAAF", "pac-12")
        getLeague("NCAAF", 1978, 2011, "NCAAF", "pac-10")
        combine_data("NCAAF", "pac-10")
        getLeague("NCAAF", 1968, 1978, "NCAAF", "pac-8")
        combine_data("NCAAF", "pac-8")
        getLeague("NCAAF", 1959, 1968, "NCAAF", "aawu")
        combine_data("NCAAF", "aawu")
        getLeague("NCAAF", 1916, 1959, "NCAAF", "pcc")
        combine_data("NCAAF", "pcc")
        getLeague("NCAAF", 1933, latest_season, "NCAAF", "sec")
        combine_data("NCAAF", "sec")
        getLeague("NCAAF", 2001, latest_season, "NCAAF", "sun-belt")
        combine_data("NCAAF", "sun-belt")

    elif league == 6:
        if not os.path.exists(f"{cwd}/scraped_data/big5"):
            os.makedirs(f"{cwd}/scraped_data/big5")

        getLeague("big5", 1996, latest_season, "big5", "big5")
        combine_data("big5", "big5")

    print(
        f"[green]Complete![/green] Data can be found in: [cyan]{cwd}/scraped_data/[/cyan]\n"
    )
