import datetime
import logging
import warnings
from time import sleep
from typing import List

import boto3
from botocore.config import Config
from mwclient.errors import MaximumRetriesExceeded, APIError
from rich.logging import RichHandler
from rich.progress import track, Progress, TaskID

from leaguepedia.leaguepedia import LeaguepediaSite
from models.league import League
from models.match import Match
from models.player import Player
from models.team import Team
from models.tournament import Tournament

leaguepedia = LeaguepediaSite()

warnings.filterwarnings(action="ignore", message=r"datetime.datetime.utcnow")

ddb = boto3.resource(
    "dynamodb", region_name="us-west-2", config=Config(max_pool_connections=30)
)

leagues_table = ddb.Table("Leagues")
tournaments_table = ddb.Table("Tournaments")
games_table = ddb.Table("Games")
matches_table = ddb.Table("Matches")
player_table = ddb.Table("Players")
team_table = ddb.Table("Teams")

logging.basicConfig(level="INFO", datefmt="[%X]", handlers=[RichHandler()])

logger = logging.getLogger(__name__)

# Remove the recency time filters.
# This will load all history and will take a long time
LOAD_HISTORICAL = False
SLEEP = True


# https://lol.fandom.com/wiki/Special:CargoTables/Leagues
def load_leagues_and_return_leagues() -> List[str]:
    logger.info("Loading Leagues")
    res = leaguepedia.query(
        tables="Leagues", fields="League, League_Short, Region, Level, IsOfficial"
    )
    updated_leagues = 0
    for league in track(res, description="Loading Leagues"):
        ddb_item = League(league)
        existing = leagues_table.get_item(Key=ddb_item.key()).get("Item", None)
        if existing != ddb_item.ddb_format():
            logger.debug(
                f"Putting updated league new: {ddb_item.ddb_format()}, old: {existing}"
            )
            leagues_table.put_item(Item=ddb_item.ddb_format())
            updated_leagues += 1
        else:
            logger.debug(f"Skipping put for {ddb_item.leagueId}")

    logger.info(f"Updated {updated_leagues} leagues.")

    return [league["League"] for league in res]


# https://lol.fandom.com/wiki/Special:CargoTables/Tournaments
def load_tourneys_and_return_overview_pages(leagues=None) -> List:
    if leagues is None:
        leagues = load_leagues_and_return_leagues()
    tourneys = []

    progress = Progress(transient=True)
    progress.start()
    overall = progress.add_task("Loading Tournaments", total=len(leagues))
    updated_tourneys = 0
    for league in leagues:
        progress.advance(overall)
        try:
            res = leaguepedia.query(
                tables="Tournaments=T,Leagues=L",
                join_on="L.League=T.League",
                fields="T.Name, T.OverviewPage, T.DateStart, T.IsQualifier, T.IsPlayoffs, T.IsOfficial, T.Year, L.League_Short, T.Date, L.League",
                where=f"L.League='{league}'",
            )
            if SLEEP:
                sleep(2)
        except (MaximumRetriesExceeded, APIError) as e:
            logger.warning(f"Hit error querying {league}", exc_info=e)
            continue
        res = filter(lambda x: x["Name"], res)
        res = filter(filter_only_recent_tourneys, res)
        res = map(remap_tournaments_manual, res)
        res = list(res)

        local_league = progress.add_task(
            f"Loading tournaments for {league}", total=len(res)
        )
        for tourney in res:
            ddb_tourney = Tournament(tourney)
            existing = tournaments_table.get_item(Key=ddb_tourney.key()).get(
                "Item", None
            )
            if existing != ddb_tourney.ddb_format():
                logger.debug(f"Putting new tournament {ddb_tourney}")
                tournaments_table.put_item(Item=ddb_tourney.ddb_format())
                updated_tourneys += 1
            else:
                logger.debug(f"Skipping put for {ddb_tourney.tournamentId}")
            tourneys.append(tourney)
            progress.advance(local_league)
        progress.stop_task(local_league)
        progress.update(local_league, visible=False)
    progress.stop_task(overall)
    progress.stop()
    logger.info(f"Updated {updated_tourneys} Tourneys")
    return tourneys


tourneys_to_exclude = {}


def remap_tournaments_manual(tourney):
    for key, value in tourneys_to_exclude.items():
        logger.debug(f"Testing {tourney['Name']} and {key}")
        if key in tourney["Name"]:
            logger.debug(
                f"Replacing {tourney['Name']} with {tourney['Name'].replace(key, value)}"
            )
            tourney["Name"] = tourney["Name"].replace(key, value)
            break
    return tourney


# Filter out tourneys not from this year
def filter_only_recent_tourneys(tourney):
    if LOAD_HISTORICAL:
        return True
    try:
        date = datetime.datetime.strptime(tourney["DateStart"], "%Y-%m-%d")
    except (TypeError, ValueError):
        return False

    return date.year == datetime.datetime.now().year


def load_matches_thread(overview_page, progress: Progress, overall: TaskID):
    name = overview_page["Name"]
    try:
        res = leaguepedia.query(
            tables="MatchSchedule=MS,Tournaments=T,MatchScheduleGame=MSG",
            join_on="MS.OverviewPage=T.OverviewPage,MS.MatchId=MSG.MatchId",
            fields="MS.MatchId,MS.OverviewPage,T.Name,MS.Team1,MS.Team2,"
            "MS.Patch,MS.DateTime_UTC,MS.Winner,MS.BestOf,MSG.VodGameStart,MS.VodHighlights",
            where=f"T.Name='{name}' AND MSG.N_GameInMatch=1",
            order_by="MS.DateTime_UTC",
        )
    except (MaximumRetriesExceeded, APIError) as e:
        logger.warning(f"Hit Error for {name}", exc_info=e)
        return

    res = list(filter(filter_only_recent_matches, res))
    logger.debug(f"Found results: {res}")
    local_matches = progress.add_task(
        f"Loading Matches for {overview_page['Name']}", total=len(res)
    )
    updated_matches = 0
    for match in res:
        ddb_item = Match(match)
        existing = matches_table.get_item(Key=ddb_item.key()).get("Item", None)
        if existing != ddb_item.ddb_format():
            logger.debug(
                f"Putting new match {ddb_item.ddb_format()}, old match {existing}"
            )
            matches_table.put_item(Item=ddb_item.ddb_format())
            updated_matches += 1
        else:
            logger.debug(f"Skipping upload for {ddb_item.matchId}")
        progress.advance(local_matches)
    logger.info(f"Updated {updated_matches} for {name}")
    progress.stop_task(local_matches)
    progress.update(local_matches, visible=False)
    progress.advance(overall)


# https://lol.fandom.com/wiki/Special:CargoTables/MatchSchedule
def load_matches(tourneys=None):
    if tourneys is None:
        tourneys = load_tourneys_and_return_overview_pages()

    progress = Progress(transient=True)
    progress.start()
    overall = progress.add_task("Loading Matches", total=len(tourneys))
    for overview_page in tourneys:
        load_matches_thread(overview_page, progress, overall)
        if SLEEP:
            sleep(2)

    progress.stop_task(overall)
    progress.stop()


# Adding this filter to reduce the cost of DDB Writes.
def filter_only_recent_matches(match):
    if LOAD_HISTORICAL:
        return True
    try:
        date = datetime.datetime.strptime(match["DateTime UTC"], "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return False
    now = datetime.datetime.now()

    # If the match is older than 1 week we don't need to update it. Vods/highlights are updated late
    # If the match is farther than 2 weeks in the future don't worry about it
    return now - datetime.timedelta(weeks=1) < date < now + datetime.timedelta(weeks=2)


# https://lol.fandom.com/wiki/Special:CargoTables/Players
def load_players():
    res = leaguepedia.query(
        tables="Players", fields="ID, Country, Age, Team, Residency, Role, IsSubstitute"
    )
    for player in track(res):
        ddb_item = Player(player)
        existing = player_table.get_item(Key=ddb_item.key()).get("Item", None)
        if existing != ddb_item.ddb_format():
            logger.debug(
                f"Putting new player: {ddb_item.ddb_format()}, existing: {existing}"
            )
            player_table.put_item(Item=ddb_item.ddb_format())
        else:
            logger.debug(f"Skipping player {ddb_item.id}")


# https://lol.fandom.com/wiki/Special:CargoTables/Teams
def load_teams():
    res = leaguepedia.query(
        tables="Teams",
        fields="Name, Short, Location, Region, IsDisbanded",
        where="IsDisbanded=0",
    )
    for team in track(res):
        ddb_item = Team(team)
        existing = team_table.get_item(Key=ddb_item.key()).get("Item", None)
        if existing != ddb_item.ddb_format():
            logger.debug(f"Putting team new: {ddb_item.ddb_format()}, old: {existing}")
            team_table.put_item(Item=ddb_item.ddb_format())
        else:
            logger.debug(f"Skipping team {ddb_item.teamId}")
