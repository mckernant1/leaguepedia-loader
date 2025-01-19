import datetime
import logging
import warnings
from concurrent.futures.thread import ThreadPoolExecutor
from logging import Logger

import boto3
from botocore.config import Config

from leaguepedia_parser.site.leaguepedia import leaguepedia
from rich.logging import RichHandler
from rich.progress import track, Progress, TaskID

from models.league import League
from models.match import Match
from models.player import Player
from models.team import Team
from models.tournament import Tournament

warnings.filterwarnings(action="ignore", message=r"datetime.datetime.utcnow")

ddb = boto3.resource('dynamodb', region_name='us-west-2', config=Config(
    max_pool_connections=30
))

leagues_table = ddb.Table('Leagues')
tournaments_table = ddb.Table('Tournaments')
games_table = ddb.Table('Games')
matches_table = ddb.Table('Matches')
player_table = ddb.Table('Players')
team_table = ddb.Table('Teams')

logging.basicConfig(
    level="INFO", datefmt="[%X]", handlers=[RichHandler()]
)

logger = Logger(__name__)

# Remove the recency time filters.
# This will load all history and will take a long time
LOAD_HISTORICAL = False

# https://lol.fandom.com/wiki/Special:CargoTables/Leagues
def load_leagues_and_return_leagues() -> [str]:
    logger.info('Loading Leagues')
    res = leaguepedia.query(
        tables='Leagues',
        fields='League, League_Short, Region, Level, IsOfficial'
    )
    for league in track(res, description='Loading Leagues'):
        ddb_item = League(league)
        existing = leagues_table.get_item(Key=ddb_item.key()).get('Item', None)
        if existing != ddb_item.ddb_format():
            logger.debug(f'Putting updated league new: {ddb_item.ddb_format()}, old: {existing}')
            leagues_table.put_item(Item=ddb_item.ddb_format())
        else:
            logger.debug(f'Skipping put for {ddb_item.leagueId}')
    return [league['League'] for league in res]



# https://lol.fandom.com/wiki/Special:CargoTables/Tournaments
def load_tourneys_and_return_overview_pages(leagues=None) -> []:
    if leagues is None:
        leagues = load_leagues_and_return_leagues()
    tourneys = []

    progress = Progress(transient=True)
    progress.start()
    overall = progress.add_task('Loading Tournaments', total=len(leagues))

    for league in leagues:
        progress.advance(overall)
        res = leaguepedia.query(
            tables='Tournaments=T,Leagues=L',
            join_on="L.League=T.League",
            fields='T.Name, T.OverviewPage, T.DateStart, T.IsQualifier, T.IsPlayoffs, T.IsOfficial, T.Year, L.League_Short, T.Date, L.League',
            where=f"L.League='{league}'"
        )
        res = filter(lambda x: x['Name'], res)
        res = filter(filter_only_recent_tourneys, res)
        res = map(remap_tournaments_manual, res)
        res = list(res)

        local_league = progress.add_task(f'Loading tournaments for {league}', total=len(res))
        for tourney in res:
            ddb_tourney = Tournament(tourney)
            existing = tournaments_table.get_item(Key=ddb_tourney.key()).get('Item', None)
            if existing != ddb_tourney.ddb_format():
                logger.debug(f'Putting new tournament {ddb_tourney}')
                tournaments_table.put_item(Item=ddb_tourney.ddb_format())
            else:
                logger.debug(f'Skipping put for {ddb_tourney.tournamentId}')
            tourneys.append(tourney)
            progress.advance(local_league)
        progress.stop_task(local_league)
        progress.update(local_league, visible=False)
    progress.stop_task(overall)
    progress.stop()
    return tourneys


tourneys_to_exclude = {}


def remap_tournaments_manual(tourney):
    for key, value in tourneys_to_exclude.items():
        logger.debug(f"Testing {tourney['Name']} and {key}")
        if key in tourney['Name']:
            logger.debug(f"Replacing {tourney['Name']} with {tourney['Name'].replace(key, value)}")
            tourney['Name'] = tourney["Name"].replace(key, value)
            break
    return tourney

# Filter out tourneys not from this year
def filter_only_recent_tourneys(tourney):
    if LOAD_HISTORICAL:
        return True
    try:
        date = datetime.datetime.strptime(tourney['DateStart'], '%Y-%m-%d')
    except (TypeError, ValueError):
        return False

    return date.year == datetime.datetime.now().year


def load_matches_thread(overview_page, progress: Progress, overall: TaskID):
    name = overview_page['Name']
    try:
        res = leaguepedia.query(
            tables='MatchSchedule=MS,Tournaments=T,ScoreboardGames=SG',
            join_on="MS.OverviewPage=T.OverviewPage,T.OverviewPage=SG.OverviewPage",
            fields='MS.MatchId, MS.OverviewPage, T.Name, MS.Team1, MS.Team2, MS.Patch, MS.DateTime_UTC, MS.Winner, MS.BestOf, SG.VOD, MS.VodHighlights',
            where=f"T.Name='{name}'",
            order_by='MS.DateTime_UTC'
        )
    except Exception:
        logger.debug(f'Hit Error for {name}')
        return

    res = list(filter(filter_only_recent_matches, res))
    local_matches = progress.add_task(f'Loading Matches for {overview_page["Name"]}', total=len(res))
    for match in res:
        ddb_item = Match(match)
        existing = matches_table.get_item(Key=ddb_item.key()).get('Item', None)
        if existing != ddb_item.ddb_format():
            logger.debug(f'Putting new match {ddb_item.ddb_format()}')
            matches_table.put_item(Item=ddb_item.ddb_format())
        else:
            logger.debug(f'Skipping upload for {ddb_item.matchId}')
        progress.advance(local_matches)
    progress.stop_task(local_matches)
    progress.update(local_matches, visible=False)
    progress.advance(overall)

# https://lol.fandom.com/wiki/Special:CargoTables/MatchSchedule
def load_matches(tourneys=None):
    if tourneys is None:
        tourneys = load_tourneys_and_return_overview_pages()

    tp = ThreadPoolExecutor(max_workers=20)

    progress = Progress(transient=True)
    progress.start()
    overall = progress.add_task('Loading Matches', total=len(tourneys))
    for overview_page in tourneys:
        tp.submit(load_matches_thread, overview_page, progress, overall)

    tp.shutdown(wait=True)
    progress.stop_task(overall)
    progress.stop()


# Adding this filter to reduce the cost of DDB Writes.
def filter_only_recent_matches(match):
    if LOAD_HISTORICAL:
        return True
    try:
        date = datetime.datetime.strptime(match['DateTime UTC'], '%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError):
        return False
    now = datetime.datetime.now()

    # If the match is older than 4 days we don't need to update it
    # If the match is farther than 2 weeks in the future don't worry about it
    return now - datetime.timedelta(days=4) < date < now + datetime.timedelta(weeks=2)


# https://lol.fandom.com/wiki/Special:CargoTables/Players
def load_players():
    res = leaguepedia.query(
        tables='Players',
        fields='ID, Country, Age, Team, Residency, Role, IsSubstitute'
    )
    for player in track(res):
        ddb_item = Player(player)
        existing = player_table.get_item(Key=ddb_item.key()).get('Item', None)
        if existing != ddb_item.ddb_format():
            logger.debug(f'Putting new player: {ddb_item.ddb_format()}, existing: {existing}')
            player_table.put_item(Item=ddb_item.ddb_format())
        else:
            logger.debug(f'Skipping player {ddb_item.id}')


# https://lol.fandom.com/wiki/Special:CargoTables/Teams
def load_teams():
    res = leaguepedia.query(
        tables='Teams',
        fields='Name, Short, Location, Region, IsDisbanded',
        where='IsDisbanded=0'
    )
    for team in track(res):
        ddb_item = Team(team)
        existing = team_table.get_item(Key=ddb_item.key()).get('Item', None)
        if existing != ddb_item.ddb_format():
            logger.debug(f'Putting team new: {ddb_item.ddb_format()}, old: {existing}')
            team_table.put_item(Item=ddb_item.ddb_format())
        else:
            logger.debug(f'Skipping team {ddb_item.teamId}')
