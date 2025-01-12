import datetime
from concurrent.futures.thread import ThreadPoolExecutor

import boto3
from leaguepedia_parser.site.leaguepedia import leaguepedia

from models.league import League
from models.match import Match
from models.player import Player
from models.team import Team
from models.tournament import Tournament

ddb = boto3.resource('dynamodb', region_name='us-west-2')

leagues_table = ddb.Table('Leagues')
tournaments_table = ddb.Table('Tournaments')
games_table = ddb.Table('Games')
matches_table = ddb.Table('Matches')
player_table = ddb.Table('Players')
team_table = ddb.Table('Teams')


# Loading Funcs
ONLY_LOAD_RECENT = False

# https://lol.fandom.com/wiki/Special:CargoTables/Leagues
def load_leagues_and_return_leagues() -> [str]:
    print('Loading Leagues')
    res = leaguepedia.query(
        tables='Leagues',
        fields='League, League_Short, Region, Level, IsOfficial'
    )
    for league in res:
        ddb_item = League(league)
        existing = leagues_table.get_item(Key=ddb_item.key()).get('Item', None)
        if existing != ddb_item.ddb_format():
            print(f'Putting updated league new: {ddb_item.ddb_format()}, old: {existing}')
            leagues_table.put_item(Item=ddb_item.ddb_format())
        else:
            print(f'Skipping put for {ddb_item.leagueId}')
    return [league['League'] for league in res]


# https://lol.fandom.com/wiki/Special:CargoTables/Tournaments
def load_tourneys_and_return_overview_pages(leagues=None) -> []:
    if leagues is None:
        leagues = load_leagues_and_return_leagues()
    size = len(leagues)
    tourneys = []
    for i, league in enumerate(leagues):
        print(f'({i}/{size}) Loading Tournaments for {league}')
        res = leaguepedia.query(
            tables='Tournaments=T,Leagues=L',
            join_on="L.League=T.League",
            fields='T.Name, T.OverviewPage, T.DateStart, T.IsQualifier, T.IsPlayoffs, T.IsOfficial, T.Year, L.League_Short, T.Date, L.League',
            where=f"L.League='{league}'"
        )
        res = filter(lambda x: x['Name'], res)
        res = filter(filter_only_recent_tourneys, res)
        res = map(remap_tournaments_manual, res)

        for tourney in res:
            ddb_tourney = Tournament(tourney)
            existing = tournaments_table.get_item(Key=ddb_tourney.key()).get('Item', None)
            if existing != ddb_tourney.ddb_format():
                print(f'Putting new tournament {ddb_tourney}')
                tournaments_table.put_item(Item=ddb_tourney.ddb_format())
            else:
                print(f'Skipping put for {ddb_tourney.tournamentId}')
            tourneys.append(tourney)
    return tourneys


tourneys_to_exclude = {}


def remap_tournaments_manual(tourney):
    for key, value in tourneys_to_exclude.items():
        # print(f"Testing {tourney['Name']} and {key}")
        if key in tourney['Name']:
            print(f"Replacing {tourney['Name']} with {tourney['Name'].replace(key, value)}")
            tourney['Name'] = tourney["Name"].replace(key, value)
            break
    return tourney

# Filter out tourneys not from this year
def filter_only_recent_tourneys(tourney):
    if not ONLY_LOAD_RECENT:
        return True
    try:
        date = datetime.datetime.strptime(tourney['DateStart'], '%Y-%m-%d')
    except (TypeError, ValueError):
        return False

    return date.year == datetime.datetime.now().year


def load_matches_thread(i, overview_page, size):
    name = overview_page['Name']
    print(f'({i}/{size}) Loading games for {name}')
    try:
        res = leaguepedia.query(
            tables='MatchSchedule=MS,Tournaments=T,ScoreboardGames=SG',
            join_on="MS.OverviewPage=T.OverviewPage,T.OverviewPage=SG.OverviewPage",
            fields='MS.MatchId, MS.OverviewPage, T.Name, MS.Team1, MS.Team2, MS.Patch, MS.DateTime_UTC, MS.Winner, MS.BestOf, SG.VOD, MS.VodHighlights',
            where=f"T.Name='{name}'",
            order_by='MS.DateTime_UTC'
        )
    except Exception:
        print(f'Hit Error for {name}')
        return

    res = list(filter(filter_only_recent_matches, res))
    # print(f'trying to write {len(res)} matches')
    for match in res:
        ddb_item = Match(match)
        existing = matches_table.get_item(Key=ddb_item.key()).get('Item', None)
        if existing != ddb_item.ddb_format():
            # print(f'Putting new match {ddb_item.ddb_format()}')
            matches_table.put_item(Item=ddb_item.ddb_format())
        # else:
        #     print(f'Skipping upload for {ddb_item.matchId}')

# https://lol.fandom.com/wiki/Special:CargoTables/MatchSchedule
def load_matches(tourneys=None):
    if tourneys is None:
        tourneys = load_tourneys_and_return_overview_pages()

    size = len(tourneys)
    tp = ThreadPoolExecutor(max_workers=10)

    for i, overview_page in enumerate(tourneys):
        tp.submit(load_matches_thread, i, overview_page, size)

    tp.shutdown(wait=True)


# Adding this filter to reduce the cost of DDB Writes.
def filter_only_recent_matches(match):
    if not ONLY_LOAD_RECENT:
        return True
    try:
        date = datetime.datetime.strptime(match['DateTime UTC'], '%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError):
        return False
    now = datetime.datetime.now()
    # If the match is older than 12 hours we don't need to update it
    # If the match is farther than 2 weeks in the future don't worry about it
    return now - datetime.timedelta(hours=12) < date < now + datetime.timedelta(weeks=2)


# https://lol.fandom.com/wiki/Special:CargoTables/Players
def load_players():
    res = leaguepedia.query(
        tables='Players',
        fields='ID, Country, Age, Team, Residency, Role, IsSubstitute'
    )
    size = len(res)
    for i, player in enumerate(res):
        print(f'({i}/{size}) Loading player {player["ID"]}')
        ddb_item = Player(player)
        existing = player_table.get_item(Key=ddb_item.key()).get('Item', None)
        if existing != ddb_item.ddb_format():
            print(f'Putting new player: {ddb_item.ddb_format()}, existing: {existing}')
            player_table.put_item(Item=ddb_item.ddb_format())
        else:
            print(f'Skipping player {ddb_item.id}')


# https://lol.fandom.com/wiki/Special:CargoTables/Teams
def load_teams():
    res = leaguepedia.query(
        tables='Teams',
        fields='Name, Short, Location, Region, IsDisbanded',
        where='IsDisbanded=0'
    )
    size = len(res)
    for i, team in enumerate(res):
        print(f'({i}/{size}) Loading team {team["Name"]}')
        ddb_item = Team(team)
        existing = team_table.get_item(Key=ddb_item.key()).get('Item', None)
        if existing != ddb_item.ddb_format():
            print(f'Putting team new: {ddb_item.ddb_format()}, old: {existing}')
            team_table.put_item(Item=ddb_item.ddb_format())
        else:
            print(f'Skipping team {ddb_item.teamId}')
