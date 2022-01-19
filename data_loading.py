import datetime

import boto3
from leaguepedia_parser.site.leaguepedia import leaguepedia
from setuptools.namespaces import flatten

ddb = boto3.resource('dynamodb')

leagues_table = ddb.Table('Leagues')
tournaments_table = ddb.Table('Tournaments')
games_table = ddb.Table('Games')
player_table = ddb.Table('Players')
team_table = ddb.Table('Teams')


# https://lol.fandom.com/wiki/Special:CargoTables/Leagues
def load_leagues_and_return_leages() -> [str]:
    print('Loading Leagues')
    res = leaguepedia.query(
        tables='Leagues',
        fields='League, League_Short, Region, Level, IsOfficial'
    )
    for league in res:
        league['League_Short'] = league.pop('League Short')
        leagues_table.put_item(Item=league)
    return [league['League'] for league in res]


# https://lol.fandom.com/wiki/Special:CargoTables/Tournaments
def load_tourneys_and_return_overview_pages(leagues=None) -> []:
    if leagues is None:
        leagues = load_leagues_and_return_leages()
    size = len(leagues)
    for i, league in enumerate(leagues):
        print(f'({i}/{size}) Loading Tournaments for {league}')
        res = leaguepedia.query(
            tables='Tournaments',
            fields='League, Name, OverviewPage, DateStart, IsQualifier, IsPlayoffs, IsOfficial, Year',
            where=f"League='{league}'"
        )
        for tourney in res:
            tournaments_table.put_item(Item=tourney)
        yield [tourney for tourney in res]


# https://lol.fandom.com/wiki/Special:CargoTables/ScoreboardGames
def load_games(overview_pages=None):
    if overview_pages is None:
        year = str(datetime.datetime.now().year)
        overview_pages = list(filter(lambda x: x['Year'] == year, flatten(load_tourneys_and_return_overview_pages())))

    size = len(overview_pages)
    for i, overview_page in enumerate(overview_pages):
        overview_page = overview_page['OverviewPage']
        print(f'({i}/{size}) Loading games for {overview_page}')
        res = leaguepedia.query(
            tables='ScoreboardGames',
            fields='GameId, MatchId, OverviewPage, Tournament, Team1, Team2, VOD, Patch, N_GameInMatch, Gamename, '
                   'DateTime_UTC',
            where=f"OverviewPage='{overview_page}'",
            order_by='DateTime_UTC'
        )
        for game in res:
            games_table.put_item(Item=game)


# https://lol.fandom.com/wiki/Special:CargoTables/Players
def load_players():
    res = leaguepedia.query(
        tables='Players',
        fields='ID, Country, Age, Team, Residency, Role, IsSubstitute'
    )
    size = len(res)
    for i, player in enumerate(res):
        print(f'({i}/{size}) Loading player {player["ID"]}')
        player_table.put_item(Item=player)


# https://lol.fandom.com/wiki/Special:CargoTables/Teams
def load_teams():
    res = leaguepedia.query(
        tables='Teams',
        fields='Name, Short, Location, Region, IsDisbanded'
    )
    size = len(res)
    for i, team in enumerate(res):
        print(f'({i}/{size}) Loading team {team["Name"]}')
        team_table.put_item(Item=team)