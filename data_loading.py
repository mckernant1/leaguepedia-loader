import datetime
from decimal import Decimal
from pprint import pprint

import boto3
from leaguepedia_parser.site.leaguepedia import leaguepedia

ddb = boto3.resource('dynamodb')

leagues_table = ddb.Table('Leagues')
tournaments_table = ddb.Table('Tournaments')
games_table = ddb.Table('Games')
matches_table = ddb.Table('Matches')
player_table = ddb.Table('Players')
team_table = ddb.Table('Teams')


# Loading Funcs

# https://lol.fandom.com/wiki/Special:CargoTables/Leagues
def load_leagues_and_return_leages() -> [str]:
    print('Loading Leagues')
    res = leaguepedia.query(
        tables='Leagues',
        fields='League, League_Short, Region, Level, IsOfficial'
    )
    for league in res:
        leagues_table.put_item(Item=transform_ddb_league(league))
    return [league['League'] for league in res]


def transform_ddb_league(league):
    return {
        'leagueId': league['League Short'].replace(' ', '_'),
        'region': league['Region'],
        'isOfficial': league['IsOfficial'].lower() == 'yes',
        'level': league['Level'],
        'leagueName': league['League']
    }


# https://lol.fandom.com/wiki/Special:CargoTables/Tournaments
def load_tourneys_and_return_overview_pages(leagues=None) -> []:
    if leagues is None:
        leagues = load_leagues_and_return_leages()
    size = len(leagues)
    tourneys = []
    for i, league in enumerate(leagues):
        print(f'({i}/{size}) Loading Tournaments for {league}')
        res = leaguepedia.query(
            tables='Tournaments=T,Leagues=L',
            join_on="L.League=T.League",
            fields='T.Name, T.OverviewPage, T.DateStart, T.IsQualifier, T.IsPlayoffs, T.IsOfficial, T.Year, L.League_Short, T.Date',
            where=f"L.League='{league}'"
        )
        res = filter(lambda x: x['Name'], res)
        res = filter(filter_only_recent_tourneys, res)

        for tourney in res:
            tournaments_table.put_item(Item=transform_ddb_tourney(tourney))
            tourneys.append(tourney)
    return tourneys


# Filter out tourneys not from this year
def filter_only_recent_tourneys(tourney):
    try:
        date = datetime.datetime.strptime(tourney['DateStart'], '%Y-%m-%d')
    except (TypeError, ValueError):
        return False

    return date.year == datetime.datetime.now().year


def transform_ddb_tourney(tourney):
    return {
        'leagueId': tourney['League Short'].replace(' ', '_'),
        'tournamentId': tourney['Name'].replace(' ', '_'),
        'startDate': tourney['DateStart'],
        'endDate': tourney['Date'],
        'isOfficial': tourney['IsOfficial'] == '1',
        'isPlayoffs': tourney['IsPlayoffs'] == '1',
        'isQualifier': tourney['IsQualifier'] == '1'
    }


# https://lol.fandom.com/wiki/Special:CargoTables/MatchSchedule
def load_matches(tourneys=None):
    if tourneys is None:
        tourneys = load_tourneys_and_return_overview_pages()

    size = len(tourneys)
    for i, overview_page in enumerate(tourneys):
        name = overview_page['Name']
        print(f'({i}/{size}) Loading games for {name}')
        try:
            res = leaguepedia.query(
                tables='MatchSchedule=MS,Tournaments=T',
                join_on="MS.OverviewPage=T.OverviewPage",
                fields='MS.MatchId, MS.OverviewPage, T.Name, MS.Team1, MS.Team2, MS.Patch, MS.DateTime_UTC, MS.Winner, MS.BestOf',
                where=f"T.Name='{name}'",
                order_by='DateTime_UTC'
            )
        except Exception:
            continue

        res = list(filter(filter_only_recent_matches, res))
        print(f'writing {len(res)} matches')
        for match in res:
            matches_table.put_item(Item=transform_ddb_match(match))


# Adding this filter to reduce the cost of DDB Writes.
def filter_only_recent_matches(match):
    try:
        date = datetime.datetime.strptime(match['DateTime UTC'], '%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError):
        return False
    now = datetime.datetime.now()
    # If the match is older than 12 hours we don't need to update it
    # If the match is farther than 2 weeks in the future don't worry about it
    return now - datetime.timedelta(hours=12) < date < now + datetime.timedelta(weeks=2)


def transform_ddb_match(match):
    return {
        'matchId': match['MatchId'].replace(" ", "_"),
        'tournamentId': match['Name'].replace(' ', '_'),
        'blueTeamId': get_team_code_from_name(match['Team1']),
        'redTeamId': get_team_code_from_name(match['Team2']),
        'winner': get_winner(match),
        'bestOf': match['BestOf'],
        'startTime': transform_datetime_utc(match['DateTime UTC']),
        'patch': match['Patch']
    }


def transform_datetime_utc(date_time):
    try:
        return Decimal(str(datetime.datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S').timestamp() * 1000))
    except (TypeError, ValueError):
        return str(-1)


def get_winner(match):
    if match['Winner'] == '1':
        return get_team_code_from_name(match['Team1'])
    elif match['Winner'] == '2':
        return get_team_code_from_name(match['Team2'])
    else:
        return None


# https://lol.fandom.com/wiki/Special:CargoTables/Players
def load_players():
    res = leaguepedia.query(
        tables='Players',
        fields='ID, Country, Age, Team, Residency, Role, IsSubstitute'
    )
    size = len(res)
    for i, player in enumerate(res):
        print(f'({i}/{size}) Loading player {player["ID"]}')
        player_table.put_item(Item=transform_ddb_player(player))


def transform_ddb_player(player):
    return {
        'id': player['ID'],
        'country': player['Country'],
        'age': int(player['Age'] if player['Age'] else -1),
        'teamId': get_team_code_from_name(player['Team']),
        'residency': player['Residency'],
        'role': player['Role'],
        'isSubstitute': player['IsSubstitute'] == '1'
    }


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
        team_table.put_item(Item=transform_ddb_team(team))


def transform_ddb_team(team):

    team_id = team['Short']

    if team_id == 'MAD' and team['Name'] == 'Mad Revolution Gaming':
        team_id = 'MAD_LAT'

    if team_id == 'INF' and team['Name'] == 'Team Infernal Drake':
        team_id = 'TID'

    if team_id == 'SN' and team['Name'] == 'Supernova':
        team_id = 'SNV'

    if team_id == 'IW' and team['Name'] == '??stanbul Wildcats':
        team['Name'] = 'Istanbul Wildcats'

    if team_id == 'RA' and team['Name'] == 'Redemption Arc':
        team_id = 'RAC'

    if team_id == 'V5' and team['Name'] == 'Vortex Five':
        team_id = 'VF'

    return {
        'teamId': team_id,
        'name': team['Name'],
        'location': team['Location'],
        'region': team['Region'],
        'isDisbanded': team['IsDisbanded'] == '1'
    }


team_code_dict = {}


def get_team_code_from_name(team_name):
    if team_code_dict == {}:
        print('Loading Team Codes into Cache...')
        res = leaguepedia.query(
            tables='Teams',
            fields='Name, Short',
        )
        for team in res:
            team_code_dict[team['Name']] = team
        print(f'Added {len(res)} team codes to the cache')
    try:
        if 'Rogue (European Team)' == team_name:
            return 'RGE'
        elif 'Evil Geniuses.NA' == team_name:
            return 'EG'
        elif 'PEACE (Oceanic Team)' == team_name:
            return 'PCE'
        elif 'RED Kalunga' == team_name:
            return 'RED'
        elif 'Team Infernal Drake' == team_name:
            return 'TID'
        elif 'DAMWON Gaming' == team_name:
            return 'DK'
        elif 'Istanbul Wildcats' == team_name:
            return 'IW'
        elif 'Afreeca Freecs' == team_name:
            return 'KDF'
        elif 'eStar (Chinese Team)' == team_name:
            return 'UP'
        elif 'Vorax Academy' == team_name:
            return 'LBR.A'
        elif 'Mousesports' == team_name:
            return 'MOUZ'
        else:
            return team_code_dict[team_name]['Short']
    except KeyError:
        print(f'Could not find short for {team_name}')
        return team_name
