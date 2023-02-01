from pprint import pprint

from leaguepedia_parser.site.leaguepedia import LeaguepediaSite

if __name__ == '__main__':
    lp = LeaguepediaSite()

    res1 = lp.query(
        tables="Leagues=L",
        fields="L.League, L.League_Short",
        where="L.League_Short='SL'"
    )

    for res in res1:
        pprint(res)



def transform_game_details(obj):
    return {
        'gameId': obj['GameId'],
        'matchId': obj['MatchId'],
        'team1': obj['Team1'],
        'team2': obj['Team2'],
        'winner': obj['WinTeam'],
        'startTime': obj['DateTime_UTC'],
        'team1Score': obj['Team1Score'],
        'team2Score': obj['Team2Score'],
        'gameLength': obj['Gamelength'],
        'team1Bans': obj['Team1Bans'],
        'team2Bans': obj['Team2Bans'],
        'team1DragCount': obj['Team1Dragons'],
        'team2DragCount': obj['Team2Dragons'],
        'team1BaronCount': obj['Team1Barons'],
        'team2BaronCount': obj['Team2Barons'],
        'team1TowerCount': obj['Team1Towers'],
        'team2TowerCount': obj['Team2Towers'],
        'team1Gold': obj['Team1Gold'],
        'team2Gold': obj['Team2Gold'],
        'team1Kills': obj['Team1Kills'],
        'team2Kills': obj['Team2Kills'],
        'team1HeraldCount': obj['Team1RiftHeralds'],
        'team2HeraldCount': obj['Team2RiftHeralds'],
        'team1InhibitorCount': obj['Team1Inhibitors'],
        'team2InhibitorCount': obj['Team2Inhibitors'],
    }


def transform_player_details(obj):
    return {
        'id': obj['Link'],
        'champion': obj['Champion'],
        'kills': obj['Kills'],
        'deaths': obj['Deaths'],
        'assists': obj['Assists'],
        'summonerSpells': obj.split(','),
        'gold': obj['Gold'],
        'cs': obj['CS'],
        'damageDelt': obj['DamageToChampions'],
        'visionScore': obj['VisionScore'],
        'items': obj['Items'].split(','),
        'trinket': obj['Trinket'],
        'role': obj['IngameRole'],
        'side': obj['Side'],
        'team': obj['Team']
    }
