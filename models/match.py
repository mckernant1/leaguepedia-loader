from dataclasses import dataclass

from util.datetime import transform_datetime_utc
from util.team_info import get_team_code_from_name


@dataclass
class Match:
    matchId: str
    tournamentId: str
    blueTeamId: str
    redTeamId: str
    winner: str
    bestOf: int
    startTime: str
    patch: str
    vod: str
    highlight: str

    def __init__(self, match):
        self.matchId = match['MatchId'].replace(" ", "_")
        self.tournamentId = match['Name'].replace(' ', '_')
        self.blueTeamId = get_team_code_from_name(match['Team1'])
        self.redTeamId = get_team_code_from_name(match['Team2'])
        self.winner = get_winner(match)
        self.bestOf = match['BestOf']
        self.startTime = transform_datetime_utc(match['DateTime UTC'])
        self.patch = match['Patch']
        self.vod = match['VOD']
        self.highlight = match['VodHighlights']

    def ddb_format(self):
        return self.__dict__

    def key(self):
        return {
            'tournamentId': self.tournamentId,
            'matchId': self.matchId
        }


def get_winner(match):
    if match['Winner'] == '1':
        return get_team_code_from_name(match['Team1'])
    elif match['Winner'] == '2':
        return get_team_code_from_name(match['Team2'])
    else:
        return None
