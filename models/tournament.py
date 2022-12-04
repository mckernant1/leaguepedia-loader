from dataclasses import dataclass


@dataclass
class Tournament:
    leagueId: str
    tournamentId: str
    startDate: str
    endDate: str
    isOfficial: bool
    isPlayoffs: bool
    isQualifier: bool

    def __init__(self, tourney):
        self.leagueId = tourney['League Short'].replace(' ', '_')
        self.tournamentId = tourney['Name'].replace(' ', '_')
        self.startDate = tourney['DateStart']
        self.endDate = tourney['Date']
        self.isOfficial = tourney['IsOfficial'] == '1'
        self.isPlayoffs = tourney['IsPlayoffs'] == '1'
        self.isQualifier = tourney['IsQualifier'] == '1'

    def ddb_format(self):
        return self.__dict__

    def key(self):
        return {
            'leagueId': self.leagueId,
            'tournamentId': self.tournamentId
        }
