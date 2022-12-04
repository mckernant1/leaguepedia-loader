from dataclasses import dataclass


@dataclass
class League:
    leagueId: str
    region: str
    isOfficial: bool
    level: str
    leagueName: str

    def __init__(self, league):
        self.leagueId = league['League Short'].replace(' ', '_')
        self.region = league['Region']
        self.isOfficial = league['IsOfficial'].lower() == 'yes'
        self.level = league['Level']
        self.leagueName = league['League']

    def ddb_format(self):
        return self.__dict__

    def key(self):
        return {
            'leagueId': self.leagueId
        }
