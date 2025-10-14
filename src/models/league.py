from dataclasses import dataclass

from models import league_map, league_short_unique


@dataclass
class League:
    leagueId: str
    region: str
    isOfficial: bool
    level: str
    leagueName: str

    def __init__(self, league):
        self.leagueName = league["League"]
        self.leagueId = league["League Short"].replace(" ", "_")
        if self.leagueName in league_map.keys():
            self.leagueId = league_short_unique(self.leagueName)
        self.region = league["Region"]
        self.isOfficial = league["IsOfficial"].lower() == "yes"
        self.level = league["Level"]

    def ddb_format(self):
        return self.__dict__

    def key(self):
        return {"leagueId": self.leagueId}
