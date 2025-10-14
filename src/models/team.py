from dataclasses import dataclass


@dataclass
class Team:
    teamId: str
    name: str
    location: str
    region: str
    isDisbanded: bool

    def __init__(self, team):
        self.teamId = swap_problematic_team_ids(team)
        self.name = team["Name"]
        self.location = team["Location"]
        self.region = team["Region"]
        self.isDisbanded = team["IsDisbanded"] == "1"

    def ddb_format(self):
        return self.__dict__

    def key(self):
        return {"teamId": self.teamId}


def swap_problematic_team_ids(team):
    team_id = team["Short"]

    if team_id == "MAD" and team["Name"] == "Mad Revolution Gaming":
        team_id = "MAD_LAT"

    if team_id == "INF" and team["Name"] == "Team Infernal Drake":
        team_id = "TID"

    if team_id == "SN" and team["Name"] == "Supernova":
        team_id = "SNV"

    if team_id == "IW" and team["Name"] == "İstanbul Wildcats":
        team["Name"] = "Istanbul Wildcats"

    if team_id == "RA" and team["Name"] == "Redemption Arc":
        team_id = "RAC"

    if team_id == "V5" and team["Name"] == "Vortex Five":
        team_id = "VF"

    return team_id
