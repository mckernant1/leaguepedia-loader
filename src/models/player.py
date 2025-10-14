from dataclasses import dataclass

from util.team_info import get_team_code_from_name


@dataclass
class Player:
    id: str
    country: str
    age: int
    teamId: str
    residency: str
    role: str
    isSubstitute: bool

    def __init__(self, player):
        self.id = player['ID']
        self.country = player['Country']
        self.age = int(player['Age'] if player['Age'] else -1)
        self.teamId = get_team_code_from_name(player['Team'])
        self.residency = player['Residency']
        self.role = player['Role']
        self.isSubstitute = player['IsSubstitute'] == '1'

    def ddb_format(self):
        return self.__dict__

    def key(self):
        return {
            'teamId': self.teamId,
            'id': self.id
        }
