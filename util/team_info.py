from logging import Logger

from leaguepedia.leaguepedia import leaguepedia

team_code_dict = {}

logger = Logger(__name__)


def get_team_code_from_name(team_name):
    if team_code_dict == {}:
        logger.debug('Loading Team Codes into Cache...')
        res = leaguepedia.query(
            tables='Teams',
            fields='Name, Short',
        )
        for team in res:
            team_code_dict[team['Name']] = team
        logger.debug(f'Added {len(res)} team codes to the cache')
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
        logger.debug(f'Could not find short for {team_name}')
        return team_name
