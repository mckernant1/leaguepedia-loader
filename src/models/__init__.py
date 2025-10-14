
# For duplicate league names we rename
league_map = {
    'LVP SuperLiga': 'LVPSL',
    'Claro Stars League': 'CSL'
}


def league_short_unique(name: str):
    return league_map[name]
