from argparse import ArgumentParser

from data_loading import load_players, \
    load_teams, \
    load_leagues_and_return_leages, \
    load_matches, \
    load_tourneys_and_return_overview_pages


def get_arg_parser() -> ArgumentParser:
    parser = ArgumentParser()
    subparsers = parser.add_subparsers()

    leagues = subparsers.add_parser('leagues')
    leagues.set_defaults(func=load_leagues_and_return_leages)

    tourneys = subparsers.add_parser('tourneys')
    tourneys.set_defaults(func=load_tourneys_and_return_overview_pages)

    games = subparsers.add_parser('matches')
    games.set_defaults(func=load_matches)

    players = subparsers.add_parser('players')
    players.set_defaults(func=load_players)

    teams = subparsers.add_parser('teams')
    teams.set_defaults(func=load_teams)

    return parser


if __name__ == '__main__':
    cmd_parser = get_arg_parser()
    cmd = cmd_parser.parse_args()

    if not hasattr(cmd, 'func'):
        cmd_parser.print_help()
    else:
        cmd.func()
