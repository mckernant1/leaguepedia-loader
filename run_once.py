from leaguepedia_parser.site.leaguepedia import leaguepedia

from data_loading import load_matches, get_team_code_from_name, team_code_dict, transform_ddb_match, matches_table

if __name__ == '__main__':
    res = leaguepedia.query(
        tables='MatchSchedule=MS,Tournaments=T',
        join_on="MS.OverviewPage=T.OverviewPage",
        fields='MS.MatchId, MS.OverviewPage, T.Name, MS.Team1, MS.Team2, MS.Patch, MS.DateTime_UTC, MS.Winner, MS.BestOf',
        where=f"T.Name='PRM 1st Division 2022 Spring'",
        order_by='DateTime_UTC'
    )

    for match in res:
        matches_table.put_item(Item=transform_ddb_match(match))



