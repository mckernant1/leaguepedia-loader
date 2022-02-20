from data_loading import load_matches, get_team_code_from_name, team_code_dict

if __name__ == '__main__':
    print(get_team_code_from_name('Rouge'))
    print(get_team_code_from_name('Cloud9'))
    print(list(filter(lambda a: 'Rouge' in a, team_code_dict)))

