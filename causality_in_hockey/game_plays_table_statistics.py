import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math
import argparse

HOCKEY_DATA = r"C:\Users\Asia\Documents\Praca magisterska\WSHOP\nhl-game-data"

HOCKEY_SHORTS_DATA = HOCKEY_DATA + r'\shorts'

PLOTS_DESTINATION = r"plots"

GAME_PLAYS = "%s\%s" %(HOCKEY_DATA, "game_plays.csv")

GAME_PLAYS_SHORT = "%s\%s" %(HOCKEY_SHORTS_DATA, "game_plays_short.csv")


def count_events_types(dataframe, column):
    event_type_dict = dict()
    for _, row in dataframe.iterrows():
        event_type = row[column]
        if event_type in event_type_dict:
            event_type_dict[event_type][0] += 1
        else:
            event_type_dict[event_type] = [1, check_if_values_in_columns_defined(row, "team_id_for", "team_id_against")]
    return event_type_dict


def check_if_values_in_columns_defined(row, *args):
    for column in args:
        if math.isnan(row[column]):
            return False
    return True


def get_blue_if_team_specified_red_otherwise(x):
    if x:
        return 'b'
    else:
        return 'r'


def plot_all_events_and_their_number(dataframe):
    event_type_dict = count_events_types(dataframe=dataframe, column="event")

    event_types = event_type_dict.keys()
    print(event_types)
    event_count_and_team_specified = event_type_dict.values()
    event_numbers = []
    event_colors = []

    for item in event_count_and_team_specified:
        event_numbers.append(item[0])
        event_colors.append(item[1])

    event_colors = [get_blue_if_team_specified_red_otherwise(color) for color in event_colors]

    x = np.arange(len(event_types))
    plt.figure(figsize=(25, 17))
    plt.bar(x, event_numbers, tick_label=list(event_types), color=event_colors)
    plt.xticks(rotation='vertical')
    plt.savefig(PLOTS_DESTINATION + '\event_types_in_game_plays.png')


def filter_events_by_column_content(dataframe, column, filter):
    is_valid = dataframe[column].isin(filter)
    return dataframe[is_valid]


def filter_out_events_by_column_content(dataframe, column, filter):
    is_valid = dataframe[column] != filter
    return dataframe[is_valid]

def filter_events_by_column_content_na(dataframe, column):
    is_valid = dataframe[column].isna()
    return dataframe[is_valid]


def filter_by_date(dataframe, date_start):
    is_valid = dataframe['game_id'].astype(str).str.startswith(date_start)
    return dataframe[is_valid]


def filter_stoppage_rows(dataframe):
    return filter_events_by_column_content(dataframe, 'event', ['Stoppage', 'Faceoff'])


def add_gameTime_column(dataframe):
    for i, row in dataframe.iterrows():
        dataframe.loc[i, 'gameTime'] = (row['period']-1)*20*60 + row['periodTime']
    return dataframe


def add_fo_winner_zone(dataframe):
    for i, row in dataframe.iterrows():
        if dataframe.loc[i,'st_x'] == -69:
            dataframe.loc[i, 'fo_winner_zone'] = "DEF"
        elif dataframe.loc[i,'st_x'] == -20:
            dataframe.loc[i, 'fo_winner_zone'] = "NEU-D"
        elif dataframe.loc[i, 'st_x'] == -0:
            dataframe.loc[i, 'fo_winner_zone'] = "NEU-C"
        elif dataframe.loc[i, 'st_x'] == 20:
            dataframe.loc[i, 'fo_winner_zone'] = "NEU-O"
        elif dataframe.loc[i, 'st_x'] == 69:
            dataframe.loc[i, 'fo_winner_zone'] = "OFF"

    return dataframe


def generate_enhanced_stoppage_data(dataframe, game_id):
    random_game_stop_plays_all = filter_events_by_column_content(dataframe, 'game_id', [game_id])

    random_game_stop_plays = filter_out_events_by_column_content(random_game_stop_plays_all, 'event', "Faceoff")


    previous_event_lookup = pd.DataFrame({'game_id': random_game_stop_plays.game_id.values,
                                          # 'next_play_num': [number+1 for number in random_game_stop_plays.play_num.values],
                                          'gameTime': random_game_stop_plays.gameTime.values,
                                          'previous_play_id': random_game_stop_plays.play_id.values,
                                          'previous_event': random_game_stop_plays.event.values,
                                          'previous_event_causer': random_game_stop_plays.team_id_for.values,
                                          'previous_event_description': random_game_stop_plays.description.values,
                                          'num_teams_involved': random_game_stop_plays.team_id_for.values})
    previous_event_lookup = filter_out_events_by_column_content(previous_event_lookup, 'previous_event_description',
                                                                'TV timeout')

    something = lambda x: len(set(x))
    first_item = lambda x: x.iloc[0]
    previous_event_lookup = previous_event_lookup.groupby(by=['game_id',
                                                              'gameTime']).agg({'num_teams_involved': 'count',
                                                                                'previous_event_causer': first_item,
                                                                                'previous_play_id': first_item,
                                                                                'previous_event': first_item,
                                                                                'previous_event_description': lambda x: "%s" % '; '.join(x)})

    previous_event_lookup.loc[previous_event_lookup['num_teams_involved'] == 2, 'previous_event_causer'] = 'both'

    random_game_faceoffs = filter_events_by_column_content(random_game_stop_plays_all, 'event', ['Faceoff'])
    random_game_faceoffs = add_fo_winner_zone(random_game_faceoffs)

    merge_faceoffs_with_stoppages = pd.merge(random_game_faceoffs, previous_event_lookup, how='left', on=['game_id', 'gameTime'])

    # print(previous_event_lookup)

    merge_faceoffs_with_stoppages[['previous_event']] = merge_faceoffs_with_stoppages[['previous_event']].fillna('unknown')
    merge_faceoffs_with_stoppages['previous_stop_reason'] = merge_faceoffs_with_stoppages['previous_event']
    merge_faceoffs_with_stoppages['previous_stop_reason'] = merge_faceoffs_with_stoppages[merge_faceoffs_with_stoppages['previous_event'] == 'Stoppage']['previous_event_description']


    merge_faceoffs_with_stoppages.loc[(merge_faceoffs_with_stoppages['previous_stop_reason'] == 'Offside') & (merge_faceoffs_with_stoppages['fo_winner_zone']=='DEF'), 'fo_winner_zone'] = "NEU-D"
    merge_faceoffs_with_stoppages.loc[(merge_faceoffs_with_stoppages['previous_stop_reason'] == 'Offside') & (merge_faceoffs_with_stoppages['fo_winner_zone']=='OFF'), 'fo_winner_zone'] = "NEU-O"



    merge_faceoffs_with_stoppages['previous_event_causer']

    # print(list(merge_faceoffs_with_stoppages))
    print(merge_faceoffs_with_stoppages['previous_stop_reason'])

    return previous_event_lookup


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='choose short or original version of data source')
    parser.add_argument('--original', dest='original', action='store_true', help='Use if original data version desired')
    args = parser.parse_args()

    # Loading data
    if args.original is True:
        game_plays = pd.read_csv(GAME_PLAYS)
    else:
        game_plays = pd.read_csv(GAME_PLAYS_SHORT)

    # Plotting events types
    # plot_all_events_and_their_number(game_plays)

    game_plays = filter_by_date(game_plays, '2017')

    filtered_stoppages = filter_stoppage_rows(game_plays)

    filtered_stoppages = add_gameTime_column(filtered_stoppages)

    # filtered_stoppages.to_csv(path_or_buf='data/game_plays_stopagges.csv')

    filtered_stoppages.set_index('game_id')

    previous_event_lookup = generate_enhanced_stoppage_data(filtered_stoppages, game_id='2017020735')

