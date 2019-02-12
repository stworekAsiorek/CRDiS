from pandas_useful_functions import *
import argparse
import pandas as pd
from logger import Logger


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

    random_game_stop_plays = filter_out_events_by_column_content(random_game_stop_plays_all, 'event', ["Faceoff"])


    previous_event_lookup = pd.DataFrame({'game_id': random_game_stop_plays.game_id.values,
                                          # 'next_play_num': [number+1 for number in random_game_stop_plays.play_num.values],
                                          'gameTime': random_game_stop_plays.gameTime.values,
                                          'previous_play_id': random_game_stop_plays.play_id.values,
                                          'previous_event': random_game_stop_plays.event.values,
                                          'previous_event_causer': random_game_stop_plays.team_id_for.values,
                                          'previous_event_description': random_game_stop_plays.description.values,
                                          'num_teams_involved': random_game_stop_plays.team_id_for.values})
    previous_event_lookup = filter_out_events_by_column_content(previous_event_lookup, 'previous_event_description',
                                                                ['TV timeout'])

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

    merge_faceoffs_with_stoppages[['previous_event']] = merge_faceoffs_with_stoppages[['previous_event']].fillna('unknown')
    merge_faceoffs_with_stoppages['previous_stop_reason'] = merge_faceoffs_with_stoppages['previous_event']
    merge_faceoffs_with_stoppages.loc[merge_faceoffs_with_stoppages['previous_event'] == 'Stoppage',
                                      'previous_stop_reason'] = merge_faceoffs_with_stoppages.loc[merge_faceoffs_with_stoppages['previous_event'] == 'Stoppage', 'previous_event_description']

    merge_faceoffs_with_stoppages.loc[(merge_faceoffs_with_stoppages['previous_stop_reason'] == 'Offside') & (merge_faceoffs_with_stoppages['fo_winner_zone'] == 'DEF'), 'fo_winner_zone'] = "NEU-D"
    merge_faceoffs_with_stoppages.loc[(merge_faceoffs_with_stoppages['previous_stop_reason'] == 'Offside') & (merge_faceoffs_with_stoppages['fo_winner_zone'] == 'OFF'), 'fo_winner_zone'] = "NEU-O"

    merge_faceoffs_with_stoppages.loc[(merge_faceoffs_with_stoppages['previous_stop_reason'] == 'Offside') & (merge_faceoffs_with_stoppages['fo_winner_zone'] == 'NEU-D'), 'previous_event_causer'] = merge_faceoffs_with_stoppages.loc[(merge_faceoffs_with_stoppages['previous_stop_reason'] == 'Offside') & (merge_faceoffs_with_stoppages['fo_winner_zone'] == 'NEU-D'), 'team_id_against']
    merge_faceoffs_with_stoppages.loc[(merge_faceoffs_with_stoppages['previous_stop_reason'] == 'Offside') & (merge_faceoffs_with_stoppages['fo_winner_zone'] == 'NEU-O'), 'previous_event_causer'] = merge_faceoffs_with_stoppages.loc[(merge_faceoffs_with_stoppages['previous_stop_reason'] == 'Offside') & (merge_faceoffs_with_stoppages['fo_winner_zone'] == 'NEU-O'), 'team_id_for']

    final_enhanced_stoppage_data = pd.DataFrame({'fo_play_id': merge_faceoffs_with_stoppages.play_id.values,
                                                 'stop_play_id': merge_faceoffs_with_stoppages.previous_play_id.values,
                                                 'stop_reason': merge_faceoffs_with_stoppages.previous_stop_reason.values,
                                                 'stop_causer': merge_faceoffs_with_stoppages.previous_event_causer.values,
                                                 'previous_event': merge_faceoffs_with_stoppages.previous_event.values,
                                                 'fo_winner_after_stoppage': merge_faceoffs_with_stoppages.team_id_for.values,
                                                 'fo_winner_zone': merge_faceoffs_with_stoppages.fo_winner_zone.values})

    return final_enhanced_stoppage_data[final_enhanced_stoppage_data['stop_causer'].notnull()]


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='choose short or original version of data source')
    parser.add_argument('--original', dest='original', action='store_true', help='Use if original data version desired')
    args = parser.parse_args()

    # Loading data
    if args.original is True:
        game_plays = pd.read_csv(FileLocations.GAME_PLAYS)
    else:
        game_plays = pd.read_csv(FileLocations.GAME_PLAYS_SHORT)

    Logger.info('Data file read')

    game_plays = filter_by_date(game_plays, '2017')
    game_plays.to_csv(path_or_buf='data/game_plays_2017.csv')

    filtered_stoppages = filter_stoppage_rows(game_plays)
    filtered_stoppages = add_gameTime_column(filtered_stoppages)
    filtered_stoppages.set_index('game_id')

    Logger.info('Dataframe preprocessed')

    unique_game_ids = filtered_stoppages.game_id.unique()
    previous_event_lookup = pd.DataFrame()
    df_creation = True
    for id in unique_game_ids:
        if df_creation:
            previous_event_lookup = generate_enhanced_stoppage_data(filtered_stoppages, game_id=id)
            df_creation = False
        else:
            next_event_lookup = generate_enhanced_stoppage_data(filtered_stoppages, game_id=id)
            previous_event_lookup = pd.concat([previous_event_lookup, next_event_lookup])
        Logger.info('Stoppage causer and gainer found for id: %s' % id)

    who_caused_stoppage = previous_event_lookup[['stop_play_id', 'stop_reason', 'stop_causer']]

    who_caused_stoppage = filter_events_by_column_content_na(who_caused_stoppage, 'stop_causer')

    unwanted_plays = ["Period Start", "Period Ready", "Period Official", "Game End",
                      #"Stoppage", #do want it this time
                      "Game Scheduled", "Shootout Complete", "Official Challenge",
                      "Game Official", "Early Intermission End", "Early Intermission Start",
                      "Emergency Goaltender"]

    game_plays_short_2017 = filter_out_events_by_column_content(game_plays, 'event', unwanted_plays)[["play_id", "game_id", "event", #"description",
                                                                                                      "team_id_for",
                                                                                                      "period", "periodTime", "dateTime"]]

    # game_plays_short_2017.set_index('play_id')
    game_plays_short_2017 = game_plays_short_2017.merge(who_caused_stoppage, left_on='play_id', right_on='stop_play_id', how='left')
    Logger.info("Merging successed!!!")

    game_plays_short_2017.to_csv(path_or_buf = 'data/game_plays_2017_with_stoppage_data.csv')
    Logger.info("Data saved")