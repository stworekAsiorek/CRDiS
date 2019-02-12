import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math
from logger import Logger
from pandas_useful_functions import FileLocations


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
    plt.savefig(FileLocations.PLOTS_DESTINATION + '\event_types_in_game_plays.png')


if __name__ == "__main__":

    game_plays = pd.read_csv(FileLocations.GAME_PLAYS)

    Logger.info('Data file read')

    # Plotting events types
    plot_all_events_and_their_number(game_plays)

    Logger.info("PLOT created")
