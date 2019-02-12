'''pandas_seful_functions.py - Contains functions which makes pandas operations easier and more readable'''

from enum import Enum

def filter_events_by_column_content(dataframe, column, filter):
    is_valid = dataframe[column].isin(filter)
    return dataframe[is_valid]


def filter_out_events_by_column_content(dataframe, column, filter):
    is_valid = dataframe[column].isin(filter)
    return dataframe[~is_valid]


def filter_events_by_column_content_na(dataframe, column):
    is_valid = dataframe[column].isna()
    return dataframe[~is_valid]

def filter_by_date(dataframe, date_start):
    is_valid = dataframe['game_id'].astype(str).str.startswith(date_start)
    return dataframe[is_valid]


def filter_stoppage_rows(dataframe):
    return filter_events_by_column_content(dataframe, 'event', ['Stoppage', 'Faceoff'])


def add_gameTime_column(dataframe):
    for i, row in dataframe.iterrows():
        dataframe.loc[i, 'gameTime'] = (row['period']-1)*20*60 + row['periodTime']
    return dataframe


class FileLocations(Enum):

    HOCKEY_DATA = r"C:\Users\Asia\Documents\Praca magisterska\WSHOP\nhl-game-data"

    HOCKEY_SHORTS_DATA = HOCKEY_DATA + r'\shorts'

    PLOTS_DESTINATION = r"plots"

    GAME_PLAYS = "%s\%s" %(HOCKEY_DATA, "game_plays.csv")

    GAME_PLAYS_SHORT ="%s\%s" %(HOCKEY_SHORTS_DATA, "game_plays_short.csv")