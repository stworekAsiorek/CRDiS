import pandas as pd
import numpy as np
from logger import Logger
import datetime


def figure_out_if_for_or_not(game_plays_w_next_plays):
    max_chain_depth = 39
    max_chain_duration = 60*2
    whistle_plays = ["Goal", "Period End"]

    seq_end = ["Goal - For", "Goal - Against", "Period End",
               "(no end in time)"]
    next_game_play = game_plays_w_next_plays.copy(deep=True)
    whistle_plays_2 = whistle_plays + next_game_play.stop_reason.dropna().unique()

    for play_n in range(1, max_chain_depth+1):

        Logger.info("play_n: %s" % (play_n))
        if play_n == 1:
            game_plays_w_next_plays['n_play_1'] = game_plays_w_next_plays['event']
            game_plays_w_next_plays['n_period_1'] = game_plays_w_next_plays['period']
        else:
            next_game_play = next_game_play[1:].append(pd.Series([np.nan]), ignore_index=True)

            game_plays_w_next_plays["".join(["n_use_", str(play_n)])] = (game_plays_w_next_plays.game_id == next_game_play.game_id) & \
                                                                        (game_plays_w_next_plays["".join(['n_period_', str(play_n - 1)])] == next_game_play.period) & \
                                                                        (next_game_play.periodTime - game_plays_w_next_plays.periodTime <= max_chain_duration) & \
                                                                        (~game_plays_w_next_plays["".join(['n_play_', str(play_n - 1)])].isin(whistle_plays_2))

            game_plays_w_next_plays.loc[game_plays_w_next_plays["".join(["n_use_", str(play_n)])].isna(), "".join(["n_use_", str(play_n)])] = False

            game_plays_w_next_plays.loc[game_plays_w_next_plays["".join(["n_use_", str(play_n)])] == True, "".join(["n_play_", str(play_n)])] = next_game_play.loc[game_plays_w_next_plays["".join(["n_use_", str(play_n)])] == True, "event"]

            game_plays_w_next_plays["".join(["n_period_", str(play_n)])] = next_game_play.period

            game_plays_w_next_plays.loc[(game_plays_w_next_plays["".join(["n_play_", str(play_n)])].isna() | game_plays_w_next_plays.team_id_for.isna() | next_game_play.team_id_for.isna()), "".join(["n_play_", str(play_n)])] = game_plays_w_next_plays["".join(["n_play_", str(play_n)])]

            game_plays_w_next_plays.loc[(game_plays_w_next_plays.team_id_for == next_game_play.team_id_for) &
                                        game_plays_w_next_plays["".join(["n_play_", str(play_n)])].notna() &
                                         game_plays_w_next_plays.team_id_for.notna() &
                                         next_game_play.team_id_for.notna(), "".join(
                ["n_play_", str(play_n)])] = game_plays_w_next_plays["".join(["n_play_", str(play_n)])] + " - For"

            game_plays_w_next_plays.loc[(game_plays_w_next_plays.team_id_for != next_game_play.team_id_for) &
                                        game_plays_w_next_plays["".join(["n_play_", str(play_n)])].notna() &
                                         game_plays_w_next_plays.team_id_for.notna() &
                                         next_game_play.team_id_for.notna(), "".join(
                ["n_play_", str(play_n)])] = game_plays_w_next_plays["".join(["n_play_", str(play_n)])] + " - Against"

    stoppage_game_plays = game_plays_w_next_plays.copy(deep=True)

    for play_n in range(2, max_chain_depth+1):
        Logger.info("play_n: %s" % (play_n))

        stoppage_game_plays.loc[stoppage_game_plays["".join(["n_play_", str(play_n - 1)])].isna(), "".join(["n_play_", str(play_n)])] = np.nan
        stoppage_game_plays.loc[stoppage_game_plays["".join(["n_play_", str(play_n - 1)])].isin(seq_end), "".join(["n_play_", str(play_n)])] = np.nan

    stoppage_game_plays["chain_end"] = stoppage_game_plays["".join(["n_play_", str(max_chain_depth)])]

    for play_n in range(max_chain_depth-1, -1, -1):

        Logger.info("play_n: %s" %(play_n))
        pl_nm1s_wNA = stoppage_game_plays["".join(["n_play_", str(play_n + 1)])]

        pl_nm1s_wNA = pl_nm1s_wNA[stoppage_game_plays.chain_end.isna()]

        pl_nm1s_wNA.loc[~(pl_nm1s_wNA.isin(seq_end) | pl_nm1s_wNA.isna())] = "(no end in time)"

        stoppage_game_plays.loc[stoppage_game_plays['chain_end'].isna(), "chain_end"] = pl_nm1s_wNA

    stoppage_game_plays = stoppage_game_plays[stoppage_game_plays.team_id_for.notna()]

    stoppage_game_plays.loc[~stoppage_game_plays.chain_end.isin(seq_end), "chain_end"] = "(play continues)"

    stoppage_play_links = pd.DataFrame()

    for play_n in range(2, max_chain_depth+1):

        Logger.info("play_n: %s" % (play_n))

        this_batch = pd.DataFrame({"game_id": stoppage_game_plays.game_id.values,
                                   "initial_play": stoppage_game_plays.n_play_1.values,
                                   "second_play": stoppage_game_plays.n_play_2.values,
                                   "chain_end": stoppage_game_plays.chain_end,
                                   "initial_team": stoppage_game_plays.team_id_for.values,
                                   "n": play_n-1,
                                   "play": stoppage_game_plays["".join(["n_play_", str(play_n-1)])].values,
                                   "n_play" : stoppage_game_plays["".join(["n_play_", str(play_n)])].values})

        this_batch['initial_play'] = this_batch.initial_play.str.replace("\\s-\\s.*$", "")
        this_batch = this_batch[this_batch["n_play"].notna()]
        this_batch.groupby(by=["initial_play", "second_play", "chain_end", "initial_team", "n", "play", "n_play"])\
            .size().reset_index(name='counts')

        if ( this_batch.empty ):
            Logger.warning("Batch %s is empty" %play_n)
        else:
            stoppage_play_links = stoppage_play_links.append(this_batch)

    return stoppage_play_links


if __name__ == "__main__":

    games_plays_short_2017 = pd.read_csv('data/game_plays_2017_with_stoppage_data.csv')
    Logger.info("Data loaded")

    games_plays_short_2017.loc[games_plays_short_2017["stop_causer"].notna(), 'team_id_for'] = \
        games_plays_short_2017.loc[games_plays_short_2017["stop_causer"].notna(), 'stop_causer']

    games_plays_short_2017['dateTime'] = pd.to_datetime(games_plays_short_2017['dateTime'], format='%Y-%m-%d %H:%M:%S')
    games_plays_short_2017.loc[games_plays_short_2017["event"] == 'Stoppage', 'dateTime'] = \
        games_plays_short_2017.loc[games_plays_short_2017["event"] == 'Stoppage', 'dateTime'] - datetime.\
            timedelta(seconds=0.1)

    games_plays_short_2017.loc[games_plays_short_2017["stop_reason"].notna(), 'event'] = \
        games_plays_short_2017.loc[games_plays_short_2017["stop_reason"].notna(), 'stop_reason']

    games_plays_short_2017.sort_values(by=['game_id', 'period', 'periodTime', 'dateTime'])

    games_plays_short_2017.to_csv(path_or_buf='data\game_plays_2017_with_stoppage_data_2.csv')

    stoppage_play_links = figure_out_if_for_or_not(games_plays_short_2017)

    stoppage_play_links.to_csv(path_or_buf='data\stoppage_play_links.csv')