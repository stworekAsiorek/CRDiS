import sys
sys.path.append("../rules_mining")
sys.path.append('../detectors')
sys.path.append('..')

from utils import *
from online_simulator import OnlineSimulator
from adwin_detector import AdwinDetector
from rules_detector import RulesDetector
import pandas as pd


def get_team_ids_for_each_gam_id(dataframe):

    game_ids_to_team_ids_mapping = dict()
    game_ids = dataframe.game_id.unique()

    for game_id in game_ids:
        teams_in_that_play = list(dataframe[dataframe.game_id == game_id]["initial_team"].unique())
        game_ids_to_team_ids_mapping[game_id] = dict(zip(teams_in_that_play, ["team no.1", "team no.2"]))

    return game_ids_to_team_ids_mapping


def improve_dataframe_for_further_experiments(dataframe, team_mapping_to_game_ids):

    dataframe.loc[dataframe["second_play"].str.contains(pat="For|Against"), "second_play"] = dataframe["second_play"] + " " + dataframe.apply(lambda x: team_mapping_to_game_ids[x.game_id][x.initial_team], axis=1)
    return dataframe


def add_goal_signal(dataframe):

    dataframe["goal_or_action"] = "Action"

    dataframe.loc[dataframe["second_play"].str.contains(pat="Goal - For|Goal - Against"), "goal_or_action"] = dataframe["second_play"]

    dataframe.goal_or_action = dataframe.goal_or_action.shift(periods=1, fill_value="Action")

    dataframe = dataframe[~dataframe["second_play"].str.contains(pat="Goal - For|Goal - Against")]

    return dataframe

def leave_only_two_signals_goal_and_play(dataframe):

    new_dataframe = pd.DataFrame({'play': dataframe.second_play.values,
                                  'goal_or_action': dataframe.goal_or_action.values})

    return new_dataframe


if __name__ == "__main__":

    print(sys.path)

    stoppage_play_links = pd.read_csv("data\stoppage_play_links.csv")

    shortened_stoppage_play_links = stoppage_play_links[stoppage_play_links["n"] == 1]

    shortened_stoppage_play_links.to_csv(path_or_buf="data\shortened_stoppage_play_links.csv")

    teams_in_play = get_team_ids_for_each_gam_id(shortened_stoppage_play_links)

    shortened_stoppage_play_links_with_filled_team = improve_dataframe_for_further_experiments(shortened_stoppage_play_links, teams_in_play)

    shortened_stoppage_play_links_with_filled_team_and_goal_signal = add_goal_signal(shortened_stoppage_play_links_with_filled_team)

    shortened_stoppage_play_links_with_filled_team_and_goal_signal.to_csv(path_or_buf="data\shortened_stoppage_play_links_filled_with_team_and_goal_signal.csv")

    shortened_stoppage_play_links_with_filled_team_and_goal_signal_only = leave_only_two_signals_goal_and_play(shortened_stoppage_play_links_with_filled_team_and_goal_signal)

    shortened_stoppage_play_links_with_filled_team_and_goal_signal_only.to_csv(path_or_buf="data\shortened_stoppage_play_links_filled_with_team_and_goal_signal_only.csv")

    seq_1 = np.array(shortened_stoppage_play_links_with_filled_team_and_goal_signal_only['play'])
    seq_2 = np.array(shortened_stoppage_play_links_with_filled_team_and_goal_signal_only['goal_or_action'])

    detector_1 = AdwinDetector(delta=0.01)
    detector_2 = AdwinDetector(delta=0.01)

    rules_detector = RulesDetector(target_seq_index=1)

    simulator = OnlineSimulator(rules_detector,
                                [detector_1, detector_2],
                                [seq_1, seq_2],
                                ['attr_1', 'attr_2'])

    simulator.run(plot=True, detect_rules=True)

    print_rules(simulator.get_rules_sets(), 0.6)
