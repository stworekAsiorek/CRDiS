import sys
sys.path.append("../rules_mining")
sys.path.append('../detectors')
sys.path.append('..')

from utils import *
from online_simulator import OnlineSimulator
from adwin_detector import AdwinDetector
from rules_detector import RulesDetector
from zscore_detector import ZScoreDetector
import pandas as pd
from sklearn.preprocessing import LabelEncoder


def return_other_key_from_double_pair_key(dict, key):
    x = [item for item in dict.keys() if item != key]
    return x[0]

def get_team_ids_for_each_gam_id(dataframe):

    game_ids_to_team_ids_mapping = dict()
    game_ids = dataframe.game_id.unique()

    for game_id in game_ids:
        teams_in_that_play = list(dataframe[dataframe.game_id == game_id]["initial_team"].unique())
        game_ids_to_team_ids_mapping[game_id] = dict(zip(teams_in_that_play, ["team no.1", "team no.2"]))

    return game_ids_to_team_ids_mapping


def improve_dataframe_for_further_experiments(dataframe, team_mapping_to_game_ids):
    dataframe.loc[dataframe["chain_end"].str.contains(pat=" - For"), "chain_end"] = dataframe["chain_end"] + " " + dataframe.apply(lambda x: team_mapping_to_game_ids[x.game_id][x.initial_team], axis=1)
    dataframe.loc[dataframe["chain_end"].str.contains(pat=" - Against"), "chain_end"] = dataframe["chain_end"].str.replace(" - Against", " - For") + " " + dataframe.apply(lambda x: team_mapping_to_game_ids[x.game_id][return_other_key_from_double_pair_key(team_mapping_to_game_ids[x.game_id], x.initial_team)], axis=1)
    dataframe.loc[dataframe["second_play"].str.contains(pat="For|Against"), "second_play"] = dataframe["second_play"] + " " + dataframe.apply(lambda x: team_mapping_to_game_ids[x.game_id][x.initial_team], axis=1)
    return dataframe


def remove_goal_signal_from_second_play(dataframe):

    dataframe = dataframe[~dataframe["second_play"].str.contains(pat="Goal - For|Goal - Against")]

    return dataframe

def leave_only_two_signals_chain_end_and_play(dataframe):

    new_dataframe = pd.DataFrame({'play': dataframe.second_play.values,
                                  'chain_end': dataframe.chain_end.values})

    return new_dataframe


if __name__ == "__main__":

    stoppage_play_links = pd.read_csv("data\stoppage_play_links.csv")

    shortened_stoppage_play_links = stoppage_play_links[stoppage_play_links["n"] == 1]

    shortened_stoppage_play_links.to_csv(path_or_buf="data\shortened_stoppage_play_links.csv")

    teams_in_play = get_team_ids_for_each_gam_id(shortened_stoppage_play_links)

    shortened_stoppage_play_links_with_filled_team = improve_dataframe_for_further_experiments(shortened_stoppage_play_links, teams_in_play)

    shortened_stoppage_play_links_with_filled_team_and_goal_signal = remove_goal_signal_from_second_play(shortened_stoppage_play_links_with_filled_team)

    shortened_stoppage_play_links_with_filled_team_and_goal_signal.to_csv(path_or_buf="data\shortened_stoppage_play_links_filled_with_team_and_goal_signal.csv")

    shortened_stoppage_play_links_with_filled_team_and_goal_signal_only = leave_only_two_signals_chain_end_and_play(shortened_stoppage_play_links_with_filled_team_and_goal_signal)

    shortened_stoppage_play_links_with_filled_team_and_goal_signal_only.to_csv(path_or_buf="data\shortened_stoppage_play_links_filled_with_team_and_goal_signal_only.csv")

    # Begginnig of rules detection:
    df = shortened_stoppage_play_links_with_filled_team_and_goal_signal_only[:6000]

    df = pd.read_csv("data\shortened_stoppage_play_links_filled_with_team_and_goal_signal_only.csv")[:10000]
    seq_names = ['chain_end_code', 'play_code']

    lb = LabelEncoder()
    df["chain_end_code"] = lb.fit_transform(df["chain_end"])


    with open("encoding/encoding.txt", "w") as encoding:
        le_name_mapping = dict(zip(lb.classes_, lb.transform(lb.classes_)))
        print(le_name_mapping)
        encoding.write(str(le_name_mapping))
        encoding.write("\n")
        df["play_code"] = lb.fit_transform(df["play"])
        le_name_mapping = dict(zip(lb.classes_, lb.transform(lb.classes_)))
        print(le_name_mapping)
        encoding.write(str(le_name_mapping))

    base_seqs = []

    for name in seq_names:
        base_seqs.append(np.array(df[name]))

    sequences = [[] for i in range(len(base_seqs))]
    for nr in range(1):
        for i, seq in enumerate(sequences):
            sequences[i] = np.concatenate((seq, base_seqs[i]))

    win_size = 20

    detector1 = ZScoreDetector(window_size=30, threshold=5.0)
    detector2 = ZScoreDetector(window_size=5, threshold=0.005)

    target_seq_index = 1

    rules_detector = RulesDetector(target_seq_index=target_seq_index,
                                   window_size=100,
                                   round_to=50,
                                   type="closed",
                                   combined=False)

    simulator = OnlineSimulator(rules_detector,
                                [detector1, detector2],
                                sequences,
                                seq_names)

    simulator.label_encoder = lb

    simulator.run(detect_rules=True)

    discovered_rules = simulator.get_rules_sets()
    print(len(discovered_rules))

    print("***********************************************************************")
    print("***********************************************************************")
    print("***********************************************************************")
    print("***********************************************************************")
    print("***********************************************************************")
    print("***********************************************************************")


    print_rules(discovered_rules, support=40)
    # print_best_rules(discovered_rules)

