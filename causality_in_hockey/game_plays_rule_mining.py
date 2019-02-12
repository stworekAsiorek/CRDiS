import sys
sys.path.append("../rules_mining")
from rules_miner import RulesMiner
import pandas as pd


def get_team_ids_for_each_gam_id(dataframe):

    game_ids_to_team_ids_mapping = dict()
    game_ids = dataframe.game_id.unique()

    for game_id in game_ids:
        teams_in_that_play = list(dataframe[dataframe.game_id == game_id]["initial_team"].unique())
        game_ids_to_team_ids_mapping[game_id] = dict(zip(teams_in_that_play, ["team no.1", "team no.2"]))

    return game_ids_to_team_ids_mapping


def improve_dataframe_for_further_experiments(dataframe, team_mapping_to_game_ids):

    dataframe["second_play"] = dataframe["second_play"] + " "  + str(dataframe["initial_play"].map(dataframe["game_id"].map(team_mapping_to_game_ids))) if dataframe["second_play"].str.contains("For|Against") else dataframe["second_play"]
    return dataframe


if __name__ == "__main__":

    stoppage_play_links = pd.read_csv("data\stoppage_play_links.csv")

    shortened_stoppage_play_links = stoppage_play_links[stoppage_play_links["n"] == 1]

    shortened_stoppage_play_links.to_csv(path_or_buf="data\shortened_stoppage_play_links.csv")

    teams_in_play = get_team_ids_for_each_gam_id(shortened_stoppage_play_links)
    print(teams_in_play)

    shortened_stoppage_play_links_with_filled_team = improve_dataframe_for_further_experiments(shortened_stoppage_play_links[:50], teams_in_play)

    shortened_stoppage_play_links_with_filled_team.to_csv(path_or_buf="data\shortened_stoppage_play_links_filled_with_team.csv")
    # print(shortened_stoppage_play_links.tail(n=50))
