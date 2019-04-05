import collections

from .. import pipette
from typing import *

class Match(NamedTuple):
    home_team: str
    away_team: str
    home_goals: int
    away_goals: int

class ExtractMatches(pipette.Task[Iterable[Match]]):
    """A task that reads the raw CSV with match results, and produces Match objects."""

    # When you change the code in a way that would change the output, bump the version.
    VERSION_TAG = "002scores"

    # List the input parameters to the task. When any of these change, the task re-runs.
    INPUTS = {
        "csv_file_path": str
    }

    def do(self, csv_file_path: str):
        home_team_column = None
        away_team_column = None
        home_goal_column = None
        away_goal_column = None

        for line in open(csv_file_path):
            line = line.strip().split(",")
            if line[0] == "Date":
                home_team_column = line.index("HomeTeam")
                away_team_column = line.index("AwayTeam")
                home_goal_column = line.index("FTHG")
                away_goal_column = line.index("FTAG")
                continue
            yield Match(
                line[home_team_column],
                line[away_team_column],
                int(line[home_goal_column]),
                int(line[away_goal_column]))


class GoalsPerTeam(pipette.Task[Dict[str, int]]):
    """A task that counts up the number of goals for each team."""
    VERSION_TAG = "001first"
    INPUTS = {
        "count_home_goals": bool,
        "count_away_goals": bool,
        "matches": pipette.Task[Iterable[Match]]    # You can specify another task as an input like this.
    }
    DEFAULTS = {
        "count_home_goals": True,
        "count_away_goals": True,
        # You could specify another task as a default argument, but we won't do that in the example.
        #"matches": SimplifyTask(csv_file_path="season-1819.csv")
    }

    # The default serialization format uses dill, which can serialize almost anything, but is not
    # human readable. Often, I'd like my data to be human readable, so I choose jsonFormat, or
    # jsonGzFormat for bigger files.
    OUTPUT_FORMAT = pipette.jsonFormat

    def do(self, count_home_goals: bool, count_away_goals: bool, matches: Iterable[Match]):
        # When Pipette calls the do() function, it automatically resolves the Task from the inputs.
        results = collections.Counter()
        for match in matches:
            if count_home_goals:
                results[match.home_team] += match.home_goals
            if count_away_goals:
                results[match.away_team] += match.away_goals
        return results


class WinsPerTeam(pipette.Task[Dict[str, int]]):
    """A task that counts up the number of won matches for each team."""
    VERSION_TAG = "001first"
    INPUTS = {
        "matches": pipette.Task[Iterable[Match]]
    }
    OUTPUT_FORMAT = pipette.jsonFormat

    def do(self, matches: Iterable[Match]):
        results = collections.Counter()
        for match in matches:
            if match.home_goals > match.away_goals:
                results[match.home_team] += 1
            elif match.home_goals < match.away_goals:
                results[match.away_team] += 1
        return results


class PredictNextChampion(pipette.Task[str]):
    """A task that predicts who will be Premier League champion next."""
    VERSION_TAG = "002arsenal"
    INPUTS = {
        "goals_per_team": pipette.Task[Dict[str, int]],
        "wins_per_team": pipette.Task[Dict[str, int]],
        "b": float
    }
    DEFAULTS = {
        "b": 0.37
    }
    OUTPUT_FORMAT = pipette.jsonFormat

    def do(self, goals_per_team: Dict[str, int], wins_per_team: Dict[str, int], b: float):
        results = []
        for team, goals in goals_per_team.items():
            results.append((goals * b + wins_per_team.get(team, 0), team))
        results.sort(reverse=True)
        return results[0][1]


if __name__ == "__main__":
    matches = ExtractMatches(csv_file_path="season-1819.csv")
    gpt = GoalsPerTeam(matches = matches)
    wpt = WinsPerTeam(matches = matches)
    next_champion = PredictNextChampion(goals_per_team=gpt, wins_per_team=wpt, b=0.42)

    import sys
    pipette.main(sys.argv, next_champion)

    # You could also get the results of the task, which will also run all dependent tasks:
    #print(next_champion.results())

    # Print the commands for all tasks, for easy copy and paste:
    #for serialized_command in pipette.to_commands(next_champion):
    #    print(serialized_command)

    # Print the task graph in graphviz format:
    #print(pipette.to_graphviz(next_champion))


