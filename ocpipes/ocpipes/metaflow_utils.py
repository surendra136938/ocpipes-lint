from __future__ import annotations

from functools import partial, wraps

from metaflow import Flow, Run, current
from metaflow.cards import Markdown, Table
from pandas import DataFrame


def profile_memory(f):
    """Metaflow step decorator to monitor peak memory utilization.

    The peak memory usage in MB will be printed after the step, and is persisted to the step artifact as `mem_usage`.
    """

    @wraps(f)
    def func(self, *args, **kwargs):
        from memory_profiler import memory_usage

        self.mem_usage = memory_usage(
            (f, (self, *args), {**kwargs}),
            max_iterations=1,
            max_usage=True,
            interval=0.2,
        )
        print(f"Peak memory usage (in MB): {self.mem_usage:.2f}")

    return func


def profile_memory_by_line(f):
    """Metaflow step decorator to run line-by-line memory profiling.

    The first column represents the line number of the code that has been profiled, the second column (Mem usage) the
    memory usage of the Python interpreter after that line has been executed. The third column (Increment) represents
    the difference in memory of the current line with respect to the last one. The last column (Line Contents) prints
    the code that has been profiled.
    """

    @wraps(f)
    def func(self, *args, **kwargs):
        from memory_profiler import LineProfiler, choose_backend, show_results

        backend = choose_backend("psutil")
        get_prof = partial(LineProfiler, backend=backend)
        show_results_bound = partial(show_results, stream=None, precision=1)
        prof = get_prof()
        prof(f)(self, *args, **kwargs)
        show_results_bound(prof)

    return func


def pip(libraries):
    """NOTE - do NOT use in production.

    Metaflow step decorator that mimics `@conda` but using `pip install` instead of `conda install`.

    If you have both `@conda` and `@pip` decorators, this decorator needs to be place after the `@conda` decorator to
    work correctly.

    To use, put the libraries to install on any step like so:
    ```
    @pip(libraries={"fastcore": "1.3.26", "wandb": ""})
    @retry(times=4)
    @step
    def get_snapshot_data(self):
    ```
    Note, you can use a specific package version or put an empty string to let the pip resolver determine the version.

    This decorator is intended for quick dev iteration only, with any required packages/versions moved to
    our main python environments once refined. For more discussion, see https://github.com/Opcity/ml/pull/497.

    """

    def decorator(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            import os
            import subprocess
            import sys

            IS_AWS_BATCH = os.getenv("AWS_BATCH_JOB_ID", None)

            if IS_AWS_BATCH:
                for library, version in libraries.items():
                    print("Pip Install:", library, version)
                    if version != "":
                        subprocess.run(
                            [
                                sys.executable,
                                "-m",
                                "pip",
                                "install",
                                library + "==" + version,
                            ]
                        )
                    else:
                        subprocess.run(
                            [sys.executable, "-m", "pip", "install", library]
                        )

            return function(*args, **kwargs)

        return wrapper

    return decorator


def enable_decorator(dec, flag: int | bool = True):
    """Decorator to enable or disable decorators by passing in a flag (default enabled). Use 0/False to disable."""

    def decorator(func):
        if flag:
            return dec(func)
        return func

    return decorator


def check_run_contains_any_tags(run: Run, tags: list[list[str]] | None = None) -> bool:
    """Returns True when all the elements (tags) of any of the inner lists of tags are contained in the specified run.

    You are able to pass in multiple lists of strings. For example, you can pass in the following `[["user:jeff",
    "run:test"], ["user:kayla]]` which will return True if the run contains either both `"user:jeff"` and "run:test"
    from the first list or contains `"user:kayla"` from the second list. This function checks all the inner lists of
    tags and returns True if all of the elements of any of the inner lists is contained in the run.

    Args:
        run (metaflow.Run): The metaflow run.
        tags (list[str], optional): The list of tags if the run has will return True.

    Returns:
        bool: True if run contains the tags and otherwise False. Default to False if no tags passed in.

    """
    if not tags:
        tags = []

    return any(all(tag in run.tags for tag in li) for li in tags)


def latest_successful_runs_using_tags(
    flow_name: str,
    # TODO: this could be made list[list[str]] like exclude_tags and use set unions to grab run_ids to build the runs
    tags: list[str] | None = None,
    num_runs: int | None = 1,
    verbose: bool = False,
    exclude_tags: list[list[str]] | None = None,
) -> list[Run]:
    """Returns a list of successful runs up to `num_runs` with specific tags that do not include `exclude_tags`.

    Args:
        flow_name (str): The flow name to grab runs from.
        tags (list[str], optional): The list of tags runs must have. Default None, which returns all runs.
        num_runs (int, optional): The maximum number of successful runs to return. Default to 1.
        verbose (bool): Whether to have verbose output about runs. Default to False.
        exclude_tags (list[list[str]], optional): List of exclusion tags that are excluded from the list of {tags}.
            Default to None, which doesn't exclude any tags. This can be both single tags `[["run:test"]]` and multiple
            tags `[["run:test", "user:jeff"]]` to exclude those runs that have both tags `"run:test"` and `"user:jeff"`,
            not just `"run:test"`. Also, it can include multiple lists of tags to ignore more runs like the following:
            `[["user:jeff", "run:test"], ["user:kayla]]`.

    Returns:
        list[Run]: contains a list of Run metaflow objects containing the desired tags up to {num_runs}. Empty list
            if no runs are found.

    """
    # set empty list to grab all runs
    if not tags:
        tags = []
    runs = list(Flow(flow_name).runs(*tags))
    if verbose:
        None if runs else print(
            "Runs is empty. Specify tags that exist or remove for latest successful run."
        )
    count = 0
    run_successes = []
    for run in runs:
        # grab successful runs that don't contain exclude_tags
        if run.successful and not check_run_contains_any_tags(run, exclude_tags):
            run_successes.append(run)
            count += 1
        # take all successful runs if num_runs is 0 or None (never enter if condition so never break out of looping
        # through runs) or break out when we reach the desired number of successful num_runs
        if num_runs and count == num_runs:
            break
    if verbose:
        None if run_successes else print(
            "Didn't find a successful run. Redefine or remove tags."
        )

    return run_successes


def make_feature_importance_card(feature_importances: DataFrame, card_id: str) -> None:
    """Make Metaflow card that is a table of feature importances and expects Rank (int), Name (str), and Value (float).

    Originally, this was made for catboost where you can call `get_feature_importances` and it returns the rank, feature
    name, and feature importance value sorted from most to least important. This could be modified to work with other
    models if you pass in the same format here of: Rank (int), Feature Names (str), Feature Importances (float).

    Args:
        feature_importances (DataFrame): The dataframe of feature importances.
        card_id (str): The card id to append the feature importances to.
    """
    card_importances = []
    for idx, row in feature_importances.iterrows():
        card_importances.append(
            [
                Markdown(f"{idx + 1:0>2d}"),
                Markdown(f"{row[0]}"),
                Markdown(f"{row[1]:.5f}"),
            ]
        )
    print("Making card for feature importances.")
    current.card[card_id].append(
        Markdown("##  Model feature importances")
    )  # pylint: disable=no-member
    feature_cols = feature_importances.columns
    current.card[card_id].append(  # pylint: disable=no-member
        Table(card_importances, headers=["Rank", feature_cols[0], feature_cols[1]])
    )
