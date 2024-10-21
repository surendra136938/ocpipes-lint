from invoke import Context, task

from .lint1 import run as run_lint


@task
def run(c: Context) -> None:
    run_lint(c, False)
