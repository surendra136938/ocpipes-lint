import os
from typing import List

from invoke import Context, task

lint_files = "ocpipes"
binary_extensions = ["zip", "gz", "tar", "png", "gif", "jpg", "pyc", "ipynb", "parquet"]


@task(help={"fix": "instruct Black to automatically fix errors that it finds"})
def black(c: Context, fix: bool = False) -> None:
    """
    Run the Black formatter. Be default this will run in "check mode" and provide output. To automatically fix errors
    found by Black, set --fix
    example Usage:
    invoke lint.black --fix
    """
    if fix:
        c.run(f"python3 -m black {lint_files}")
    else:
        c.run(f"python3 -m black --check {lint_files}")


@task
def flake8(c: Context) -> None:
    """
    Run the Flake8 linter. Flake8 discovers common mistakes that can lead to unmaintainable code. Flake8 will not
    automatically fix the errors, it will list the file and line number where the errors are located and a brief
    description.
    """
    c.run(f"python3 -m flake8 {lint_files}")

    
@task(help={"fix": "Automatically fix missing EOF newlines"})
def eof_newline(c: Context, fix: bool = False) -> None:
    """
    Checks if there is a Newline at the end of a file
    """
    files_without_newlines: List[str] = []

    for path, dirs, files in os.walk("."):
        files = [
            f
            for f in files
            if not f[0] == "." and (f.split(".")[-1] not in binary_extensions)
        ]
        dirs[:] = [
            d
            for d in dirs
            if not d[0] == "."
            and not d.startswith("__")
            and not d == "lib"
            and not d == "node_modules"
            and not d == "jenkins-deploy"
            and not d.endswith(".egg-info")
        ]

        for file in files:
            with open(f"{path}/{file}", "rb+") as fh:
                try:
                    fh.seek(-1, os.SEEK_END)
                except OSError:
                    continue
                last_character = fh.read(1)
                if last_character != b"\n" and last_character != b"":
                    files_without_newlines.append(f"{path}/{file}")
                    if fix:
                        fh.seek(0, os.SEEK_END)
                        fh.write(b"\n")

    if len(files_without_newlines) != 0:
        print(
            f"The following {len(files_without_newlines)} files do not end in a new line:"
        )
        print("\n".join(files_without_newlines))
        exit(1)


@task(
    help={
        "directory": "Specify directory to lint",
    }
)
def ruff(c: Context, directory: str = "ocpipes") -> None:
    """
    Run the Ruff linter for the ocpipes directory. This will run in check mode based on the pyproject.toml
    file settings in the ocpipes directory.
    Example usage:
    invoke lint.ruff --directory <your_directory>
    """
    c.run(f"python3 -m ruff check {directory}")


@task(
    help={"fix": "instruct linting tools to automatically fix errors that they finds"}
)
def run(c: Context, fix: bool = False) -> None:
    """
    Run all linting tools. Defaults to checking for errors only. To automatically fix errors use `--fix`.
    NOTE: Not all tools support automatically fixing errors.
    """
    black(c, fix)
    flake8(c)
    ruff(c, directory)
    eof_newline(c, fix)