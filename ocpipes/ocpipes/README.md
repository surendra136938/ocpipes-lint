# Opcity Pipes

Opcity pipes "dev" is where nerds come to plumb data with the best of them.
Shouts to the GOAT.

![Alt text](img/mario.jpg?raw=True "GOAT 1981 - Present")

## Usage

To use `ocpipes`, simply import it in your python environment. The most common sequence
of tasks to get the go-to training data for Opcity is:

```
from ocpipes.db import get_data

query = """
some query
"""

df = get_data(query)

```
