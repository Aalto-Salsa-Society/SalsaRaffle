# SalsaRaffle

This repository houses the program that allocates students to the different
levels.

## Usage

This project is built with `uv`, which is the recommended way to run it. To
install `uv` run:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Run this project with:

```bash
uv run raffle
```

Check for typing and linting errors respectively:

```bash
uv run pyright
uv run ruff check
```

## Design Decisions

The script divides the students into their respective levels. The highest
priority is given to those who were on the waiting list during the previous
cycle, but only for registrations that took part in the raffle. Following,
priority is given for everyone's first preference, then second preference,
finally late registrations. Late registrations to the previous cycle will not
be prioritized in any way.

Currently the classes are limited to 15 people per role, so 15 leaders and 15
followers. Should the number of registrations for one of the two roles be lower
than the limit, the number of participants for the other role will be capped
accordingly (e.g. if we have registrations from 18 leaders and 13 followers,
the number of leaders will be capped to 13). Exceeding registrations will be
gathered into a waiting list.
