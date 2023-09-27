#!/usr/bin/env python3

import numpy as np
import pandas as pd

# A seed for reproducible but random results
np.random.seed(0)

CSV_PATH = 'responses.csv'
COLUMNS_FULL = ['Telegram handle', 'First preference', 'Second preference', 'Dance role', 'If I am admitted to my first preference...']
COLUMNS = ['handle', 'first_preference', 'second_preference', 'role', 'only_first_preference']

GROUPS_MAP = {'Level 1 M (Monday)': 'S1M', 'Level 1 T (Tuesday)': 'S1T', 'Level 2': 'S2', 'Level 3': 'S3'}
MAX_PER_GROUP = 15


def main():
    df = pd.read_csv(CSV_PATH, usecols=COLUMNS_FULL)[COLUMNS_FULL]
    df.columns = COLUMNS

    # Column cleaning/creation
    df['1'] = df['first_preference'].map(GROUPS_MAP) + df.role.str.get(0)
    df['2'] = df['second_preference'].map(GROUPS_MAP) + df.role.str.get(0)
    df['only_1'] = df['only_first_preference'] == 'I would still like to join my second preference, if there is enough space'
    df.drop(columns=['first_preference', 'second_preference', 'role', 'only_first_preference'], inplace=True)

    # First preference is required
    df = df[df['1'].notnull()]

    # Create columns for each group
    groups = df['1'].unique().tolist()
    for group in groups:
        df[group] = None

    df = df.sample(frac=1).reset_index(drop=True)
    # At this point we have a dataframe with the following columns:
    # handle = Telegram handle
    # 1 = First preference
    # 2 = Second preference
    # only_1 = Whether the person only wants to join first preference
    # all other columns = Groups that contain their place in the list

    # Assign all first preferences
    for group in groups:
        chose_group = df['1'].eq(group)
        df.loc[chose_group, group] = chose_group.cumsum()

    # Assign all second preference that are not in first preference
    unlucky = df[groups].gt(MAX_PER_GROUP).any(axis=1)
    for group in groups:
        to_assign = df['2'].eq(group) & unlucky
        df.loc[to_assign, group] = to_assign.cumsum() + df[group].max()

    # Assign all remaining second preference
    for group in groups:
        to_assign = df['2'].eq(group) & ~unlucky & ~df['only_1']
        df.loc[to_assign, group] = to_assign.cumsum() + df[group].max()

    for group in groups:
        print(df[df[group].notnull()].sort_values(group)[['handle', group]].reset_index(drop=True).to_string())


if __name__ == '__main__':
    main()
