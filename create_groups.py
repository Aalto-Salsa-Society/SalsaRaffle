#!/usr/bin/env python3

import numpy as np
import pandas as pd

# A seed for reproducible but random results
np.random.seed(0)

CSV_PATH = 'responses.csv'
COLUMNS_FULL = [
    'Telegram handle', 'Full name (first and last name)',
    'First preference', 'First preference dance role',
    'Second preference', 'Second preference dance role',
    'first_only'
]
COLUMNS = [
    'handle', 'name',
    'first_preference', 'first_preference_role',
    'second_preference', 'second_preference_role',
    'only_first_preference'
]
GROUPS_MAP = {
    'Salsa Level 1 M (Monday)': 'S1M',
    'Salsa Level 1 T (Tuesday)': 'S1T',
    'Salsa Level 2': 'S2',
    'Salsa Level 3': 'S3',
    'Bachata Level 1': 'B1',
    'Bachata Level 2': 'B2',
}

ATTENDANCE_COLUMNS_FULL = ['Name', 'Week 1', 'Week 2', 'Week 3', 'Week 4']
ATTENDANCE_COLUMNS = ['name', 'week1', 'week2', 'week3', 'week4']

MAX_PER_GROUP = 15


def get_low_prio(names: pd.Series, attendance_path: str = 'attendance.csv') -> pd.Series:
    attendance_df = pd.read_csv(attendance_path)[ATTENDANCE_COLUMNS_FULL]
    attendance_df.columns = ATTENDANCE_COLUMNS

    # Mark disruptions for no-shows
    attendance_df['disruption'] = attendance_df[['week1', 'week2', 'week3', 'week4']].eq('No show').any(axis=1)
    # Mark disruptions for giving notice twice
    attendance_df['disruption'] |= attendance_df[['week1', 'week2', 'week3', 'week4']].eq('Gave notice').sum(axis=1).ge(2)

    return names.isin(attendance_df[attendance_df['disruption']].name)


def get_high_prio(names: pd.Series) -> pd.Series:
    return names.isin(pd.read_csv('high_prio.csv').name)


def main():
    df = pd.read_csv(CSV_PATH)[COLUMNS_FULL]
    df.columns = COLUMNS

    # Column cleaning/creation
    df['1'] = df['first_preference'].map(GROUPS_MAP) + df['first_preference_role'].str.get(0)
    df['2'] = df['second_preference'].map(GROUPS_MAP) + df['second_preference_role'].str.get(0)
    df['2'].replace({np.nan: None}, inplace=True)
    df['only_1'] = df['only_first_preference'].eq('I would still like to join my second preference, if there is enough space')
    df = df[['handle', 'name', '1', '2', 'only_1']]

    df['low_prio'] = get_low_prio(df['name'], 'attendance_bachata.csv')
    df['low_prio'] |= get_low_prio(df['name'], 'attendance_level1M.csv')
    df['low_prio'] |= get_low_prio(df['name'], 'attendance_level1T.csv')
    df['low_prio'] |= get_low_prio(df['name'], 'attendance_level2.csv')
    df['low_prio'] |= get_low_prio(df['name'], 'attendance_level3.csv')

    # Give high priority only to those who are not low priority
    df['high_prio'] = get_high_prio(df['name']) & ~df['low_prio']

    # First preference is required
    df = df[df['1'].notnull()]

    # Create columns for each group
    groups = df['1'].unique().tolist()
    for group in groups:
        df[group] = None

    df = df.sample(frac=1).reset_index(drop=True)
    # At this point we have a dataframe with the following columns:
    # handle = Telegram handle
    # name = Full name
    # high_prio = Whether the person is high priority
    # low_prio = Whether the person is low priority (cannot be both)
    # 1 = First preference
    # 2 = Second preference
    # only_1 = Whether the person only wants to join first preference
    # all other columns = Groups that contain their place in the list

    # Assign high priority first preference
    for group in groups:
        to_assign = df['1'].eq(group) & df['high_prio']
        df.loc[to_assign, group] = to_assign.cumsum()

    # Assign high priority second preference that are not in first preference
    unlucky = df[groups].gt(MAX_PER_GROUP).any(axis=1)
    for group in groups:
        to_assign = df['2'].eq(group) & df['high_prio'] & unlucky
        df.loc[to_assign, group] = to_assign.cumsum() + df[group].max()

    # Assign all first preferences
    for group in groups:
        to_assign = df['1'].eq(group) & ~df['high_prio'] & ~df['low_prio']
        df.loc[to_assign, group] = to_assign.cumsum() + df[group].max()

    # Assign all second preference that are not in first preference
    unlucky = df[groups].gt(MAX_PER_GROUP).any(axis=1)
    for group in groups:
        to_assign = df['2'].eq(group) & unlucky & ~df['high_prio'] & ~df['low_prio']
        df.loc[to_assign, group] = to_assign.cumsum() + df[group].max()

    # Assign all remaining second preference not in low priority
    for group in groups:
        to_assign = df['2'].eq(group) & df[group].isnull() & ~df['low_prio']
        df.loc[to_assign, group] = to_assign.cumsum() + df[group].max()

    # Assign all remaining first preference
    for group in groups:
        to_assign = df['1'].eq(group) & df[group].isnull()
        df.loc[to_assign, group] = to_assign.cumsum() + df[group].max()

    # Assign all remaining second preference
    for group in groups:
        to_assign = df['2'].eq(group) & df[group].isnull()
        df.loc[to_assign, group] = to_assign.cumsum() + df[group].max()

    print(df.to_string())
    for group in groups:
        print(df[df[group].notnull()].sort_values(group)[['handle', group]].reset_index(drop=True).to_string())


if __name__ == '__main__':
    main()
