#!/usr/bin/env python3

import os
from typing import Callable

import numpy as np
import pandas as pd

# A seed for reproducible but random results
np.random.seed(0)

CSV_PATH = 'responses.csv'
COLUMNS_FULL = [
    'Telegram handle', 'Full name (first and last name)', 'Email address',
    'First preference', 'First preference dance role',
    'Second preference', 'Second preference dance role',
    'first_only'
]
COLUMNS = [
    'handle', 'name', 'email',
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

ATTENDANCE_COLUMNS_FULL = ['Handle', 'Week 1', 'Week 2', 'Week 3', 'Week 4']
ATTENDANCE_COLUMNS = ['handle', 'week1', 'week2', 'week3', 'week4']

MAX_PER_GROUP = 15


def get_low_prio(handles: pd.Series, attendance_path: str = 'attendance.csv') -> pd.Series:
    attendance_df = pd.read_csv(attendance_path)[ATTENDANCE_COLUMNS_FULL]
    attendance_df.columns = ATTENDANCE_COLUMNS

    # Mark disruptions for no-shows
    attendance_df['disruption'] = attendance_df[['week1', 'week2', 'week3', 'week4']].eq('No show').any(axis=1)
    # Mark disruptions for giving notice twice
    attendance_df['disruption'] |= attendance_df[['week1', 'week2', 'week3', 'week4']].eq('Gave notice').sum(axis=1).ge(2)

    return handles.isin(attendance_df[attendance_df['disruption']].handle)


def get_high_prio(handles: pd.Series) -> pd.Series:
    return handles.isin(pd.read_csv('high_prio.csv').handle)


def assign_spot(df: pd.DataFrame, assign_rule: Callable[[str], pd.Series]):
    for group in df['1'].unique():
        # Find all people that need to be assigned according to the rule
        assignees = assign_rule(group)
        # Assign them a spot in the group starting from the highest number in that group
        df.loc[assignees, group] = assignees.cumsum() + (df[group].max() if df[group].any() else 0)


def main():
    df = pd.read_csv(CSV_PATH)[COLUMNS_FULL]
    df.columns = COLUMNS

    # Column cleaning/creation
    df['1'] = df['first_preference'].map(GROUPS_MAP) + df['first_preference_role'].str.get(0)
    df['2'] = df['second_preference'].map(GROUPS_MAP) + df['second_preference_role'].str.get(0)
    df['2'].replace({np.nan: None}, inplace=True)
    df['only_1'] = df['only_first_preference'].isnull()
    df = df[['handle', 'name', 'email', '1', '2', 'only_1']]

    attendance_files = (f for f in os.listdir() if os.path.isfile(f) and f.startswith('attendance_'))
    df['low_prio'] = False
    for attendance_file in attendance_files:
        df['low_prio'] |= get_low_prio(df['handle'], attendance_file)

    # Give high priority only to those who are not low priority
    df['high_prio'] = get_high_prio(df['handle']) & ~df['low_prio']

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
    # email = Email address
    # high_prio = Whether the person is high priority
    # low_prio = Whether the person is low priority (cannot be both)
    # 1 = First preference
    # 2 = Second preference
    # only_1 = Whether the person only wants to join first preference
    # all other columns = Groups that contain their place in the list

    # Assign high priority first preference
    assign_spot(df, lambda group: df['1'].eq(group) & df['high_prio'])
    # Assign high priority second preference that are not in first preference
    unlucky = df[groups].gt(MAX_PER_GROUP).any(axis=1)
    assign_spot(df, lambda group: df['2'].eq(group) & df['high_prio'] & unlucky)
    # Assign all first preferences
    assign_spot(df, lambda group: df['1'].eq(group) & ~df['high_prio'] & ~df['low_prio'])
    # Assign all second preference that are not in first preference
    unlucky = df[groups].gt(MAX_PER_GROUP).any(axis=1)
    assign_spot(df, lambda group: df['2'].eq(group) & ~df['high_prio'] & ~df['low_prio'] & unlucky)
    # Assign all remaining second preference not in low priority
    assign_spot(df, lambda group: df['2'].eq(group) & df[group].isnull() & ~df['low_prio'] & ~df['only_1'])
    # Assign all remaining first preference
    assign_spot(df, lambda group: df['1'].eq(group) & df[group].isnull())
    # Assign all remaining second preference
    assign_spot(df, lambda group: df['2'].eq(group) & df[group].isnull() & ~df['only_1'])

    accepted = df[groups].le(MAX_PER_GROUP).any(axis=1)
    print('Accepted emails:')
    print(*df[accepted]['email'].unique().tolist(), sep=', ')
    print()
    df.drop('email', axis=1, inplace=True)

    print(df.to_string())


if __name__ == '__main__':
    main()
