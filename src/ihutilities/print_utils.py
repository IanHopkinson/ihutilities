#!/usr/bin/env python
# encoding: utf-8

import dataclasses
import math

from typing import Dict, List, Optional


def print_dictionary_comparison(
    dict1: Dict, dict2: Dict, name1: str = "first_dict", name2: str = "second_dict"
) -> None:
    unified_keys = set(list(dict1.keys()))
    unified_keys.update(list(dict2.keys()))
    total_width = 100
    key_width = max(len(x) for x in unified_keys) + 1

    dict_one_width = math.floor((total_width - 4 - key_width) / 2)
    dict_two_width = dict_one_width
    total_width = key_width + dict_one_width + dict_two_width + 4

    print("\n", flush=True)
    print("-" * total_width, flush=True)
    print(
        f"|{'key':<{key_width}}|{name1:<{dict_one_width}}|{name2:<{dict_two_width}}|",
        flush=True,
    )
    print("-" * total_width, flush=True)

    for key in unified_keys:
        print(
            f"|{key:<{key_width}}"
            f"|{str(dict1.get(key, 'Not present')):<{dict_one_width}.{dict_one_width}}"
            f"|{str(dict2.get(key, 'Not present')):<{dict_two_width}.{dict_two_width}}|",
            flush=True,
        )
    print("-" * total_width, flush=True)


def print_dictionary(dict1: Dict) -> None:
    unified_keys = list(dict1.keys())
    total_width = 100
    key_width = max(len(x) for x in unified_keys) + 1

    dict_one_width = math.floor((total_width - 4 - key_width))
    dict_two_width = 0
    total_width = key_width + dict_one_width + dict_two_width + 4

    print("-" * total_width, flush=True)
    print(
        f"|{'key':<{key_width}}|{'value':<{dict_one_width}}|",
        flush=True,
    )
    print("-" * total_width, flush=True)

    for key in unified_keys:
        print(
            f"|{key:<{key_width}}"
            f"|{str(dict1.get(key, 'Not present')):<{dict_one_width}.{dict_one_width}}|",
            flush=True,
        )
    print("-" * total_width, flush=True)


def print_table_from_list_of_dicts(
    column_data_rows: List[Dict],
    excluded_fields: Optional[List] = None,
    included_fields: Optional[List] = None,
    truncate_width: int = 130,
    max_total_width: int = 150,
) -> None:

    if (len(column_data_rows)) == 0:
        return
    if dataclasses.is_dataclass(column_data_rows[0]):
        temp_data = []
        for row in column_data_rows:
            temp_data.append(dataclasses.asdict(row))
        column_data_rows = temp_data

    if excluded_fields is None:
        excluded_fields = []

    if included_fields is None:
        included_fields = list(column_data_rows[0])

    column_table_header_dict = {}
    for field in included_fields:
        widths = [len(str(x[field])) for x in column_data_rows]
        widths.append(len(field))  # .append(len(field))
        max_field_width = max(widths)

        column_table_header_dict[field] = max_field_width + 1
        if max_field_width > truncate_width:
            column_table_header_dict[field] = truncate_width

    total_width = (
        sum(v for k, v in column_table_header_dict.items() if k not in excluded_fields)
        + len(column_table_header_dict)
        - 1
    )

    if total_width > max_total_width:
        print(
            f"\nCalculated total_width of {total_width} "
            f"exceeds proposed max_total_width of {max_total_width}. "
            "The resulting table may be unattractive.",
            flush=True,
        )

    print("-" * total_width, flush=True)

    for k in included_fields:
        if k not in excluded_fields:
            width = column_table_header_dict[k]
            print(f"|{k:<{width}.{width}}", end="", flush=True)
    print("|", flush=True)
    print("-" * total_width, flush=True)

    for row in column_data_rows:
        for k in included_fields:
            value = row[k]

            if k not in excluded_fields:
                width = column_table_header_dict[k]
                print(f"|{str(value):<{width}.{width}}", end="", flush=True)
        print("|", flush=True)

    print("-" * total_width, flush=True)
