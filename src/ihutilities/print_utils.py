#!/usr/bin/env python
# encoding: utf-8

import dataclasses
import math

from typing import Dict, List, Optional


def print_dictionary_comparison(
    dict1: Dict,
    dict2: Dict,
    name1: str = "first_dict",
    name2: str = "second_dict",
    differences: bool = False,
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
        value1 = dict1.get(key, "Not present")
        value2 = dict2.get(key, "Not present")

        if differences and value1 == value2:
            continue
        else:
            print(
                f"|{key:<{key_width}}"
                f"|{str(value1):<{dict_one_width}.{dict_one_width}}"
                f"|{str(value2):<{dict_two_width}.{dict_two_width}}|",
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


# This function is sort of obselete except it returns the string that it prints.
def pretty_print_dict(dictionary: Dict) -> str:
    # print("Feature names: {}\n".format(feature_names))
    WIDTH = 160
    # find longest feature_name
    max_width = max([len(key) for key in dictionary.keys()]) + 2
    # find out how many of longest feature name fit in 80 characters
    n_columns = math.floor(WIDTH / (max_width + 7))
    # Build format string
    fmt = "%{}s:%3d".format(max_width)
    # feed feature_names into format string
    report = ""
    i = 1
    for key, value in dictionary.items():
        report = report + fmt % (key, value)
        if (i % n_columns) == 0:
            report = report + "\n"
        i = i + 1

    print(report)
    return report


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


def colour_text(text: str, colour: Optional[str] = "red") -> str:
    """
    Decorate a text string with ANSI escape codes for coloured text in bash-like shells

    Args:
        text (str): A list of addresses

    Keyword arguments:
        colour (str): the required colour (currently supported: red, green, blue, cyan,
        white, yellow, magenta, grey, black)

    Returns:
        coloured_text (str): a dictionary containing the answers

    """
    # Long list of colours/
    # https://stackoverflow.com/questions/15580303/python-output-complex-line-with-floats-colored-by-value
    # https://github.com/ryanoasis/public-bash-scripts/blob/master/unix-color-codes.sh
    prefix_set = {}

    prefix_set["red"] = "\033[91m"
    prefix_set["green"] = "\033[92m"
    prefix_set["blue"] = "\033[94m"
    prefix_set["cyan"] = "\033[96m"
    prefix_set["white"] = "\033[97m"
    prefix_set["yellow"] = "\033[93m"
    prefix_set["magenta"] = "\033[95m"
    prefix_set["grey"] = "\033[90m"
    prefix_set["black"] = "\033[30m"
    prefix_set["default"] = "\033[99m"

    prefix_set["light_red"] = "\033[31m"
    prefix_set["light_green"] = "\033[32m"
    prefix_set["light_yellow"] = "\033[33m"
    prefix_set["light_blue"] = "\033[34m"
    prefix_set["light_magenta"] = "\033[35m"
    prefix_set["light_cyan"] = "\033[36m"
    prefix_set["light_white"] = "\033[37m"

    prefix = prefix_set.get(colour, prefix_set["default"])

    suffix = "\033[0m"

    coloured_text = prefix + text + suffix

    return coloured_text
