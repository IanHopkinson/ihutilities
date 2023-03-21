#!/usr/bin/env python
# encoding: utf-8

import operator

from collections import OrderedDict
from typing import Any, List, Optional, Dict
from warnings import warn


def get_with_alts(
    dictionary: dict,
    primary_key: str,
    default_value: Optional[str] = "",
    allow_default: bool = False,
    alternatives: Optional[List] = None,
    deprecated_from_version: str = "2022.12.5",
) -> Any:
    if alternatives is None:
        alternatives = []

    alternatives.extend(
        [
            primary_key,
            primary_key.title(),
            primary_key.lower(),
            primary_key.lower().replace(" ", "_"),
        ]
    )
    value = None
    successful_key = ""
    for key in alternatives:
        if key in dictionary.keys():
            value = dictionary[key]
            successful_key = key
            break

    if successful_key == "" and not allow_default:
        raise KeyError(f"Requested key {primary_key} not available and allow_default set to false")

    if successful_key == "" and allow_default:
        value = default_value

    if successful_key != primary_key and not allow_default:
        warn(
            f"as of version {deprecated_from_version} '{successful_key}' "
            f"is replaced with '{primary_key}'",
            DeprecationWarning,
            stacklevel=2,
        )

    elif successful_key != primary_key and allow_default:
        warn(
            f"as of version {deprecated_from_version} '{primary_key}' " f"is expected",
            DeprecationWarning,
            stacklevel=2,
        )

    return value


def sort_dict_by_value(unordered_dict: Dict) -> Dict:
    sorted_dict = sorted(unordered_dict.items(), key=operator.itemgetter(1))
    return OrderedDict(sorted_dict)
