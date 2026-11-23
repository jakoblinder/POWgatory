"""
The contributions whose grids POWHEG combines into fullgrid files at the start of stage 3.

In stage 3, bbinit.f (POWHEG-BOX-RES) goes through the contributions btilde, remn and reg. For each
one that is switched on, a seed combines the stage-2 grids into a fullgrid file, or loads that
file if another seed has written it already. This module evaluates the same switches
(bbinit.f, pwhg_init.f) from a powheg.input file.

Whether a process has regular contributions at all (flg_withreg, find_regions.f) depends on the
process and not on the input, so reg counts unless the input switches it off.
"""

import re
from pathlib import Path
from typing import Any, Dict, List

# What powheginput returns for a keyword that is not in the input file.
ABSENT = -1e6


def grid_contributions(input_file: Path, powheg_parameters: Dict[str, Any]) -> List[str]:
    """
    The contributions ("btilde", "remn", "reg") whose grids POWHEG combines in stage 3, for
    input_file with powheg_parameters set on top, as POWHEGStageSetup writes a stage's input.
    """
    entries = _read_entries(input_file)
    entries.update({keyword: str(value) for keyword, value in powheg_parameters.items()})

    def value(keyword: str) -> float:
        return _number(entries, keyword, input_file)

    born_only = value("bornonly") == 1 or value("LOevents") == 1
    virt_only = value("virtonly") == 1
    # flg_withdamp of pwhg_init.f, which switches the remnants on
    with_damp = value("hfact") > 0 or value("withdamp") == 1 or value("bornzerodamp") == 1 or value("hdamp") > 0

    contributions = []
    if value("withbtilde") != 0:
        contributions.append("btilde")
    if with_damp and not born_only and not virt_only and value("withremnants") != 0:
        contributions.append("remn")
    if not born_only and not virt_only and value("withregular") != 0:
        contributions.append("reg")
    return contributions


def _read_entries(input_file: Path) -> Dict[str, str]:
    """
    The value of every keyword in input_file, as text. As in powheginput.f, everything from a "#"
    or "!" on is a comment, and the first word of a line is its keyword.
    """
    entries = {}
    with open(input_file) as handle:
        for line in handle:
            words = re.split(r"[#!]", line, maxsplit=1)[0].split()
            if words:
                entries[words[0]] = " ".join(words[1:])
    return entries


def _number(entries: Dict[str, str], keyword: str, input_file: Path) -> float:
    """The value of keyword as POWHEG reads it, a Fortran number like 1d0, or ABSENT if it is not set."""
    if keyword not in entries:
        return ABSENT

    text = entries[keyword].split()[0] if entries[keyword] else ""
    try:
        return float(text.lower().replace("d", "e"))
    except ValueError:
        raise ValueError(
            f"ERROR: Cannot read the value {text!r} of {keyword} in {input_file} or in stage3's "
            f"powheg_parameters. Set stages.stage3_fullgrid.contributions to 1, 2 or 3 by hand."
        ) from None
