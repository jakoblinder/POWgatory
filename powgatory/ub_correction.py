"""
Upper bound correction factors from POWHEG's stage-4 counters files.

With ubexcess_correct 1, POWHEG writes into pwgcounters-st4-<seed>.dat the btilde and remnant
cross sections used for the event generation, the ones estimated on the fly during it, and the
factor by which event weights were increased after upper bound violations. The analysis programs
multiply the event weights by ub_btilde_corr and ub_remn_corr from their powheg.input
(lhef_analysis.f, main-PYTHIA8.f). This is the logic of POWHEG-BOX's
Scripts/FindReweightFromCounters.py, as functions.

Usage as a script, with the output of FindReweightFromCounters.py:
    python -m powgatory.ub_correction pwgcounters-st4-????.dat
"""

import argparse
import math
import re
import sys
from pathlib import Path
from typing import Dict, List

EVENT_TYPES = ("btilde", "remn")


class MissingCounterError(ValueError):
    """A counters file lacks one of the lines the correction factor is computed from."""


def _read_counter(text: str, which: str, name: str, counter_file) -> float:
    match = re.search(rf"^ *{which} {name}:[ =]*([^$ ]*) *$", text, re.M)
    if match is None:
        raise MissingCounterError(f'{counter_file} does not contain a "{which} {name}:" line')
    return float(match.group(1))


def combine_counters(counter_files: List[Path], which: str) -> Dict[str, float]:
    """
    Combine the counters of one event type ("btilde" or "remn") over several counters files.

    Returns the cross section used, the estimate and its error, and the bound violation
    correction factor (1 if the file has none). The estimates are combined weighted with their
    numbers of points; the correction factors are averaged as FindReweightFromCounters.py does.

    Raises:
        MissingCounterError: if a file lacks one of the lines.
        ValueError: if the files used different cross sections.
    """
    tot_numpts = 0
    for counter_file in counter_files:
        text = Path(counter_file).read_text()
        sig_used     = _read_counter(text, which, "cross section used", counter_file)
        sig_estim    = _read_counter(text, which, "cross section estimate", counter_file)
        sig_estimerr = _read_counter(text, which, "cross section error estimate", counter_file)
        sig_numpts   = _read_counter(text, which, "cross section estimate num. points", counter_file)
        try:
            sig_ubcorr = _read_counter(text, which, "bound violation correction factor", counter_file)
        except MissingCounterError:
            sig_ubcorr = 1

        if tot_numpts == 0:
            tot_numpts   = sig_numpts
            tot_estim    = sig_estim
            tot_estimerr = sig_estimerr
            tot_used     = sig_used
            tot_ubcorr   = sig_ubcorr
            tot_nfiles   = 1
        else:
            if tot_used != sig_used:
                raise ValueError(f"The {which} cross section used in {counter_file} is inconsistent with the previous files.")

            sumsq = (tot_estimerr**2 * tot_numpts + tot_estim**2) * tot_numpts \
                  + (sig_estimerr**2 * sig_numpts + sig_estim**2) * sig_numpts
            tot_estim    = (tot_estim * tot_numpts + sig_estim * sig_numpts) / (tot_numpts + sig_numpts)
            tot_numpts   = tot_numpts + sig_numpts
            tot_estimerr = math.sqrt((sumsq / tot_numpts - tot_estim**2) / tot_numpts)
            if tot_ubcorr != 1 or sig_ubcorr != 1:
                tot_ubcorr = (tot_ubcorr * tot_nfiles + sig_ubcorr) / (tot_nfiles + 1)
                tot_nfiles = tot_nfiles + 1

    return {"used": tot_used, "estimate": tot_estim, "error": tot_estimerr, "ubcorr": tot_ubcorr}


def correction_factors(counter_files: List[Path]) -> Dict[str, float]:
    """
    The powheg.input lines ub_btilde_corr and ub_remn_corr for the events of these counters files.

    An event type gets a factor only if its weights were corrected for upper bound violations
    (bound violation correction factor != 1); otherwise the estimate on the fly differs from the
    cross section used only by its statistical error, and the events need no correction. An
    event type whose counters are missing gets no factor either.
    """
    factors = {}
    for which in EVENT_TYPES:
        try:
            combined = combine_counters(counter_files, which)
        except MissingCounterError:
            continue
        if combined["ubcorr"] != 1:
            factors[f"ub_{which}_corr"] = combined["estimate"] / combined["used"] / combined["ubcorr"]
    return factors


def main() -> int:
    """Print the combined counters and correction factors, as FindReweightFromCounters.py does."""
    parser = argparse.ArgumentParser(
        prog="python -m powgatory.ub_correction",
        description="Upper bound correction factors from POWHEG's stage-4 counters files, as FindReweightFromCounters.py.",
    )
    parser.add_argument("counter_files", nargs="+", help="pwgcounters-st4-<seed>.dat files, combined into one factor")
    args = parser.parse_args()

    for which in EVENT_TYPES:
        try:
            combined = combine_counters(args.counter_files, which)
        except MissingCounterError as error:
            print(error)
            print(f"skipping {which}")
            continue

        print(f"Cross section used for {which} event generation: {combined['used']}")
        print(f"Cross section computed on the fly for {which} event generation: {combined['estimate']} +- {combined['error']}")
        if combined["ubcorr"] != 1:
            factor = combined["estimate"] / combined["used"] / combined["ubcorr"]
            print(f"Weight increment factor due to corrections for upper bound violation in {which} events: {combined['ubcorr']}")
            print(f"Total correction factor for {which} events: {factor}")
            print(f"ub_{which}_corr     {factor}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
