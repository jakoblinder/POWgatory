"""
Merge histogram files (gnuplot-style .top tables) of many POWHEG runs into one.

Data lines hold four numbers: the bin edges, the value and its error. All other lines (comments
like "# name index 3", blank lines) are passed through from the first file. All files must have
the same layout. They are read one at a time, so memory grows with the number of lines of one
file, not with the number of files.

This is the logic of mergedata (the C++ rewrite of POWHEG's Fortran program), with numpy:
    python -m powgatory.merge <method> files... [-modern] [-norename] [-out file]
"""

import argparse
import io
import math
import re
import sys
from pathlib import Path
from typing import List

import numpy as np

METHOD_NAMES = {
    1: "mean",      # Sets with equal statistics: mean, errors added in quadrature / n
    2: "uneven",    # Sets with different statistics: weighted with 1/error^2
    3: "sum",       # Sum (like born + virtual + real)
    4: "max",       # Maximum, with its error
    5: "min",       # Minimum, with its error
    6: "sum-last",  # Sum of all but the last set, minus the last set
}


def _parse_data_line(line: str):
    """
    The first four numbers of line, or None if it is no data line. Like mergedata, nan and inf
    do not count as numbers; unlike mergedata, Fortran D exponents do, so merged files can be
    merged again.
    """
    tokens = line.split()
    if len(tokens) < 4:
        return None
    try:
        values = [float(token.replace("D", "E").replace("d", "e")) for token in tokens[:4]]
    except ValueError:
        return None
    if all(math.isfinite(value) for value in values):
        return values
    else:
        return None


def _split_lines(text: str) -> List[str]:
    """The lines of text, without line ends, as C++'s getline gives them."""
    lines = text.split("\n")
    if text.endswith("\n"):
        lines.pop()
    return lines


def _data_columns(raw: bytes, data_index: np.ndarray, edges: np.ndarray):
    """
    Values and errors (columns 3 and 4) of the data lines of a later file, given as bytes, and
    which of them count (finite numbers).

    numpy parses the whole file at once, skipping comments and blank lines; its rows are the data
    lines of the first file if their number and both bin edges agree. Otherwise, e.g. if a line
    is data in one file and text in the other, the file is parsed line by line, as mergedata does.
    """
    try:
        columns = np.loadtxt(io.BytesIO(raw.replace(b"D+", b"E+").replace(b"D-", b"E-")), ndmin=2, usecols=(0, 1, 2, 3))
        aligned = columns.shape[0] == len(data_index) and np.array_equal(columns[:, :2], edges)
    except ValueError:
        aligned = False

    if aligned:
        values, errors = columns[:, 2], columns[:, 3]
        counts = np.isfinite(values) & np.isfinite(errors)
    else:
        lines  = _split_lines(raw.decode())
        parsed = [_parse_data_line(lines[k]) for k in data_index]
        counts = np.array([numbers is not None for numbers in parsed], dtype=bool)
        values = np.array([numbers[2] if numbers else 0.0 for numbers in parsed])
        errors = np.array([numbers[3] if numbers else 0.0 for numbers in parsed])

    return np.where(counts, values, 0.0), np.where(counts, errors, 0.0), counts


def _fortran_number(x: float) -> str:
    """x as mergedata writes it by default, like Fortran: 0.12345678D+01, -.12345678D+01."""
    if x == 0.0:
        return "0.00000000D+00"
    elif not math.isfinite(x):
        return "NaN" if math.isnan(x) else ("Infinity" if x > 0 else "-Infinity")

    exponent = int(math.floor(math.log10(abs(x))))
    mantissa = abs(x) / 10.0**exponent
    if mantissa >= 1.0:
        mantissa /= 10.0
        exponent += 1
    elif mantissa < 0.1:
        mantissa *= 10.0
        exponent -= 1

    if x < 0.0:
        return ("-%1.8fD%+03d" % (mantissa, exponent)).replace("-0.", "-.", 1)
    else:
        return "%1.8fD%+03d" % (mantissa, exponent)


def _format_line(numbers, fortran_format: bool) -> str:
    if fortran_format:
        return " " + " ".join(_fortran_number(number) for number in numbers)
    else:
        return " ".join("%14.8E" % number for number in numbers)


def merge_histograms(input_files: List, method: int, output_file, fortran_format: bool = True):
    """
    Merge the histogram files input_files with method (see METHOD_NAMES) into output_file.

    The numbers are written in Fortran D notation by default (as mergedata), with
    fortran_format=False as %14.8E. The output is written under a temporary name and then renamed,
    so it never exists half-written.

    Raises:
        ValueError: for an unknown method, an output file that is one of the inputs, no input
            files, or files with different numbers of lines.
    """
    if method not in METHOD_NAMES:
        raise ValueError(f"Unknown merge method {method}, expected one of {sorted(METHOD_NAMES)}.")
    elif not input_files:
        raise ValueError("No input files to merge.")
    elif Path(output_file).resolve() in {Path(f).resolve() for f in input_files}:
        raise ValueError(f"The output file {output_file} is one of the input files.")

    # The first file fixes the layout: which lines are data, the bin edges, and the start values.
    lines = _split_lines(Path(input_files[0]).read_text())
    parsed = [_parse_data_line(line) for line in lines]
    data_index = np.array([k for k, numbers in enumerate(parsed) if numbers is not None], dtype=int)
    first = np.array([parsed[k] for k in data_index]).reshape(-1, 4)
    edges, value, error = first[:, :2], first[:, 2], first[:, 3]

    if method == 2:
        weighted = error != 0.0
        y   = np.where(weighted, value / np.where(weighted, error, 1.0)**2, 0.0)
        err = np.where(weighted, 1.0 / np.where(weighted, error, 1.0)**2, 0.0)
    else:
        y, err = value.copy(), error**2

    for ifile, path in enumerate(input_files[1:], start=1):
        raw = Path(path).read_bytes()
        if raw.count(b"\n") + (0 if raw.endswith(b"\n") or not raw else 1) != len(lines):
            raise ValueError(f"File {path} does not match {input_files[0]} in length.")
        w3, w4, counts = _data_columns(raw, data_index, edges)

        if method == 6 and ifile == len(input_files) - 1:
            w3 = -w3
        if method in (1, 3, 6):
            y   += w3
            err += np.where(counts, w4**2, 0.0)
        elif method == 2:
            take = counts & (w4 != 0.0)
            safe = np.where(take, w4, 1.0)
            y   += np.where(take, w3 / safe**2, 0.0)
            err += np.where(take, 1.0 / safe**2, 0.0)
        elif method == 4:
            take = counts & (w3 > y)
            y   = np.where(take, w3, y)
            err = np.where(take, w4**2, err)
        else:
            take = counts & (w3 < y)
            y   = np.where(take, w3, y)
            err = np.where(take, w4**2, err)

    nfiles = len(input_files)
    if method == 1:
        y, err = y / nfiles, np.sqrt(err) / nfiles
    elif method == 2:
        weighted = err != 0.0
        y   = np.where(weighted, y / np.where(weighted, err, 1.0), 0.0)
        err = np.where(weighted, 1.0 / np.sqrt(np.where(weighted, err, 1.0)), 0.0)
    else:
        err = np.sqrt(err)

    merged = [line.rstrip(" \t\r\n") for line in lines]
    for row, k in enumerate(data_index):
        merged[k] = _format_line((edges[row, 0], edges[row, 1], y[row], err[row]), fortran_format)

    temporary = Path(f"{output_file}.tmp")
    temporary.write_text("".join(line + "\n" for line in merged))
    temporary.replace(output_file)


def default_output_name(first_file, method: int) -> Path:
    """
    The name of the merged file: the seed in the first file's name replaced by the method's name,
    i.e. the last "-<digits>-" (pwg-0001-NLO.top -> pwg-mean-NLO.top), or else the last
    "-<digits>." (pwgLHEF_analysis-0001.top -> pwgLHEF_analysis-mean.top).

    Raises:
        ValueError: if the name contains neither (mergedata would overwrite the first file).
    """
    path = Path(first_file)
    matches = list(re.finditer(r"-\d+(?=-)", path.name)) or list(re.finditer(r"-\d+(?=\.)", path.name))
    if not matches:
        raise ValueError(f"No seed found in the name {path.name}; give the output file with -out.")

    last = matches[-1]
    return path.with_name(f"{path.name[:last.start()]}-{METHOD_NAMES[method]}{path.name[last.end():]}")


def main() -> int:
    """Command line like mergedata: <method> files... [-modern] [-norename] [-out file]."""
    parser = argparse.ArgumentParser(
        prog="python -m powgatory.merge",
        description="Merge histogram files (.top) of many POWHEG runs, like mergedata.",
        epilog="methods: " + ", ".join(f"{number} {name}" for number, name in METHOD_NAMES.items()),
    )
    parser.add_argument("method", type=int, choices=sorted(METHOD_NAMES), help="how to combine the files")
    parser.add_argument("files", nargs="+", help="histogram files with the same layout")
    parser.add_argument("-modern", "--modern", action="store_true",
                        help="write numbers as 0.12345678E+00 instead of Fortran's 0.12345678D+00")
    parser.add_argument("-norename", "--norename", action="store_true",
                        help="write fort.12 instead of naming the output after the first file")
    parser.add_argument("-out", "--out", metavar="FILE",
                        help="output file (default: the first file's name with its seed replaced by the method)")
    args = parser.parse_args()

    if args.out:
        output_file = args.out
    elif args.norename:
        output_file = "fort.12"
    else:
        output_file = default_output_name(args.files[0], args.method)

    print(f"Merging {len(args.files)} files with method {args.method} ({METHOD_NAMES[args.method]}) into {output_file}")
    merge_histograms(args.files, args.method, output_file, fortran_format=not args.modern)
    return 0


if __name__ == "__main__":
    sys.exit(main())
