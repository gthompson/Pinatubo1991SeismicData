"""
winsuds.py
==========

Python wrappers for WinSUDS (PC-SUDS) utility executables on Windows.

This module provides a thin, explicit, and testable interface between
modern Python/ObsPy pipelines and legacy WinSUDS command-line utilities
(e.g. DEMUX, IRIG, SUD2MSED, SUD2SAC).

Design principles
-----------------
• One function per WinSUDS executable
• Exact alignment with documented WinSUDS usage
• File-based interfaces (WinSUDS tools do NOT operate on directories)
• No shell invocation (subprocess list-form only)
• Mockable for CI (WinSUDS not required in tests)
• Future-proof: native Python replacements can be swapped in later

Important WinSUDS quirks
-----------------------
• Many tools write output files into the *current working directory*
• Exit codes are not always reliable — validate outputs explicitly
• Files may be overwritten without warning
• Long paths and spaces may cause silent failures
• Some tools append logs (e.g. IRIG.LOG) to the CWD

Author: Glenn Thompson 
"""

from __future__ import annotations

import subprocess
import shutil
from pathlib import Path
from typing import Iterable, Optional


# ---------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------

class WinSUDSError(RuntimeError):
    """Raised when a WinSUDS utility fails or cannot be invoked."""


# ---------------------------------------------------------------------
# Internal runner
# ---------------------------------------------------------------------

def _run_suds_tool(
    exe: str,
    args: Iterable[str | Path],
    *,
    cwd: Optional[Path] = None,
    check: bool = True,
) -> subprocess.CompletedProcess:
    """
    Run a WinSUDS utility executable safely.

    Parameters
    ----------
    exe : str
        Executable name (e.g. 'demux.exe').
    args : iterable of str or Path
        Command-line arguments, passed positionally.
    cwd : Path, optional
        Working directory for execution.
    check : bool
        If True, raise an exception on non-zero exit code.

    Returns
    -------
    subprocess.CompletedProcess

    Raises
    ------
    WinSUDSError
        If the executable is not found or execution fails.
    """
    exe_path = shutil.which(exe)
    if exe_path is None:
        raise WinSUDSError(f"{exe} not found on PATH")

    cmd = [exe_path, *map(str, args)]

    try:
        return subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            check=check,
        )
    except subprocess.CalledProcessError as e:
        raise WinSUDSError(
            f"{exe} failed (exit code {e.returncode})\n"
            f"STDOUT:\n{e.stdout}\n"
            f"STDERR:\n{e.stderr}"
        ) from e


# ---------------------------------------------------------------------
# WinSUDS utility wrappers
# ---------------------------------------------------------------------

def demux(
    input_file: Path,
    output_file: Optional[Path] = None,
    *,
    bits: Optional[int] = None,
    cwd: Optional[Path] = None,
):
    """
    Demultiplex a multiplexed PC-SUDS waveform file.

    WinSUDS usage
    -------------
    DEMUX inputfile [outputfile]
      /12  Force data type to unsigned 12 bit
      /16  Force data type to signed 16 bit

    Parameters
    ----------
    input_file : Path
        Multiplexed PC-SUDS input file.
    output_file : Path, optional
        Output demultiplexed file. Defaults to inputfile.DMX.
    bits : int, optional
        Force data type: 12 or 16.
    cwd : Path, optional
        Working directory.

    Returns
    -------
    subprocess.CompletedProcess
    """
    args: list[str | Path] = []

    if bits == 12:
        args.append("/12")
    elif bits == 16:
        args.append("/16")
    elif bits is not None:
        raise ValueError("bits must be 12, 16, or None")

    args.append(input_file)

    if output_file:
        args.append(output_file)

    return _run_suds_tool("demux.exe", args, cwd=cwd)


def irig(
    suds_file: Path,
    *,
    station: Optional[str] = None,
    diagnostics: bool = False,
    cwd: Optional[Path] = None,
):
    """
    Decode IRIG-E timing from a PC-SUDS file.

    WinSUDS usage
    -------------
    IRIG [/D] sudsfspec [station name]

    Notes
    -----
    • Works on muxed or demuxed files
    • Appends messages to IRIG.LOG in the working directory
    • /D writes diagnostic output to IRIG.ASC

    Parameters
    ----------
    suds_file : Path
        Input PC-SUDS file or file specification.
    station : str, optional
        Station name (default is "IRIG").
    diagnostics : bool
        If True, write diagnostic output.
    cwd : Path, optional
        Working directory.

    Returns
    -------
    subprocess.CompletedProcess
    """
    args: list[str | Path] = []

    if diagnostics:
        args.append("/D")

    args.append(suds_file)

    if station:
        args.append(station)

    return _run_suds_tool("irig.exe", args, cwd=cwd)


def sud2msed(
    suds_file: Path,
    seed_file: Path,
    *,
    cwd: Optional[Path] = None,
):
    """
    Convert a demultiplexed PC-SUDS file to Mini-SEED.

    WinSUDS usage
    -------------
    SUD2MSED SUDSFileName SEEDFileName

    Parameters
    ----------
    suds_file : Path
        Demultiplexed PC-SUDS input file.
    seed_file : Path
        Output Mini-SEED filename.
    cwd : Path, optional
        Working directory.

    Returns
    -------
    subprocess.CompletedProcess
    """
    return _run_suds_tool(
        "sud2msed.exe",
        [suds_file, seed_file],
        cwd=cwd,
    )


def sud2sac(
    suds_file: Path,
    *,
    little_endian: bool = False,
    cwd: Optional[Path] = None,
):
    """
    Convert a demultiplexed PC-SUDS file to SAC format.

    WinSUDS usage
    -------------
    SUD2SAC [-l] inputfile

    Output files are named:
        inputbasename.Snn
    where nn is the channel number in hexadecimal.

    Parameters
    ----------
    suds_file : Path
        Demultiplexed PC-SUDS input file.
    little_endian : bool
        If True, write SAC files in little-endian format.
    cwd : Path, optional
        Working directory.

    Returns
    -------
    subprocess.CompletedProcess
    """
    args: list[str | Path] = []

    if little_endian:
        args.append("-l")

    args.append(suds_file)

    return _run_suds_tool("sud2sac.exe", args, cwd=cwd)


def sud2gse(
    suds_file: Path,
    *,
    cwd: Optional[Path] = None,
):
    """
    Convert a demultiplexed PC-SUDS file to GSE2.0 format.

    WinSUDS usage
    -------------
    SUD2GSE inputfile

    Output file:
        inputbasename.GSE

    Parameters
    ----------
    suds_file : Path
        Demultiplexed PC-SUDS input file.
    cwd : Path, optional
        Working directory.

    Returns
    -------
    subprocess.CompletedProcess
    """
    return _run_suds_tool(
        "sud2gse.exe",
        [suds_file],
        cwd=cwd,
    )


def sud2asc(
    input_file: Path,
    output_file: Optional[Path] = None,
    *,
    no_data: bool = False,
    comments_only: bool = False,
    quiet: bool = False,
    date_format: Optional[str] = None,
    cwd: Optional[Path] = None,
):
    """
    Convert a PC-SUDS file to ASCII format.

    WinSUDS usage
    -------------
    SUD2ASC [switches] inputfile [outputfile]

    Switches
    --------
    /N   No data, extract structs only
    /C   Extract comment structs only
    /Q   Quiet (no verbose output)
    /DM  Month/day date format
    /DJ  Day-of-year (Julian) format
    /DU  Unformatted time (seconds since 1970-01-01)

    Parameters
    ----------
    input_file : Path
        PC-SUDS input file.
    output_file : Path, optional
        ASCII output file (defaults to stdout).
    no_data : bool
        Extract structs only.
    comments_only : bool
        Extract comment structs only.
    quiet : bool
        Suppress verbose output.
    date_format : {"MD", "J", "U"}, optional
        Date/time format.
    cwd : Path, optional
        Working directory.

    Returns
    -------
    subprocess.CompletedProcess
    """
    args: list[str | Path] = []

    if no_data:
        args.append("/N")
    if comments_only:
        args.append("/C")
    if quiet:
        args.append("/Q")

    if date_format == "MD":
        args.append("/DM")
    elif date_format == "J":
        args.append("/DJ")
    elif date_format == "U":
        args.append("/DU")
    elif date_format is not None:
        raise ValueError("date_format must be one of {'MD', 'J', 'U', None}")

    args.append(input_file)

    if output_file:
        args.append(output_file)

    return _run_suds_tool("sud2asc.exe", args, cwd=cwd)


def sud2mat(
    suds_file: Path,
    *,
    cwd: Optional[Path] = None,
):
    """
    Convert a PC-SUDS file to MATLAB v4 MAT-file format.

    WinSUDS usage
    -------------
    SUD2MAT InputFileSpec

    Notes
    -----
    • Output files are written to the current working directory
    • No command-line switches are supported

    Parameters
    ----------
    suds_file : Path
        PC-SUDS input file.
    cwd : Path, optional
        Working directory.

    Returns
    -------
    subprocess.CompletedProcess
    """
    return _run_suds_tool(
        "sud2mat.exe",
        [suds_file],
        cwd=cwd,
    )
