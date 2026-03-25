"""Stdin/stdout CSV helpers with in-memory streams (no optional extras).

Run::

    python docs/examples/io/extras_stdin_stdout.py
"""

from __future__ import annotations

from io import StringIO

from pydantable.io.extras import read_csv_stdin, write_csv_stdout


def main() -> None:
    inp = StringIO("c,d\n9,10\n")
    data = read_csv_stdin(stream=inp)
    assert [int(x) for x in data["c"]] == [9]
    assert [int(x) for x in data["d"]] == [10]

    out = StringIO()
    write_csv_stdout({"c": ["x"], "d": ["y"]}, stream=out)
    body = out.getvalue()
    assert "c" in body and "x" in body

    print("extras_stdin_stdout: ok")


if __name__ == "__main__":
    main()
