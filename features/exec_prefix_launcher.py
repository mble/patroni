#!/usr/bin/env python
"""Test double for a ``postgresql.postgres_exec_prefix`` launcher.

It appends the argv it was asked to execute to a log file and then replaces itself with that argv, which is the
contract Patroni expects from a real launcher.

Usage: exec_prefix_launcher.py --log LOGFILE -- postgres [options...]
"""
import os
import sys


def main() -> None:
    argv = sys.argv[1:]
    if len(argv) < 3 or argv[0] != '--log':
        sys.stderr.write('exec_prefix_launcher.py: expected `--log LOGFILE -- command...`\n')
        return sys.exit(2)

    logfile, argv = argv[1], argv[2:]
    if argv[0] == '--':
        argv = argv[1:]

    with open(logfile, 'a') as f:
        f.write('\t'.join(argv) + '\n')
        f.flush()
        os.fsync(f.fileno())

    try:
        # `execvp`, not `execv`: Patroni passes `postgres` without a directory when `postgresql.bin_dir` is unset
        os.execvp(argv[0], argv)
    except OSError as e:
        sys.stderr.write('exec_prefix_launcher.py: failed to exec {0}: {1}\n'.format(argv[0], e))
        sys.exit(1)


if __name__ == '__main__':
    main()
