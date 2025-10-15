
import argparse

from typing import (
    Callable,
)

cli = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)

subparsers = cli.add_subparsers(
    title="commands",
    description="plugin commands",
)


def command(name: str, **kwargs) -> Callable:
    """Wrap subcommand function"""

    def decorator(fun):
        if isinstance(fun, tuple):
            func, options = fun
        else:
            func = fun
            options = ()
        parser = subparsers.add_parser(name, description=func.__doc__, **kwargs)
        for opt in options:
            parser.add_argument(*opt[0], **opt[1])
        parser.set_defaults(func=func)

    return decorator


def argument(*args, **kwargs):
    arg = (args, kwargs)

    def decorator(fun):
        if isinstance(fun, tuple):
            # tuple(Callable, tuple[option])
            return (fun[0], (*fun[1], arg))
        else:
            return (fun, (arg,))

    return decorator


#
# Commands
#

@command("install-version", help="Show current database install version")
def show_install_version(args):
    from .plugin_tools import resources
    print(resources.schema_version())


@command("default-schema", help="Show default schema name")
def show_default_schema(args):
    from .plugin_tools import resources
    print(resources.schema_name())


def main() -> None:
    """Main function for the CLI menu."""

    args = cli.parse_args()
    if "func" not in args:
        cli.print_help()
        cli.exit(1)
    else:
        args.func(args)


if __name__ == "__main__":
    main()
