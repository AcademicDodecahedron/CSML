import click
from returns.maybe import Maybe
from sphinx_click.ext import _indent


def format_arguments(ctx: click.Context):
    for arg in ctx.command.params:
        if not isinstance(arg, click.Argument):
            continue

        yield ".. option:: {}".format(arg.human_readable_name)
        yield ""

        yield _indent(
            "{type}{help}".format(
                type="**Required**" if arg.required else "(Optional)",
                help=Maybe.from_optional(getattr(arg, "help", None))
                .map(lambda help: ": " + help)
                .value_or(""),
            )
        )

        yield ""


def setup(app):
    def replace_arguments(app, ctx: click.Context, lines: list[str]):
        lines.clear()
        lines.extend(format_arguments(ctx))

    app.connect("sphinx-click-process-arguments", replace_arguments)
