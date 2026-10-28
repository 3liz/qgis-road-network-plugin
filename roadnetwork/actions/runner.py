from inspect import Parameter, signature
from typing import Any

from ..plugin_tools.i18n import tr
from .methods import (
    create_editing_session,
    display_edited_data,
    drop_editing_session,
    finalize_roundabout,
)
from .tools import display_error_message

ACTIONS = (
    create_editing_session,
    display_edited_data,
    drop_editing_session,
    finalize_roundabout,
)


class RunActionError(Exception):
    pass


class InvalidArgumentError(RunActionError):
    pass


class MissingParameterError(RunActionError):
    pass


class MethodNotFoundError(RunActionError):
    pass


def run(name: str, **kwargs: Any) -> RunActionError | None:
    """
    Run the action with the given name and arguments.

    :param name: Name of the action to run. The available values are:
        - create_editing_session
        - display_edited_data
        - drop_editing_session
        - finalize_roundabout
    :param **kwargs: Arguments to pass to the action.
    :return: None, or the error if the action cannot be run
    """
    # Find action
    if action := next((a for a in ACTIONS if a.__name__ == name), None):
        # Check arguments
        sig = signature(action)
        params = sig.parameters

        def check(param: Parameter) -> Any:
            if param.name not in kwargs:
                raise MissingParameterError(param.name)

            anno = param.annotation
            value = kwargs[param.name]
            if not isinstance(value, anno):
                try:
                    value = anno(value)
                except ValueError:
                    raise InvalidArgumentError(param.name) from None
            return value

        try:
            args = {arg: check(param) for arg, param in params.items()}
        except RunActionError as err:
            display_error_message(
                tr("RoadNetwork action"),
                tr('Invalid arguments for the action "{}". Expected: {}, received: {}').format(
                    name, sig, kwargs
                ),
            )
            return err

        action(**args)
        return None

    display_error_message(
        tr("RoadNetwork action"),
        tr('The action "{}" has not been found').format(name),
    )

    return MethodNotFoundError(name)
