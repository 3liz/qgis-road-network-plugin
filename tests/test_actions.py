"""Test the runner of the QGIS project actions."""

import pytest

from roadnetwork.actions import runner


@pytest.fixture
def messages(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    """Catch the error messages instead of displaying them in the QGIS message bar."""
    caught: list[tuple[str, str]] = []
    monkeypatch.setattr(
        runner, "display_error_message", lambda title, message: caught.append((title, message))
    )
    return caught


def test_run_unknown_action(messages: list[tuple[str, str]]):
    """An unknown action name returns an error."""
    error = runner.run("unknown_action")
    assert isinstance(error, runner.MethodNotFoundError)
    assert len(messages) == 1
    assert "unknown_action" in messages[0][1]


def test_run_action_missing_parameter(messages: list[tuple[str, str]]):
    """A missing parameter returns an error."""
    error = runner.run("drop_editing_session", layer_id="editing_sessions")
    assert isinstance(error, runner.MissingParameterError)
    assert str(error) == "editing_session_id"
    assert len(messages) == 1


def test_run_action_invalid_parameter(messages: list[tuple[str, str]]):
    """A parameter which cannot be converted to the expected type returns an error."""
    error = runner.run("drop_editing_session", layer_id="editing_sessions", editing_session_id="abc")
    assert isinstance(error, runner.InvalidArgumentError)
    assert str(error) == "editing_session_id"
    assert len(messages) == 1


def test_actions_names():
    """All the actions used in the QGIS project are available."""
    names = {action.__name__ for action in runner.ACTIONS}
    assert names == {
        "create_editing_session",
        "display_edited_data",
        "drop_editing_session",
        "finalize_roundabout",
    }
