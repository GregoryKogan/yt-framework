"""Minimal fake YT operation object used by dev-mode clients."""


class DevOperation:
    """Fake operation for dev run_map; implements wait, get_state, get_error."""

    def __init__(
        self,
        returncode: int,
        stderr_message: str = "",
        *,
        leg_name: str = "Mapper",
    ) -> None:
        self._returncode = returncode
        self._stderr = stderr_message
        self._leg_name = leg_name
        self.id = f"dev-operation-{id(self)}"  # Fake operation ID for dev mode

    def wait(self) -> None:
        pass

    def get_state(self) -> str:
        return "completed" if self._returncode == 0 else "failed"

    def get_error(self) -> str | None:
        if self._returncode == 0:
            return None
        return self._stderr or f"{self._leg_name} exited with code {self._returncode}"
