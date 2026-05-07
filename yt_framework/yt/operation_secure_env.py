"""Split operation env into public ``environment`` vs YTsaurus ``secure_vault``."""

from __future__ import annotations

import os
import shlex
from typing import (
    Any,
    Collection,
    Dict,
    FrozenSet,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
)

# Keys that may stay in plain job ``environment`` (visible in the YT web UI).
DEFAULT_PUBLIC_ENV_KEYS: FrozenSet[str] = frozenset(
    {
        "YT_STAGE_NAME",
        "YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB",
        "TOKENIZER_ARTIFACT_FILE",
        "TOKENIZER_ARTIFACT_DIR",
        "TOKENIZER_ARTIFACT_NAME",
    }
)

_SECURE_PREFIX = "YT_SECURE_VAULT_"
# yt.wrapper reads pickling key from YT_SECURE_VAULT__PICKLING_KEY; inner name is _PICKLING_KEY.
_SKIP_PROMOTE_INNER = frozenset({"_PICKLING_KEY"})


def partition_env_for_yt_spec(
    env: Mapping[str, str],
    extra_public_keys: Optional[Collection[str]] = None,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Split env into (public_environment, secure_vault_flat_strings).

    Non-allowlisted keys are placed in ``secure_vault`` as string scalars.
    """
    public_allow = set(DEFAULT_PUBLIC_ENV_KEYS)
    if extra_public_keys:
        public_allow.update(str(k) for k in extra_public_keys)
    public: Dict[str, str] = {}
    secure: Dict[str, str] = {}
    for k, v in env.items():
        sk = str(k)
        if sk in public_allow:
            public[sk] = str(v)
        else:
            secure[sk] = str(v)
    return public, secure


def promote_secure_vault_environment() -> None:
    """
    Copy ``YT_SECURE_VAULT_<name>`` into ``<name>`` when unset (stdlib TypedJob hook).

    Skips aggregate ``YT_SECURE_VAULT`` and internal ``_PICKLING_KEY``.
    """
    for k, v in list(os.environ.items()):
        if not k.startswith(_SECURE_PREFIX) or k == "YT_SECURE_VAULT":
            continue
        inner = k[len(_SECURE_PREFIX) :]
        if inner in _SKIP_PROMOTE_INNER:
            continue
        os.environ.setdefault(inner, v)


def wrap_shell_command_with_secure_vault_promotion(inner: str) -> str:
    """
    Prefix a shell command so vault vars are promoted then the user command runs under bash.

    Uses only the Python stdlib (no ``yt_framework`` import) for minimal job sandboxes.
    This wrapper intentionally requires both ``python3`` and ``bash`` binaries in
    the job image.
    """
    snippet = (
        "import os,sys\n"
        "p='YT_SECURE_VAULT_'\n"
        "for k,v in list(os.environ.items()):\n"
        " if not k.startswith(p) or k=='YT_SECURE_VAULT':\n"
        "  continue\n"
        " n=k[len(p):]\n"
        " if n in {'_PICKLING_KEY'}:\n"
        "  continue\n"
        " os.environ.setdefault(n,v)\n"
        "os.execvp(sys.argv[1],sys.argv[1:])\n"
    )
    return "python3 -c " + shlex.quote(snippet) + " bash -c " + shlex.quote(inner)


def pop_secure_env_client_kwargs(
    kwargs: MutableMapping[str, Any],
) -> tuple[Any, bool, Any]:
    """Remove framework-only kwargs before YT SpecBuilder or dev subprocess paths."""
    environment_public_keys = kwargs.pop("environment_public_keys", None)
    use_plain = bool(kwargs.pop("use_plain_environment_for_secrets", False))
    user_secure_vault = kwargs.pop("secure_vault", None)
    return environment_public_keys, use_plain, user_secure_vault


def merge_secure_vault(
    env_secrets: Mapping[str, str],
    *,
    docker_image: Optional[str],
    docker_auth: Optional[Mapping[str, str]],
    user_secure_vault: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    """
    Build a single ``secure_vault`` mapping for the operation spec.

    User ``secure_vault`` entries override framework env-derived keys except
    ``docker_auth``, which is shallow-merged with user sub-dict (user wins on
    duplicate keys).
    """
    out: Dict[str, Any] = dict(env_secrets)
    if docker_image:
        out["docker_auth"] = dict(docker_auth or {})
    if not user_secure_vault:
        return out
    for k, v in user_secure_vault.items():
        if k == "docker_auth" and isinstance(v, Mapping):
            base = out.get("docker_auth")
            if isinstance(base, Mapping):
                merged = dict(base)
                merged.update(dict(v))
                out["docker_auth"] = merged
            else:
                out["docker_auth"] = dict(v)
        else:
            out[k] = v
    return out
