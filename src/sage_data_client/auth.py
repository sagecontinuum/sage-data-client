from pathlib import Path
from typing import Dict, Optional, Tuple

CREDENTIALS_PATH = Path.home() / ".sage" / "credentials"
SAGE_BASE_URL = "https://sagecontinuum.org/"
PORTAL_URL = "https://portal.sagecontinuum.org"

def load_credentials(path: Path = CREDENTIALS_PATH) -> Dict[str, str]:
    """Load credentials from file. Returns dict with 'username' and 'token' keys."""
    try:
        creds = {}
        for line in path.read_text().splitlines():
            line = line.strip()
            if "=" in line:
                k, v = line.split("=", 1)
                creds[k.strip()] = v.strip()
        return creds
    except FileNotFoundError:
        return {}


def load_token(path: Path = CREDENTIALS_PATH) -> Optional[str]:
    """Load auth token from credentials file. Returns None if not found."""
    return load_credentials(path).get("token") or None


def save_credentials(username: str, token: str, path: Path = CREDENTIALS_PATH) -> None:
    """Save username and token to credentials file with owner-only permissions."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("username={}\ntoken={}\n".format(username, token))
    path.chmod(0o600)
