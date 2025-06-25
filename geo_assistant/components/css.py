from typing import Any

# Shared frosted-glass styles
FROSTED_CONTAINER: dict[str, Any] = {
    "backgroundColor": "rgba(255,255,255,0.3)",
    "backdropFilter": "blur(8px)",
    "WebkitBackdropFilter": "blur(8px)",
    "border": "1px solid rgba(255,255,255,0.6)",
    "borderRadius": "5px",
}


FROSTED_INPUT: dict[str, Any] = {
    "backgroundColor": "rgba(255,255,255,0.25)",
    "backdropFilter": "blur(6px)",
    "WebkitBackdropFilter": "blur(6px)",
    "border": "1px solid rgba(255,255,255,0.5)",
    "borderRadius": "4px",
}