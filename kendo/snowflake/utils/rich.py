from rich import print


def colored_print(
    text: str, level: None | str = "info" or "warning" or "error" or "success"
):
    if not level:
        print(text)
        return

    color = None
    if level == "info":
        color = "cyan"
    elif level == "warning":
        color = "yellow"
    elif level == "error":
        color = "red"
    elif level == "success":
        color = "green"

    print(f"[{color}]{text}[/{color}]")
