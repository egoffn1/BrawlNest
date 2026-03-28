#!/usr/bin/env python3
"""
  python main.py           — CLI (интерактивное меню)
  python main.py api       — REST API (порт 8000)
  python main.py <команда> — прямой вызов
"""
import sys
import asyncio


def _run_cli():
    from cli import main
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        from rich.console import Console
        Console(highlight=False).print("\n  [dim]interrupted · data saved[/dim]\n")
        sys.exit(0)


def _run_api():
    try:
        import uvicorn
    except ImportError:
        print("uvicorn не установлен: pip install uvicorn fastapi")
        sys.exit(1)
    from config import SERVER_CFG
    uvicorn.run(
        "api.rest_api:app",
        host=SERVER_CFG.get("host", "0.0.0.0"),
        port=SERVER_CFG.get("port", 8000),
        reload=False,
    )


if __name__ == "__main__":
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else "cli"
    if mode == "api":
        _run_api()
    else:
        _run_cli()
