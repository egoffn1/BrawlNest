#!/usr/bin/env python3
"""
  python main.py           — CLI (интерактивное меню)
  python main.py api       — REST API сервер
  python main.py <команда> — прямой вызов CLI команды
"""
import sys, asyncio

def _run_cli():
    from cli import main
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        from rich.console import Console
        Console(highlight=False).print("\n  [dim]Прервано[/dim]\n")
        sys.exit(0)

def _run_api():
    try:
        import uvicorn
    except ImportError:
        print("uvicorn не установлен: pip install uvicorn fastapi"); sys.exit(1)
    import os
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "80"))
    uvicorn.run("api.rest_api:app", host=host, port=port, reload=False)

if __name__ == "__main__":
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else "cli"
    if mode == "api":
        _run_api()
    else:
        _run_cli()
