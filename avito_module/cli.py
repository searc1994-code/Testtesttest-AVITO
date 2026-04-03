from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from .blueprint import create_standalone_app
from .service import AvitoService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Avito module worker / dev server")
    parser.add_argument(
        "mode",
        choices=[
            "serve",
            "sync",
            "drafts",
            "send",
            "poll",
            "test-token",
            "bootstrap-browser",
            "backfill",
            "replay-dlq",
            "metrics",
        ],
        help="run mode",
    )
    parser.add_argument("--tenant-id", default="default")
    parser.add_argument("--base-dir", default="")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5077)
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--max-chats", type=int, default=20)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--auto-only", action="store_true")
    parser.add_argument("--chat-id", action="append", default=[])
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--max-events", type=int, default=500)
    parser.add_argument("--dlq-id", type=int, default=0)
    return parser



def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    base_dir = Path(args.base_dir) if args.base_dir else None
    if args.mode == "serve":
        app = create_standalone_app(base_dir=base_dir)
        app.config["AVITO_MODULE_DEFAULT_TENANT_ID"] = args.tenant_id
        app.run(host=args.host, port=args.port, debug=False)
        return 0

    service = AvitoService(args.tenant_id, base_dir=base_dir)
    try:
        if args.mode == "sync":
            result = service.sync_once(max_chats=args.max_chats)
            print(result)
            return 0
        if args.mode == "drafts":
            result = service.generate_drafts(limit=args.limit, chat_ids=args.chat_id or None)
            print(result)
            return 0
        if args.mode == "send":
            result = service.send_ready_drafts(limit=args.limit, auto_only=args.auto_only)
            print(result)
            return 0
        if args.mode == "test-token":
            token = service.api_client.ensure_token()
            print({"token_present": bool(token), "prefix": token[:12]})
            return 0
        if args.mode == "bootstrap-browser":
            result = service.bootstrap_browser_state(timeout_seconds=args.timeout_seconds)
            print(result)
            return 0
        if args.mode == "backfill":
            result = service.backfill_history(hours=args.hours, max_events=args.max_events)
            print(result)
            return 0
        if args.mode == "replay-dlq":
            if not args.dlq_id:
                parser.error("--dlq-id is required for replay-dlq")
            result = service.replay_dead_letter(args.dlq_id)
            print(result)
            return 0
        if args.mode == "metrics":
            print(service.metrics_snapshot())
            return 0
        if args.mode == "poll":
            import time

            while True:
                if service.config.polling_fallback_enabled:
                    service.sync_once(max_chats=args.max_chats)
                service.generate_drafts(limit=args.limit, chat_ids=args.chat_id or None)
                if service.config.auto_mode in {"simple_only", "all"}:
                    service.send_ready_drafts(limit=args.limit, auto_only=(service.config.auto_mode == "simple_only"))
                time.sleep(max(5, args.interval))
        return 0
    finally:
        service.close()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
