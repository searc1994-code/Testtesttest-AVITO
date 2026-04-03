
import argparse

import tenant_manager
from browser_bot import interactive_login
from safe_logs import log_event


def main() -> None:
    parser = argparse.ArgumentParser(description="Interactive WB login for a specific tenant")
    parser.add_argument("--tenant", required=True, help="Tenant ID")
    args = parser.parse_args()

    tenant_manager.apply_tenant_context(args.tenant)
    log_event("tenants", "interactive_login_cli_start", tenant_id=args.tenant)
    status = interactive_login()
    tenant_manager.update_tenant(args.tenant, last_login_at=status.get("saved_at", ""))
    log_event("tenants", "interactive_login_cli_finish", tenant_id=args.tenant, saved_at=status.get("saved_at", ""))
    print("Сессия сохранена:")
    for k, v in status.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
