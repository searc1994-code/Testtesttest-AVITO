
import argparse

import tenant_manager
from browser_bot import process_queue
from safe_logs import log_event


def main() -> None:
    parser = argparse.ArgumentParser(description="Process queued WB complaint drafts via browser automation")
    parser.add_argument("--tenant", help="Tenant ID")
    parser.add_argument("--all-tenants", action="store_true", help="Process queues sequentially for all enabled tenants")
    parser.add_argument("--max-items", type=int, default=0, help="How many queued complaints to process now; 0 means all")
    parser.add_argument("--dry-run", action="store_true", help="Open the dialog and stop before final submit")
    args = parser.parse_args()

    log_event("complaint_worker", "worker_cli_start", tenant_id=args.tenant or "", all_tenants=bool(args.all_tenants), max_items=max(0, args.max_items), dry_run=bool(args.dry_run))
    if args.all_tenants:
        for tenant in tenant_manager.load_tenants():
            if not tenant.get("enabled", True):
                continue
            tenant_id = tenant["id"]
            tenant_manager.apply_tenant_context(tenant_id)
            result = process_queue(max_items=max(0, args.max_items), dry_run=args.dry_run)
            log_event("complaint_worker", "worker_tenant_finish", tenant_id=tenant_id, result=result)
            print(f"{tenant_id}: {result.get('message') or result}")
        return

    if not args.tenant:
        parser.error("Укажите --tenant или --all-tenants")
    tenant_manager.apply_tenant_context(args.tenant)
    result = process_queue(max_items=max(0, args.max_items), dry_run=args.dry_run)
    log_event("complaint_worker", "worker_single_finish", tenant_id=args.tenant, result=result)
    print(result.get("message") or result)


if __name__ == "__main__":
    main()
