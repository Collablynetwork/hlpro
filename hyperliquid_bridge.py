#!/usr/bin/env python3
import json
import os
import sys
import warnings

warnings.filterwarnings("ignore")

Account = None
Exchange = None
Info = None
Cloid = None
IMPORT_ERROR = None


def emit(payload, exit_code=0):
    sys.stdout.write(json.dumps(payload, separators=(",", ":")))
    sys.stdout.flush()
    raise SystemExit(exit_code)


def load_args():
    if len(sys.argv) < 2:
        emit({"ok": False, "error": "Missing command"}, 1)

    command = sys.argv[1]
    raw = sys.argv[2] if len(sys.argv) > 2 else "{}"
    try:
        args = json.loads(raw)
    except json.JSONDecodeError as exc:
        emit({"ok": False, "error": f"Invalid JSON args: {exc}"}, 1)
    return command, args


def ensure_deps():
    global Account, Exchange, Info, Cloid, IMPORT_ERROR
    if IMPORT_ERROR:
        emit(
            {
                "ok": False,
                "error": f"bridge-dependency-missing: {IMPORT_ERROR}",
                "python_executable": sys.executable,
            },
            2,
        )

    if all(value is not None for value in (Account, Exchange, Info, Cloid)):
        return

    try:
        from eth_account import Account as ImportedAccount  # noqa: E402
        from hyperliquid.exchange import Exchange as ImportedExchange  # noqa: E402
        from hyperliquid.info import Info as ImportedInfo  # noqa: E402
        from hyperliquid.utils.types import Cloid as ImportedCloid  # noqa: E402
    except ModuleNotFoundError as exc:
        IMPORT_ERROR = f"Missing Python module '{exc.name}'"
        emit(
            {
                "ok": False,
                "error": f"bridge-dependency-missing: {IMPORT_ERROR}",
                "python_executable": sys.executable,
            },
            2,
        )
    except Exception as exc:  # noqa: BLE001
        IMPORT_ERROR = str(exc)
        emit(
            {
                "ok": False,
                "error": f"bridge-dependency-missing: {IMPORT_ERROR}",
                "python_executable": sys.executable,
            },
            2,
        )

    Account = ImportedAccount
    Exchange = ImportedExchange
    Info = ImportedInfo
    Cloid = ImportedCloid


def base_url(args):
    return args.get("base_url") or os.environ.get("HYPERLIQUID_API_URL") or "https://api.hyperliquid.xyz"


def build_info(args):
    ensure_deps()
    return Info(base_url=base_url(args), skip_ws=True, timeout=30)


def build_wallet(secret_key):
    ensure_deps()
    if not secret_key:
        emit({"ok": False, "error": "Missing HYPERLIQUID_SECRET_KEY"}, 2)
    return Account.from_key(secret_key)


def build_user_address(args):
    ensure_deps()
    account_address = (args.get("account_address") or os.environ.get("HYPERLIQUID_ACCOUNT_ADDRESS") or "").lower() or None
    vault_address = (args.get("vault_address") or os.environ.get("HYPERLIQUID_VAULT_ADDRESS") or "").lower() or None
    secret_key = args.get("secret_key") or os.environ.get("HYPERLIQUID_SECRET_KEY", "")
    derived_address = Account.from_key(secret_key).address.lower() if secret_key else None
    user_address = vault_address or account_address or derived_address
    if not user_address:
        emit({"ok": False, "error": "Missing account, vault, or secret key for user context"}, 2)
    return account_address, vault_address, user_address


def build_exchange(args):
    secret_key = args.get("secret_key") or os.environ.get("HYPERLIQUID_SECRET_KEY", "")
    wallet = build_wallet(secret_key)
    account_address, vault_address, user_address = build_user_address(args)
    info = build_info(args)
    exchange = Exchange(
        wallet,
        base_url=base_url(args),
        meta=info.meta(),
        spot_meta=info.spot_meta(),
        account_address=account_address,
        vault_address=vault_address,
        timeout=30,
    )
    return wallet, info, exchange, user_address


def maybe_cloid(value):
    ensure_deps()
    if not value:
        return None
    return Cloid.from_str(value)


def parse_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def doctor_command(_args):
    try:
        ensure_deps()
        emit(
            {
                "ok": True,
                "data": {
                    "python_executable": sys.executable,
                    "deps_ok": True,
                },
            }
        )
    except SystemExit:
        raise


def meta_command(args):
    info = build_info(args)
    meta = info.meta()
    universe = []
    for asset in meta["universe"]:
        universe.append(
            {
                "coin": asset["name"],
                "szDecimals": asset["szDecimals"],
            }
        )
    emit({"ok": True, "data": {"universe": universe}})


def all_mids_command(args):
    info = build_info(args)
    emit({"ok": True, "data": info.all_mids()})


def user_state_command(args):
    info = build_info(args)
    _, _, user_address = build_user_address(args)
    emit({"ok": True, "data": info.user_state(user_address), "user": user_address})


def spot_user_state_command(args):
    info = build_info(args)
    _, _, user_address = build_user_address(args)
    emit({"ok": True, "data": info.spot_user_state(user_address), "user": user_address})


def open_orders_command(args):
    info = build_info(args)
    _, _, user_address = build_user_address(args)
    emit({"ok": True, "data": info.frontend_open_orders(user_address), "user": user_address})


def order_status_command(args):
    info = build_info(args)
    _, _, user_address = build_user_address(args)
    if args.get("oid") is not None:
        data = info.query_order_by_oid(user_address, int(args["oid"]))
    elif args.get("cloid"):
        data = info.query_order_by_cloid(user_address, Cloid.from_str(args["cloid"]))
    else:
        emit({"ok": False, "error": "oid or cloid is required"}, 1)
    emit({"ok": True, "data": data, "user": user_address})


def user_fills_by_time_command(args):
    info = build_info(args)
    _, _, user_address = build_user_address(args)
    start_time = int(args.get("start_time") or 0)
    if start_time <= 0:
        emit({"ok": False, "error": "start_time is required"}, 1)
    end_time = args.get("end_time")
    aggregate_by_time = parse_bool(args.get("aggregate_by_time"), False)
    data = info.user_fills_by_time(
        user_address,
        start_time,
        int(end_time) if end_time is not None else None,
        aggregate_by_time,
    )
    emit({"ok": True, "data": data, "user": user_address})


def place_entry_command(args):
    _, _, exchange, user_address = build_exchange(args)
    coin = args["coin"]
    is_buy = parse_bool(args.get("is_buy"))
    size = float(args["size"])
    leverage = int(args.get("leverage") or 10)
    slippage = float(args.get("slippage") or 0.05)
    cloid = maybe_cloid(args.get("cloid"))

    leverage_result = exchange.update_leverage(leverage, coin, is_cross=False)
    order_result = exchange.market_open(
        coin,
        is_buy,
        size,
        slippage=slippage,
        cloid=cloid,
    )

    emit(
        {
            "ok": True,
            "data": {
                "user": user_address,
                "leverage": leverage_result,
                "order": order_result,
            },
        }
    )


def place_tpsl_command(args):
    _, _, exchange, user_address = build_exchange(args)
    coin = args["coin"]
    exit_is_buy = parse_bool(args.get("is_buy"))
    size = float(args["size"])
    target_px = float(args["target_px"])
    stop_px = float(args["stop_px"])
    tp_cloid = maybe_cloid(args.get("tp_cloid"))
    sl_cloid = maybe_cloid(args.get("sl_cloid"))
    grouping = args.get("grouping") or "positionTpsl"

    orders = [
        {
            "coin": coin,
            "is_buy": exit_is_buy,
            "sz": size,
            "limit_px": 0,
            "order_type": {
                "trigger": {
                    "triggerPx": target_px,
                    "isMarket": True,
                    "tpsl": "tp",
                }
            },
            "reduce_only": True,
            "cloid": tp_cloid,
        },
        {
            "coin": coin,
            "is_buy": exit_is_buy,
            "sz": size,
            "limit_px": 0,
            "order_type": {
                "trigger": {
                    "triggerPx": stop_px,
                    "isMarket": True,
                    "tpsl": "sl",
                }
            },
            "reduce_only": True,
            "cloid": sl_cloid,
        },
    ]

    result = exchange.bulk_orders(orders, grouping=grouping)
    emit({"ok": True, "data": result, "user": user_address})


def cancel_orders_command(args):
    _, _, exchange, user_address = build_exchange(args)
    coin = args["coin"]
    oids = [int(value) for value in (args.get("oids") or [])]
    if not oids:
        emit({"ok": False, "error": "oids are required"}, 1)
    result = exchange.bulk_cancel([{"coin": coin, "oid": oid} for oid in oids])
    emit({"ok": True, "data": result, "user": user_address})


def market_close_command(args):
    _, _, exchange, user_address = build_exchange(args)
    coin = args["coin"]
    size = args.get("size")
    slippage = float(args.get("slippage") or 0.05)
    cloid = maybe_cloid(args.get("cloid"))
    result = exchange.market_close(
        coin,
        sz=float(size) if size is not None else None,
        slippage=slippage,
        cloid=cloid,
    )
    emit({"ok": True, "data": result, "user": user_address})


def usd_class_transfer_command(args):
    _, _, exchange, user_address = build_exchange(args)
    amount = float(args["amount"])
    to_perp = parse_bool(args.get("to_perp"), False)
    result = exchange.usd_class_transfer(amount, to_perp)
    emit({"ok": True, "data": result, "user": user_address})


COMMANDS = {
    "doctor": doctor_command,
    "meta": meta_command,
    "all_mids": all_mids_command,
    "user_state": user_state_command,
    "spot_user_state": spot_user_state_command,
    "open_orders": open_orders_command,
    "order_status": order_status_command,
    "user_fills_by_time": user_fills_by_time_command,
    "place_entry": place_entry_command,
    "place_tpsl": place_tpsl_command,
    "cancel_orders": cancel_orders_command,
    "market_close": market_close_command,
    "usd_class_transfer": usd_class_transfer_command,
}


def main():
    command, args = load_args()
    handler = COMMANDS.get(command)
    if handler is None:
        emit({"ok": False, "error": f"Unsupported command: {command}"}, 1)

    try:
        handler(args)
    except Exception as exc:  # noqa: BLE001
        emit({"ok": False, "error": str(exc), "python_executable": sys.executable}, 1)


if __name__ == "__main__":
    main()
