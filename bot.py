@app.route("/external-check")
def external_check():
    """
    Защищённый endpoint для независимого внешнего контроля.
    """

    print("EXTERNAL_CHECK: request received", flush=True)

    if not EXTERNAL_CHECK_TOKEN:
        print("EXTERNAL_CHECK: token is not configured", flush=True)
        return jsonify({
            "ok": False,
            "reason": "EXTERNAL_CHECK_TOKEN не настроен",
        }), 503

    supplied_token = request.headers.get(
        "X-External-Check-Token",
        "",
    )

    if not supplied_token or not secrets.compare_digest(
        supplied_token,
        EXTERNAL_CHECK_TOKEN,
    ):
        print("EXTERNAL_CHECK: unauthorized", flush=True)
        return jsonify({
            "ok": False,
            "reason": "Unauthorized",
        }), 401

    print("EXTERNAL_CHECK: token OK, starting self-check", flush=True)

    ok, reason = perform_external_self_check()

    print(f"EXTERNAL_CHECK: self-check finished: ok={ok}", flush=True)

    payload = {
        "ok": ok,
        "reason": reason,
        "checked_at": now_utc().astimezone(
            KYIV_TZ
        ).isoformat(),
    }

    return jsonify(payload), 200 if ok else 503
