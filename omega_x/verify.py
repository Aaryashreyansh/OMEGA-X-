def verify_report(report: dict, expected_task_ids: set[str]) -> dict:
    errors = []
    results = report.get("results", {})
    if set(results) != expected_task_ids:
        errors.append("Task coverage mismatch")
    summary = report.get("summary", {})
    statuses = [r.get("status") for r in results.values()]
    for key, status in (("completed", "SUCCESS"), ("failed", "FAILED"), ("blocked", "BLOCKED")):
        if summary.get(key) != statuses.count(status):
            errors.append(f"{key} count mismatch")
    if summary.get("total") != len(expected_task_ids):
        errors.append("Total task count mismatch")
    return {"valid": not errors, "errors": errors}
