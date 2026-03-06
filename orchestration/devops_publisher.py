"""
orchestration/devops_publisher.py
-----------------------------------
Azure DevOps Test Run publisher for reconciliation results.

Supports both on-premises ADO Server and cloud ADO Services by using the
``/_apis/test/`` namespace (the ``/_apis/testplan/`` namespace is cloud-only
and returns HTTP 404 on ADO Server).

Workflow — streaming (preferred)
---------------------------------
    publisher = DevOpsPublisher()
    run_id = publisher.open_run(run_name)           # create run once

    for each dataset after reports are written:
        publisher.publish_dataset_result(run_id, ...) # publish immediately

    summary = publisher.close_run(run_id)            # finalise + return URL

Workflow — single-call (legacy)
---------------------------------
    publisher.publish_all(run_name, dataset_results)
"""

import base64
import os
from pathlib import Path

import requests


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _basic_auth(pat: str) -> str:
    """Encode a PAT as an HTTP Basic authorisation header value."""
    token = base64.b64encode(f":{pat}".encode()).decode()
    return f"Basic {token}"


def _assert_ok(response: requests.Response, context: str) -> dict:
    """Raise RuntimeError if the response indicates failure; return JSON body."""
    if not response.ok:
        raise RuntimeError(
            f"ADO API error during [{context}] "
            f"HTTP {response.status_code}: {response.text[:500]}"
        )
    return response.json()


# ---------------------------------------------------------------------------
# Publisher class
# ---------------------------------------------------------------------------

class DevOpsPublisher:
    """
    Publishes reconciliation results to an Azure DevOps Test Run.

    All configuration is read from constructor arguments or from environment
    variables (``ADO_ORG_URL``, ``ADO_PROJECT``, ``ADO_PLAN_ID``,
    ``ADO_SUITE_ID``, ``ADO_PAT``).

    Parameters
    ----------
    org_url  : str, optional   Base ADO organisation URL.
    project  : str, optional   ADO project name.
    plan_id  : int, optional   Test Plan ID.
    suite_id : int, optional   Test Suite ID.
    pat      : str, optional   Personal Access Token.
    logger   : optional        Logger instance (must expose ``.info()``, ``.success()``, ``.warning()``).
    """

    def __init__(
        self,
        org_url: str | None = None,
        project: str | None = None,
        plan_id: int | None = None,
        suite_id: int | None = None,
        pat: str | None = None,
        logger=None,
    ):
        def _clean(value) -> str:
            if not value:
                return ""
            return str(value).strip().strip('"').strip("'")

        self.org_url = _clean(org_url or os.environ.get("ADO_ORG_URL")).rstrip("/")
        self.project = _clean(project or os.environ.get("ADO_PROJECT"))
        self.plan_id = int(plan_id or os.environ.get("ADO_PLAN_ID"))
        self.suite_id = int(suite_id or os.environ.get("ADO_SUITE_ID"))
        self.pat = _clean(pat or os.environ.get("ADO_PAT"))
        self._logger = logger

        if not all([self.org_url, self.project, self.plan_id, self.suite_id, self.pat]):
            raise ValueError("Missing required ADO configuration values.")

        self._auth = _basic_auth(self.pat)
        self._base = f"{self.org_url}/{self.project}/_apis"
        self._api_ver = "api-version=5.0"   # compatible with on-prem + cloud

        self._test_points: list | None = None

    # -------------------------------------------------------------------------
    # Logging helpers
    # -------------------------------------------------------------------------

    def _log(self, msg: str):
        if self._logger:
            self._logger.info(msg)
        else:
            print(f"[DevOps] {msg}")

    def _log_ok(self, msg: str):
        if self._logger:
            self._logger.success(msg)
        else:
            print(f"[DevOps] ✅ {msg}")

    def _log_warn(self, msg: str):
        if self._logger:
            self._logger.warning(msg)
        else:
            print(f"[DevOps] ⚠️  {msg}")

    # -------------------------------------------------------------------------
    # Streaming public API
    # -------------------------------------------------------------------------

    def open_run(self, run_name: str) -> int:
        """
        Validate connection, cache test points, create an InProgress ADO Test Run.

        Returns the new ``run_id``.
        """
        self._log("Validating ADO connection...")
        self._validate_connection()
        self._log_ok("Connected to Azure DevOps")

        self._log("Fetching test points from suite...")
        self._test_points = self._fetch_test_points()

        if not self._test_points:
            raise RuntimeError("No test points found in the configured ADO test suite.")

        self._log(f"Found {len(self._test_points)} test point(s):")
        for pt in self._test_points:
            self._log(f"  PointID={pt['point_id']}  CaseID={pt['testcase_id']}  Title='{pt['title']}'")

        point_ids = [pt["point_id"] for pt in self._test_points]

        self._log("Creating ADO Test Run...")
        run_id = self._create_run(run_name, point_ids)
        self._log_ok(f"Test Run created — run_id={run_id}")
        return run_id

    def publish_dataset_result(
        self,
        run_id: int,
        dataset_name: str,
        status_summary: dict,
        duration_seconds: float,
        csv_path: str | None = None,
        html_path: str | None = None,
    ) -> dict | None:
        """
        Post a single dataset's reconciliation result to an open ADO run.

        Parameters
        ----------
        run_id           : int    Open ADO run identifier.
        dataset_name     : str    Name used to match a test point (substring match).
        status_summary   : dict   Keys: MATCH, MISMATCH, MISSING_IN_DEST, EXTRA_IN_DEST.
        duration_seconds : float  Processing duration.
        csv_path         : str    Path to combined CSV (attached as artefact).
        html_path        : str    Path to HTML report (attached as artefact).

        Returns
        -------
        dict or None
            Created ADO result record, or ``None`` if no matching test point found.
        """
        if self._test_points is None:
            raise RuntimeError("Call open_run() before publish_dataset_result().")

        point = self._match_test_point(dataset_name, self._test_points)
        if not point:
            self._log_warn(f"No test point matched '{dataset_name}' — skipping")
            return None

        passed = status_summary.get("MATCH", 0)
        mismatches = status_summary.get("MISMATCH", 0)
        missing = status_summary.get("MISSING_IN_DEST", 0)
        extra = status_summary.get("EXTRA_IN_DEST", 0)
        total = passed + mismatches + missing + extra
        pct = round((passed / total * 100), 1) if total else 0.0

        outcome = "Passed" if (mismatches == 0 and missing == 0) else "Failed"

        comment = (
            f"Match: {passed} ({pct}%)"
            + (f", Mismatch: {mismatches}" if mismatches else "")
            + (f", Missing: {missing}" if missing else "")
            + (f", Extra: {extra}" if extra else "")
        )

        payload = {
            "testPoint": {"id": str(point["point_id"])},
            "testCase": {"id": str(point["testcase_id"])},
            "testCaseTitle": point["title"],
            "outcome": outcome,
            "durationInMs": int(duration_seconds * 1000),
            "comment": comment,
            "state": "Completed",
        }

        results = self._add_results(run_id, [payload])
        if not results:
            self._log_warn(f"ADO returned no result record for '{dataset_name}'")
            return None

        result = results[0]
        result_id = result["id"]
        self._log_ok(f"Result posted — '{dataset_name}' | {outcome} | {pct}% match | result_id={result_id}")

        if csv_path:
            self._safe_attach(run_id, result_id, csv_path, dataset_name, "CSV")
        if html_path:
            self._safe_attach(run_id, result_id, html_path, dataset_name, "HTML report")

        return result

    def close_run(self, run_id: int) -> dict:
        """Mark the ADO run as Completed and return a summary dict with the run URL."""
        self._complete_run(run_id)
        run_url = f"{self.org_url}/{self.project}/_testManagement/runs?runId={run_id}"
        self._log_ok(f"Test Run completed — {run_url}")
        return {"run_id": run_id, "run_url": run_url}

    # -------------------------------------------------------------------------
    # Legacy single-call API
    # -------------------------------------------------------------------------

    def publish_all(self, run_name: str, dataset_results: list) -> dict | None:
        """
        Upload all dataset results in a single call (delegates to streaming API).

        Each item in ``dataset_results`` must contain:
            dataset_name, status_summary, duration_seconds, csv_path, html_path
        """
        run_id = self.open_run(run_name)
        for dr in dataset_results:
            self.publish_dataset_result(
                run_id=run_id,
                dataset_name=dr["dataset_name"],
                status_summary=dr.get("status_summary", {}),
                duration_seconds=dr.get("duration_seconds", 0),
                csv_path=dr.get("csv_path"),
                html_path=dr.get("html_path"),
            )
        return self.close_run(run_id)

    # -------------------------------------------------------------------------
    # Private helpers
    # -------------------------------------------------------------------------

    def _headers(self) -> dict:
        return {"Authorization": self._auth, "Content-Type": "application/json"}

    def _validate_connection(self) -> None:
        url = f"{self.org_url}/_apis/connectionData?api-version=6.0-preview"
        resp = requests.get(url, headers=self._headers())
        if not resp.ok:
            raise RuntimeError(
                f"ADO connection failed — HTTP {resp.status_code}: {resp.text[:200]}"
            )

    def _fetch_test_points(self) -> list:
        """Paginate through test points using the /_apis/test/ namespace."""
        points = []
        skip, page_size = 0, 100

        while True:
            url = (
                f"{self._base}/test/Plans/{self.plan_id}"
                f"/Suites/{self.suite_id}/points"
                f"?{self._api_ver}&$top={page_size}&$skip={skip}"
            )
            data = _assert_ok(requests.get(url, headers=self._headers()), "fetch test points")
            batch = data.get("value", [])
            if not batch:
                break

            for item in batch:
                tc = item.get("testCase", {})
                points.append({
                    "point_id": item["id"],
                    "testcase_id": tc.get("id"),
                    "title": tc.get("name"),
                })

            if len(batch) < page_size:
                break
            skip += page_size

        return points

    def _match_test_point(self, dataset_name: str, points: list) -> dict | None:
        """Return the first test point whose title contains ``dataset_name`` (case-insensitive)."""
        needle = dataset_name.lower()
        for pt in points:
            if pt["title"] and needle in pt["title"].lower():
                return pt
        return None

    def _create_run(self, run_name: str, point_ids: list) -> int:
        url = f"{self._base}/test/runs?{self._api_ver}"
        body = {
            "name": run_name,
            "plan": {"id": str(self.plan_id)},
            "automated": True,
            "state": "InProgress",
        }
        data = _assert_ok(
            requests.post(url, headers=self._headers(), json=body),
            "create test run",
        )
        return data["id"]

    def _add_results(self, run_id: int, results: list) -> list:
        url = f"{self._base}/test/runs/{run_id}/results?{self._api_ver}"
        data = _assert_ok(
            requests.post(url, headers=self._headers(), json=results),
            "add test results",
        )
        return data.get("value", [])

    def _safe_attach(self, run_id: int, result_id: int, file_path: str, label: str, kind: str):
        path = Path(file_path)
        if not path.exists():
            self._log_warn(f"{kind} not found for '{label}' — {path}")
            return
        try:
            self._log(f"Attaching {kind} for '{label}'...")
            self._attach_file(run_id, result_id, path)
            self._log_ok(f"{kind} attached for '{label}'")
        except Exception as exc:
            self._log_warn(f"Could not attach {kind} for '{label}': {exc}")

    def _attach_file(self, run_id: int, result_id: int, file_path: Path) -> None:
        with open(file_path, "rb") as fh:
            encoded = base64.b64encode(fh.read()).decode()

        url = (
            f"{self._base}/test/runs/{run_id}"
            f"/results/{result_id}/attachments?{self._api_ver}"
        )
        body = {
            "attachmentType": "GeneralAttachment",
            "fileName": file_path.name,
            "stream": encoded,
        }
        _assert_ok(requests.post(url, headers=self._headers(), json=body), "attach file")

    def _complete_run(self, run_id: int) -> None:
        url = f"{self._base}/test/runs/{run_id}?{self._api_ver}"
        _assert_ok(
            requests.patch(url, headers=self._headers(), json={"state": "Completed"}),
            "complete run",
        )
