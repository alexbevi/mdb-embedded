"""Tests for web_app.py security hardening: auth, rate limiting, input validation, CSP.

These tests import web_app once and patch module-level globals to avoid
conflicting embedded database handles.
"""

import json

import pytest

from smongo import MongoClient as EmbeddedClient


@pytest.fixture
def _web_app(tmp_path):
    """Import web_app once per test and rewire to an isolated data directory."""
    try:
        import web_app as wa
    except RuntimeError:
        pytest.skip("web_app requires exclusive access to its local data directory")

    old_client = wa.client
    old_cache = wa._collections_cache.copy()
    old_streams = wa._watch_streams.copy()
    old_key = wa._API_KEY
    old_limiter = wa._limiter

    wa.client = EmbeddedClient(f"local://{tmp_path}/web_sec_redb")
    wa._collections_cache.clear()
    wa._watch_streams.clear()

    yield wa

    wa.client = old_client
    wa._collections_cache.clear()
    wa._collections_cache.update(old_cache)
    wa._watch_streams.clear()
    wa._watch_streams.update(old_streams)
    wa._API_KEY = old_key
    wa._limiter = old_limiter


@pytest.fixture
def app_client(_web_app):
    _web_app._API_KEY = None
    _web_app.app.config["TESTING"] = True
    with _web_app.app.test_client() as client:
        yield client


@pytest.fixture
def authed_web_app(_web_app):
    _web_app._API_KEY = "test-secret-key"
    _web_app.app.config["TESTING"] = True
    return _web_app


@pytest.fixture
def authed_app_client(authed_web_app):
    with authed_web_app.app.test_client() as client:
        yield client


# ── CSP Headers ───────────────────────────────────────────────────────


class TestCSPHeaders:
    def test_script_src_allows_inline(self, app_client):
        resp = app_client.get("/")
        csp = resp.headers.get("Content-Security-Policy", "")
        parts = csp.split("script-src")
        assert len(parts) > 1
        script_directive = parts[1].split(";")[0]
        assert "'self'" in script_directive
        assert "'unsafe-inline'" in script_directive

    def test_has_frame_deny(self, app_client):
        resp = app_client.get("/")
        assert resp.headers.get("X-Frame-Options") == "DENY"

    def test_has_nosniff(self, app_client):
        resp = app_client.get("/")
        assert resp.headers.get("X-Content-Type-Options") == "nosniff"


# ── Input Validation ─────────────────────────────────────────────────


class TestInputValidation:
    def test_reject_oversized_shell_command(self, app_client):
        huge_cmd = "db.users.find(" + "{}" + ")" + "x" * 20000
        resp = app_client.post(
            "/api/shell",
            data=json.dumps({"command": huge_cmd}),
            content_type="application/json",
        )
        assert resp.status_code == 400
        assert "limit" in resp.get_json()["error"].lower()

    def test_reject_bad_collection_name_dollar(self, app_client):
        resp = app_client.post(
            "/api/query",
            data=json.dumps({"coll": "$system", "query": {}}),
            content_type="application/json",
        )
        assert resp.status_code in (400, 500)

    def test_reject_bad_collection_name_null(self, app_client):
        resp = app_client.post(
            "/api/query",
            data=json.dumps({"coll": "foo\x00bar", "query": {}}),
            content_type="application/json",
        )
        assert resp.status_code in (400, 500)

    def test_reject_long_collection_name(self, app_client):
        resp = app_client.post(
            "/api/query",
            data=json.dumps({"coll": "a" * 200, "query": {}}),
            content_type="application/json",
        )
        assert resp.status_code in (400, 500)

    def test_reject_pipeline_too_many_stages(self, app_client):
        pipeline = [{"$match": {}}] * 60
        resp = app_client.post(
            "/api/aggregate",
            data=json.dumps({"coll": "users", "pipeline": pipeline}),
            content_type="application/json",
        )
        assert resp.status_code == 400
        assert "stage limit" in resp.get_json()["error"].lower()

    def test_empty_shell_command_rejected(self, app_client):
        resp = app_client.post(
            "/api/shell",
            data=json.dumps({"command": ""}),
            content_type="application/json",
        )
        assert resp.status_code == 400


# ── Collection Name Validation ───────────────────────────────────────


class TestCollectionNameValidation:
    def test_valid_name_works(self, app_client):
        resp = app_client.get("/api/docs?coll=users")
        assert resp.status_code == 200

    def test_names_with_underscores_hyphens(self, app_client):
        resp = app_client.get("/api/docs?coll=my_collection-v2")
        assert resp.status_code == 200


# ── API Key Auth ─────────────────────────────────────────────────────


class TestAPIKeyAuth:
    def test_no_key_required_when_unset(self, app_client):
        resp = app_client.get("/api/docs?coll=users")
        assert resp.status_code == 200

    def test_rejects_without_key(self, authed_app_client):
        resp = authed_app_client.get("/api/docs?coll=users")
        assert resp.status_code == 401
        assert "Unauthorized" in resp.get_json()["error"]

    def test_accepts_correct_key(self, authed_app_client):
        resp = authed_app_client.get(
            "/api/docs?coll=users",
            headers={"Authorization": "Bearer test-secret-key"},
        )
        assert resp.status_code == 200

    def test_rejects_wrong_key(self, authed_app_client):
        resp = authed_app_client.get(
            "/api/docs?coll=users",
            headers={"Authorization": "Bearer wrong-key"},
        )
        assert resp.status_code == 401

    def test_non_api_routes_not_affected(self, authed_app_client):
        resp = authed_app_client.get("/")
        assert resp.status_code == 200


# ── Rate Limiting ────────────────────────────────────────────────────


class TestRateLimiting:
    def test_rate_limiter_allows_normal_traffic(self, app_client):
        for _ in range(5):
            resp = app_client.get("/api/docs?coll=users")
            assert resp.status_code == 200
