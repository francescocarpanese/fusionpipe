"""
Tests for the node-group API routes in pipeline_handle.py.

Uses FastAPI TestClient with a dependency override to inject a fresh per-test
PostgreSQL database, consistent with the rest of the test suite.
"""

import pytest
from fastapi.testclient import TestClient

from fusionpipe.api.main import app
from fusionpipe.api.routes.pipeline_handle import get_db
from fusionpipe.utils import db_utils, pip_utils


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _override_get_db(conn):
    """Return a FastAPI dependency override that yields *conn*."""
    def _inner():
        yield conn
    return _inner


def _setup(cur, conn):
    """
    Create a project + pipeline + two nodes and commit.
    Returns (pipeline_id, [node_id_1, node_id_2]).
    """
    project_id = pip_utils.generate_project_id()
    db_utils.add_project(cur, project_id=project_id)
    pipeline_id = pip_utils.generate_pip_id()
    db_utils.add_pipeline_to_pipelines(cur, pipeline_id=pipeline_id, tag="test", project_id=project_id)
    node_ids = []
    for _ in range(2):
        nid = pip_utils.generate_node_id()
        db_utils.add_node_to_nodes(cur, node_id=nid, project_id=project_id, folder_path=f"/tmp/{nid}")
        db_utils.add_node_to_pipeline(cur, node_id=nid, pipeline_id=pipeline_id)
        node_ids.append(nid)
    conn.commit()
    return pipeline_id, node_ids


@pytest.fixture
def client(pg_test_db):
    """TestClient with the DB dependency wired to the per-test pg database."""
    conn = pg_test_db
    cur = db_utils.init_db(conn)
    conn.commit()

    app.dependency_overrides[get_db] = _override_get_db(conn)
    with TestClient(app) as c:
        c._cur = cur      # expose cursor for direct DB assertions
        c._conn = conn
        yield c

    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# POST /create_node_group
# ---------------------------------------------------------------------------

class TestCreateNodeGroup:

    def test_creates_group_and_returns_group_id(self, client):
        cur, conn = client._cur, client._conn
        pipeline_id, node_ids = _setup(cur, conn)

        payload = {
            "pipeline_id": pipeline_id,
            "node_ids": node_ids,
            "tag": "my group",
            "pos_x": 10.0,
            "pos_y": 20.0,
            "width": 300.0,
            "height": 200.0,
        }
        resp = client.post("/create_node_group", json=payload)
        assert resp.status_code == 200, resp.text

        data = resp.json()
        assert "group_id" in data
        assert data["group_id"].startswith("ng_")

        # Verify persistence in DB
        group_id = data["group_id"]
        groups = db_utils.get_groups_for_pipeline(cur, pipeline_id)
        assert len(groups) == 1
        g = groups[0]
        assert g["group_id"] == group_id
        assert g["tag"] == "my group"
        assert set(g["node_ids"]) == set(node_ids)

    def test_returns_400_when_pipeline_id_missing(self, client):
        cur, conn = client._cur, client._conn
        _, node_ids = _setup(cur, conn)

        resp = client.post("/create_node_group", json={"node_ids": node_ids})
        assert resp.status_code == 400

    def test_returns_400_when_node_ids_missing(self, client):
        cur, conn = client._cur, client._conn
        pipeline_id, _ = _setup(cur, conn)

        resp = client.post("/create_node_group", json={"pipeline_id": pipeline_id})
        assert resp.status_code == 400

    def test_returns_400_when_node_already_in_group(self, client):
        """A node cannot belong to two groups simultaneously."""
        cur, conn = client._cur, client._conn
        pipeline_id, node_ids = _setup(cur, conn)

        # Create first group successfully
        resp = client.post(
            "/create_node_group",
            json={"pipeline_id": pipeline_id, "node_ids": [node_ids[0]]},
        )
        assert resp.status_code == 200

        # Trying to add the same node to a second group must fail
        resp2 = client.post(
            "/create_node_group",
            json={"pipeline_id": pipeline_id, "node_ids": [node_ids[0], node_ids[1]]},
        )
        assert resp2.status_code == 400
        assert node_ids[0] in resp2.json()["detail"]


# ---------------------------------------------------------------------------
# DELETE /delete_node_group/{group_id}
# ---------------------------------------------------------------------------

class TestDeleteNodeGroup:

    def test_deletes_group(self, client):
        cur, conn = client._cur, client._conn
        pipeline_id, node_ids = _setup(cur, conn)

        # Create group via route
        resp = client.post(
            "/create_node_group",
            json={"pipeline_id": pipeline_id, "node_ids": node_ids},
        )
        group_id = resp.json()["group_id"]

        # Delete it
        del_resp = client.delete(f"/delete_node_group/{group_id}")
        assert del_resp.status_code == 200

        # Verify it is gone
        groups = db_utils.get_groups_for_pipeline(cur, pipeline_id)
        assert all(g["group_id"] != group_id for g in groups)

        # Nodes must still exist
        for nid in node_ids:
            cur.execute("SELECT COUNT(*) FROM nodes WHERE node_id = %s", (nid,))
            assert cur.fetchone()[0] == 1


# ---------------------------------------------------------------------------
# POST /update_node_group_collapse/{group_id}
# ---------------------------------------------------------------------------

class TestUpdateNodeGroupCollapse:

    def _create_group(self, client, pipeline_id, node_ids):
        resp = client.post(
            "/create_node_group",
            json={"pipeline_id": pipeline_id, "node_ids": node_ids},
        )
        return resp.json()["group_id"]

    def test_sets_collapsed_true(self, client):
        cur, conn = client._cur, client._conn
        pipeline_id, node_ids = _setup(cur, conn)
        group_id = self._create_group(client, pipeline_id, node_ids)

        resp = client.post(f"/update_node_group_collapse/{group_id}", json={"collapsed": True})
        assert resp.status_code == 200

        cur.execute("SELECT collapsed FROM node_groups WHERE group_id = %s", (group_id,))
        assert cur.fetchone()[0] is True

    def test_sets_collapsed_false(self, client):
        cur, conn = client._cur, client._conn
        pipeline_id, node_ids = _setup(cur, conn)
        group_id = self._create_group(client, pipeline_id, node_ids)

        # Collapse first
        client.post(f"/update_node_group_collapse/{group_id}", json={"collapsed": True})

        # Then expand
        resp = client.post(f"/update_node_group_collapse/{group_id}", json={"collapsed": False})
        assert resp.status_code == 200

        cur.execute("SELECT collapsed FROM node_groups WHERE group_id = %s", (group_id,))
        assert cur.fetchone()[0] is False

    def test_returns_400_when_collapsed_missing(self, client):
        cur, conn = client._cur, client._conn
        pipeline_id, node_ids = _setup(cur, conn)
        group_id = self._create_group(client, pipeline_id, node_ids)

        resp = client.post(f"/update_node_group_collapse/{group_id}", json={})
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /update_node_group_position/{group_id}
# ---------------------------------------------------------------------------

class TestUpdateNodeGroupPosition:

    def _create_group(self, client, pipeline_id, node_ids):
        resp = client.post(
            "/create_node_group",
            json={"pipeline_id": pipeline_id, "node_ids": node_ids},
        )
        return resp.json()["group_id"]

    def test_updates_position_and_dimensions(self, client):
        cur, conn = client._cur, client._conn
        pipeline_id, node_ids = _setup(cur, conn)
        group_id = self._create_group(client, pipeline_id, node_ids)

        resp = client.post(
            f"/update_node_group_position/{group_id}",
            json={"pos_x": 99.5, "pos_y": 88.0, "width": 600.0, "height": 450.0},
        )
        assert resp.status_code == 200

        cur.execute(
            "SELECT pos_x, pos_y, width, height FROM node_groups WHERE group_id = %s",
            (group_id,),
        )
        row = cur.fetchone()
        assert float(row[0]) == 99.5
        assert float(row[1]) == 88.0
        assert float(row[2]) == 600.0
        assert float(row[3]) == 450.0

    def test_uses_defaults_when_fields_absent(self, client):
        """Omitting fields should fall back to the route's default values (0, 0, 400, 300)."""
        cur, conn = client._cur, client._conn
        pipeline_id, node_ids = _setup(cur, conn)
        group_id = self._create_group(client, pipeline_id, node_ids)

        resp = client.post(f"/update_node_group_position/{group_id}", json={})
        assert resp.status_code == 200

        cur.execute(
            "SELECT pos_x, pos_y, width, height FROM node_groups WHERE group_id = %s",
            (group_id,),
        )
        row = cur.fetchone()
        assert float(row[0]) == 0.0
        assert float(row[1]) == 0.0
        assert float(row[2]) == 400.0
        assert float(row[3]) == 300.0


# ---------------------------------------------------------------------------
# POST /update_node_group_tag/{group_id}
# ---------------------------------------------------------------------------

class TestUpdateNodeGroupTag:

    def _create_group(self, client, pipeline_id, node_ids, tag="initial"):
        resp = client.post(
            "/create_node_group",
            json={"pipeline_id": pipeline_id, "node_ids": node_ids, "tag": tag},
        )
        return resp.json()["group_id"]

    def test_updates_tag(self, client):
        cur, conn = client._cur, client._conn
        pipeline_id, node_ids = _setup(cur, conn)
        group_id = self._create_group(client, pipeline_id, node_ids, tag="before")

        resp = client.post(
            f"/update_node_group_tag/{group_id}",
            json={"tag": "after"},
        )
        assert resp.status_code == 200

        cur.execute("SELECT tag FROM node_groups WHERE group_id = %s", (group_id,))
        assert cur.fetchone()[0] == "after"

    def test_returns_400_when_tag_missing(self, client):
        cur, conn = client._cur, client._conn
        pipeline_id, node_ids = _setup(cur, conn)
        group_id = self._create_group(client, pipeline_id, node_ids)

        resp = client.post(f"/update_node_group_tag/{group_id}", json={})
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Round-trip: create → update → get pipeline dict → delete
# ---------------------------------------------------------------------------

def test_node_group_full_roundtrip(client):
    """
    Full lifecycle: create group, update its tag and position, verify it
    appears in the pipeline dict, then delete it and confirm it is gone.
    """
    cur, conn = client._cur, client._conn
    pipeline_id, node_ids = _setup(cur, conn)

    # Create
    create_resp = client.post(
        "/create_node_group",
        json={
            "pipeline_id": pipeline_id,
            "node_ids": node_ids,
            "tag": "v1",
            "pos_x": 0.0,
            "pos_y": 0.0,
            "width": 400.0,
            "height": 300.0,
        },
    )
    assert create_resp.status_code == 200
    group_id = create_resp.json()["group_id"]

    # Update tag
    assert client.post(f"/update_node_group_tag/{group_id}", json={"tag": "v2"}).status_code == 200

    # Update position
    assert client.post(
        f"/update_node_group_position/{group_id}",
        json={"pos_x": 50.0, "pos_y": 60.0, "width": 500.0, "height": 400.0},
    ).status_code == 200

    # Collapse
    assert client.post(
        f"/update_node_group_collapse/{group_id}", json={"collapsed": True}
    ).status_code == 200

    # Verify via DB helper
    groups = db_utils.get_groups_for_pipeline(cur, pipeline_id)
    g = next(x for x in groups if x["group_id"] == group_id)
    assert g["tag"] == "v2"
    assert g["pos_x"] == 50.0
    assert g["pos_y"] == 60.0
    assert g["width"] == 500.0
    assert g["height"] == 400.0
    assert g["collapsed"] is True
    assert set(g["node_ids"]) == set(node_ids)

    # Delete
    assert client.delete(f"/delete_node_group/{group_id}").status_code == 200

    groups_after = db_utils.get_groups_for_pipeline(cur, pipeline_id)
    assert all(g["group_id"] != group_id for g in groups_after)
