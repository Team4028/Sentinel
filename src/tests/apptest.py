import json
import os
import unittest
import requests
import sqlite3
try:
    import app
except ModuleNotFoundError:
    import src.app as app

def get_tables(conn):
    cursor = conn.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    return [row[0] for row in cursor.fetchall()]


def get_schema(conn, table):
    cursor = conn.execute(f"PRAGMA table_info({table})")
    return cursor.fetchall()


def get_rows(conn, table):
    cursor = conn.execute(f"SELECT * FROM {table}")
    return cursor.fetchall()


def assert_sqlite_equal(db1_path, db2_path):
    conn1 = sqlite3.connect(db1_path)
    conn2 = sqlite3.connect(db2_path)

    try:
        tables1 = get_tables(conn1)
        tables2 = get_tables(conn2)

        assert tables1 == tables2, (
            f"Tables differ:\n{tables1}\n!=\n{tables2}"
        )

        for table in tables1:
            schema1 = get_schema(conn1, table)
            schema2 = get_schema(conn2, table)

            assert schema1 == schema2, (
                f"Schema differs for table '{table}'"
            )

            rows1 = get_rows(conn1, table)
            rows2 = get_rows(conn2, table)

            assert rows1 == rows2, (
                f"Data differs for table '{table}'"
            )

    finally:
        conn1.close()
        conn2.close()

def assert_url_active(url):
    res = requests.get(url)
    assert res.status_code == 200, (
        f"Bad request status code for {url}: {res.status_code}"
    )
    return res

class TestApp(unittest.TestCase):

    BASE_URL = "http://localhost:5001/"

    def __init__(self, methodName = "runTest"):
        super().__init__(methodName)
        app.create_app(False, False, testing_init_info=app.TestingInfo("2025", "2025iri", os.path.join("test-data", "2025-iri-rawdata.csv"))).run(port=5001, use_reloader=False)
        requests.post("http://localhost:5001/reproc")

    def test_processing(self):
        assert_sqlite_equal(os.path.join("dataout", "sentinel.db"), os.path.join("test-data", "data-out-ex.db"))
    
    def test_main(self):
        assert_url_active(self.BASE_URL)
    
    def test_manage_accounts(self):
        assert_url_active(self.BASE_URL + "manage-accounts")
    
    def test_get_user(self):
        res = requests.post(self.BASE_URL + "get-user-display", headers={"id": "admin"})
        self.assertEqual(res.text, f"""
        <style>
            .user-pill {{
                font-family: monospace;
                display: inline-flex;
                align-items: center;
                gap: 8px;

                padding: 6px 12px;
                border-radius: 999px;
                background: #444;
                font-size: 14px;
            }}

            .avatar {{
                width: 24px;
                height: 24px;
                border-radius: 50%;
                text-align: center;
                font-size: 20px;
                line-height: 24px;
            }}

            .name {{
                white-space: nowrap;
            }}
        </style>
        <div class="user-pill">
            <div class="avatar" style="background-color: #b000f0;">Admin</div>
            <span class="name">admin</span>
        </div>
    """)
        
    def test_explore(self):
        assert_url_active(self.BASE_URL + "explore")
    
    def test_edit_file(self):
        assert_url_active(self.BASE_URL + f"edit-file?filepath={os.path.join("dataout", "sentinel.db")}")

    def test_view_file(self):
        assert_url_active(self.BASE_URL + f"view-file?filepath={os.path.join("dataout", "sentinel.db")}")

    def test_jobs(self):
        assert_url_active(self.BASE_URL + "jobs")

    def test_whoami(self):
        assert_url_active(self.BASE_URL + "我是谁")

    def test_sw(self):
        assert_url_active(self.BASE_URL + "service_worker.js")

    def test_pit(self):
        assert_url_active(self.BASE_URL + "pit")

    def test_auton_simple(self):
        assert_url_active(self.BASE_URL + "auton-simple")

    def test_changes(self):
        assert_url_active(self.BASE_URL + "changes")

    def test_health(self):
        res = assert_url_active(self.BASE_URL + "health")
        self.assertIn(res.text, ["No event data", "Sentinel is watching"])

    def test_percent(self):
        assert_url_active(self.BASE_URL + "percent")

    def test_auton_scout(self):
        assert_url_active(self.BASE_URL + "auton-scout")

    def test_edit_yaml(self):
        assert_url_active(self.BASE_URL + "edit-yaml")

    def test_edit_app_conf(self):
        assert_url_active(self.BASE_URL + "edit-app-conf")

    def test_get_config(self):
        res = assert_url_active(self.BASE_URL + "get-config")
        with open(os.path.join("src", "config", "app-config.json"), 'r') as r:
            js = json.load(r)

        self.assertDictEqual(res.json(), js)

    def test_read_log(self):
        assert_url_active(self.BASE_URL + "read-log")

    def test_multi_view(self):
        assert_url_active(self.BASE_URL + "multi-team-view")

    def test_vap(self):
        assert_url_active(self.BASE_URL + "view-all-picklists")

    def test_plist(self):
        assert_url_active(self.BASE_URL + "picklist")
    
    def test_notifications(self):
        assert_url_active(self.BASE_URL + "test-notification")
        res2 = assert_url_active(self.BASE_URL + "notifyq") # just added a not to the q
        self.assertDictEqual(res2.json(),
            {
                "title": "Test",
                "body": "this is a test notification",
                "icon": self.BASE_URL + "static/favicon.ico",
            }
        )
