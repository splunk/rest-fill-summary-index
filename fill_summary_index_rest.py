#!/usr/bin/env python3
"""
Backfill summary index data by dispatching saved searches at their historical
scheduled times using Splunk REST API endpoints.
"""

import atexit
import base64
import getpass
import glob
import hashlib
import json
import math
import os
import ssl
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen


owner: str = "nobody"
trigger: bool = True
sleep: float = 5.0
maxjobs: int = 1
indexarg: Optional[str] = None
dedup: bool = False
reverseorder: bool = False
sched_start_time: Optional[int] = None
sched_end_time: Optional[int] = None
showprogress: bool = False
verify_ssl: bool = False

app: Optional[str] = None
host: Optional[str] = None
et: Optional[str] = None
lt: Optional[str] = None
user: Optional[str] = None
password: Optional[str] = None
session_key: Optional[str] = None

ssname_list = []

timefield = "search_now"
namefield = "source"

dedupsearch = 'search splunk_server=local index=$index$ $namefield$="$name$" | stats count by $timefield$'
distdedupsearch = 'search index=$index$ $namefield$="$name$" | stats count by $timefield$'


class SplunkRestError(Exception):
    pass


def print_error(msg, code=1):
    print(msg)
    sys.exit(code)


def print_usage():
    print(
        """
Description:
  This script backfills summary indexes that are populated by saved searches by
  executing those saved searches as they would have run at scheduled times for
  a provided time range.

Usage:
  python fill_summary_index_rest.py [OPTIONS]

Core options:
  -host <string>          Splunk management host for REST API (required for Splunk Cloud).
                          Example: https://prd-p-xxxxx.splunkcloud.com:8089
  -et <string>            Earliest time (required). Epoch or relative time string.
  -lt <string>            Latest time (required). Epoch or relative time string.
  -app <string>           App context.
  -owner <string>         Owner context (default: nobody).

Saved search selection:
  -name <string>          One saved search name. Can be repeated.
  -names <string>         Comma-separated saved search names.
  -namefile <file>        File with names, one per line. Text after # is ignored.
                          Use "*" to include all enabled, scheduled summary-index searches.

Auth:
  -auth <string>          Username or username:password
  -sk <string>            Session key (skip username/password login)

Execution:
  -j <int>                Max concurrent jobs (1..16, default 1)
  -sleep <float>          Poll sleep in seconds (default 5)
  -trigger <boolean>      Trigger actions while dispatching (default true)
  -dedup <boolean>        Skip scheduled times that already exist in summary index
  -reverseorder <boolean> Run from latest to earliest
  -showprogress <boolean> Show per-job progress while polling

Scheduling window:
  -sched_start_time <int> Start time in HHMM local time
  -sched_end_time <int>   End time in HHMM local time

Advanced:
  -index <string>         Summary index name override
  -dedupsearch <string>   Dedup search template
  -namefield <string>     Field containing saved search name in summary events
  -timefield <string>     Field containing scheduled timestamp in summary events
  -nolocal <boolean>      Use distributed dedup search template
  -insecure <boolean>     Disable TLS certificate verification

Boolean values:
  true values: 1, t, true, yes
  false values: 0, f, false, no
        """
    )
    sys.exit(0)


def parse_bool(opt, value) -> bool:
    v = str(value).strip().lower()
    if v in {"1", "t", "true", "yes"}:
        return True
    if v in {"0", "f", "false", "no"}:
        return False
    print_error("Invalid boolean value '%s' for %s option" % (value, opt))
    raise ValueError("unreachable")


def validate_hhmm(opt, value):
    h = int(value / 100)
    m = int(value % 100)
    if h > 23:
        print_error("hours > 23. Use military time format for %s." % opt)
    if m > 59:
        print_error("minutes > 59. Use military time format for %s." % opt)


def normalize_host(input_host):
    raw = input_host.strip()
    if not raw:
        return raw

    if not raw.startswith("http://") and not raw.startswith("https://"):
        raw = "https://" + raw

    parsed = urlparse(raw)
    if not parsed.hostname:
        print_error("Invalid -host value '%s'" % input_host)

    scheme = parsed.scheme or "https"
    port = parsed.port or 8089
    if port == 8000:
        print(
            "WARNING: Port 8000 is the Splunk Web UI port and does not serve the "
            "management REST API. The management API is typically on port 8089.\n"
            "  Suggested: -host %s://%s:8089" % (scheme, parsed.hostname)
        )
    if port == 8089 and scheme == "http":
        print(
            "WARNING: Splunk management API on port 8089 requires HTTPS. "
            "Upgrading scheme from http to https automatically."
        )
        scheme = "https"
    return "%s://%s:%d" % (scheme, parsed.hostname, port)


def extract_json_value(data, candidates):
    for key in candidates:
        if key in data and data[key] not in (None, ""):
            return data[key]
    return None


def extract_content(data):
    if isinstance(data, dict):
        if "entry" in data and isinstance(data["entry"], list) and data["entry"]:
            first = data["entry"][0]
            if isinstance(first, dict):
                if "content" in first and isinstance(first["content"], dict):
                    return first["content"]
                return first
        if "content" in data and isinstance(data["content"], dict):
            return data["content"]
    return data if isinstance(data, dict) else {}


def get_scheduled_times_from_payload(data):
    # Try direct key first.
    if isinstance(data, dict) and "scheduled_times" in data and isinstance(data["scheduled_times"], list):
        return data["scheduled_times"]

    if isinstance(data, dict) and "entry" in data and isinstance(data["entry"], list):
        for entry in data["entry"]:
            if not isinstance(entry, dict):
                continue
            content = entry.get("content") if isinstance(entry.get("content"), dict) else entry
            if isinstance(content, dict) and "scheduled_times" in content and isinstance(content["scheduled_times"], list):
                return content["scheduled_times"]

    return []


def create_lock_file(hostname, appname):
    token = "%s|%s" % (hostname, appname)
    digest = hashlib.sha1(token.encode("utf-8")).hexdigest()[:10]
    lock_path = os.path.join(tempfile.gettempdir(), "fsidx_%s.lock" % digest)

    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("ascii"))
        os.close(fd)
    except FileExistsError:
        print_error("An instance of fill_summary_index_rest is already running for app=%s and host=%s" % (appname, hostname))

    def cleanup():
        if os.path.exists(lock_path):
            try:
                os.remove(lock_path)
            except OSError:
                pass

    atexit.register(cleanup)


def enforce_schedule_window(start_hhmm, end_hhmm):
    if start_hhmm is None:
        return

    timenow = time.localtime()
    timenow_min = timenow.tm_hour * 60 + timenow.tm_min
    start_min = 60 * int(start_hhmm / 100) + int(start_hhmm % 100)

    if end_hhmm is None:
        end_min = None
    else:
        end_min = 60 * int(end_hhmm / 100) + int(end_hhmm % 100)

    sleep_secs = 0
    if start_min > timenow_min:
        if end_min is None or end_min > start_min or (end_min < start_min and timenow_min > end_min):
            sleep_secs = 60 * (start_min - timenow_min)
    elif start_min < timenow_min:
        if end_min is not None and end_min > start_min:
            sleep_secs = 60 * ((24 * 60) - timenow_min + start_min)

    if sleep_secs > 0:
        print("Pausing %d seconds until schedule window opens" % sleep_secs)
        time.sleep(sleep_secs)


class SplunkRestClient:
    def __init__(self, base_url, verify=True):
        self.base_url = base_url.rstrip("/")
        self.verify = verify
        self._session_key = None
        self._basic_auth_header = None

    def set_session_key(self, key):
        self._session_key = key

    def set_basic_auth(self, username, pwd):
        token = ("%s:%s" % (username, pwd)).encode("utf-8")
        self._basic_auth_header = "Basic %s" % base64.b64encode(token).decode("ascii")

    def _headers(self):
        h = {}
        if self._basic_auth_header:
            h["Authorization"] = self._basic_auth_header
        elif self._session_key:
            h["Authorization"] = "Splunk %s" % self._session_key
        return h

    def _request(self, method, path, params=None, data=None):
        url = self.base_url + path
        params = dict(params or {})
        params.setdefault("output_mode", "json")

        query = urlencode(params)
        full_url = url + ("?" + query if query else "")

        payload = None
        if data is not None:
            payload = urlencode(data).encode("utf-8")

        headers = self._headers()
        if payload is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        req = Request(full_url, data=payload, method=method.upper(), headers=headers)
        ssl_context = None
        if not self.verify:
            ssl_context = ssl._create_unverified_context()

        try:
            with urlopen(req, timeout=60, context=ssl_context) as resp:
                body = resp.read().decode("utf-8")
        except HTTPError as exc:
            msg = exc.read().decode("utf-8", errors="replace").strip()
            if msg.lstrip().startswith(("<!DOCTYPE", "<html", "<HTML")):
                parsed_url = urlparse(full_url)
                hint = ""
                if parsed_url.port == 8000:
                    hint = " (Port 8000 is the Splunk Web UI — use port 8089 for the management REST API)"
                raise SplunkRestError(
                    "HTTP %d for %s: server returned HTML instead of JSON%s"
                    % (exc.code, full_url, hint)
                )
            if len(msg) > 600:
                msg = msg[:600] + "..."
            raise SplunkRestError("HTTP %d for %s: %s" % (exc.code, full_url, msg))
        except URLError as exc:
            reason = exc.reason if hasattr(exc, "reason") else exc
            reason_str = str(reason)
            if isinstance(reason, ConnectionResetError) or "Connection reset" in reason_str:
                parsed_url = urlparse(full_url)
                hint = ""
                if parsed_url.scheme == "http" and parsed_url.port in (8089, None):
                    hint = " (Splunk management API requires HTTPS — use https:// instead of http://)"
                raise SplunkRestError(
                    "Connection reset by peer for %s%s" % (full_url, hint)
                )
            if "CERTIFICATE_VERIFY_FAILED" in reason_str or "certificate verify failed" in reason_str:
                raise SplunkRestError(
                    "SSL certificate verification failed for %s. "
                    "If using a self-signed certificate, add -insecure true to skip verification." % full_url
                )
            raise SplunkRestError("REST request failed for %s: %s" % (full_url, str(exc)))

        try:
            return json.loads(body)
        except ValueError:
            return {}

    def list_saved_searches(self, appname, ownerval):
        return self._request(
            "GET",
            "/servicesNS/%s/%s/saved/searches" % (quote(ownerval, safe=""), quote(appname, safe="")),
            params={"count": 0},
        )

    def get_saved_search(self, appname, ownerval, search_name):
        return self._request(
            "GET",
            "/servicesNS/%s/%s/saved/searches/%s"
            % (quote(ownerval, safe=""), quote(appname, safe=""), quote(search_name, safe="")),
        )

    def get_scheduled_times(self, appname, ownerval, search_name, earliest, latest):
        return self._request(
            "GET",
            "/servicesNS/%s/%s/saved/searches/%s/scheduled_times"
            % (quote(ownerval, safe=""), quote(appname, safe=""), quote(search_name, safe="")),
            params={"earliest_time": earliest, "latest_time": latest},
        )

    def dispatch_search(self, appname, ownerval, search):
        data = self._request(
            "POST",
            "/servicesNS/%s/%s/search/jobs" % (quote(ownerval, safe=""), quote(appname, safe="")),
            data={"search": search},
        )
        sid = extract_json_value(data, ["sid", "id"])
        if sid:
            return sid
        if "entry" in data and data["entry"]:
            e = data["entry"][0]
            sid = extract_json_value(e, ["sid", "name", "id"])
            if sid:
                return sid
        raise SplunkRestError("Could not find sid in dispatch search response")

    def dispatch_saved_search(self, appname, ownerval, search_name, now_utc, trigger_actions):
        data = self._request(
            "POST",
            "/servicesNS/%s/%s/saved/searches/%s/dispatch"
            % (quote(ownerval, safe=""), quote(appname, safe=""), quote(search_name, safe="")),
            data={"dispatch.now": str(now_utc), "trigger_actions": trigger_actions},
        )
        sid = extract_json_value(data, ["sid", "id"])
        if sid:
            return sid
        if "entry" in data and data["entry"]:
            e = data["entry"][0]
            sid = extract_json_value(e, ["sid", "name", "id"])
            if sid:
                return sid
        raise SplunkRestError("Could not find sid in saved search dispatch response")

    def get_job_state(self, appname, ownerval, sid):
        data = self._request(
            "GET",
            "/servicesNS/%s/%s/search/jobs/%s"
            % (quote(ownerval, safe=""), quote(appname, safe=""), quote(sid, safe="")),
        )
        content = extract_content(data)

        raw_done = str(content.get("isDone", "0")).lower()
        is_done = raw_done in {"1", "true", "t"}

        done_progress = 0.0
        try:
            done_progress = float(content.get("doneProgress", 0.0))
        except (TypeError, ValueError):
            done_progress = 0.0

        return is_done, done_progress

    def get_job_results(self, appname, ownerval, sid):
        data = self._request(
            "GET",
            "/servicesNS/%s/%s/search/jobs/%s/results"
            % (quote(ownerval, safe=""), quote(appname, safe=""), quote(sid, safe="")),
            params={"count": 0},
        )
        if "results" in data and isinstance(data["results"], list):
            return data["results"]
        if "entry" in data and isinstance(data["entry"], list):
            rows = []
            for entry in data["entry"]:
                if isinstance(entry, dict):
                    if "content" in entry and isinstance(entry["content"], dict):
                        rows.append(entry["content"])
                    else:
                        rows.append(entry)
            return rows
        return []


def update_job_list(client, appname, ownerval, jobs, show_progress=False, trigger_actions=True):
    new_jobs = []
    for sid in jobs:
        try:
            is_done, progress = client.get_job_state(appname, ownerval, sid)
            if is_done:
                if not trigger_actions:
                    print(" ... job '%s' finished (not triggering actions)" % sid)
                else:
                    print(" ... job '%s' finished" % sid)
            else:
                new_jobs.append(sid)
                if show_progress:
                    print(" ... job '%s' progress: %.1f%%" % (sid, (progress * 100.0)))
        except Exception as exc:  # noqa: BLE001
            print(" ... job '%s' FAILED: %s" % (sid, str(exc)))
    return new_jobs


def parse_args():
    global app, owner, et, lt, user, password, session_key, trigger
    global sleep, maxjobs, indexarg, dedup, reverseorder, sched_start_time
    global sched_end_time, showprogress, dedupsearch, namefield, timefield
    global host, verify_ssl

    if len(sys.argv) == 2 and sys.argv[1] in {"-h", "-help", "help", "--help", "--h"}:
        print_usage()

    i = 1
    while i < len(sys.argv):
        opt = sys.argv[i]
        if i + 1 >= len(sys.argv):
            print_error("Missing value for option '%s'" % opt)
        val = sys.argv[i + 1]
        i += 2

        if opt == "-app":
            app = val
        elif opt == "-host":
            host = val
        elif opt == "-name":
            ssname_list.append(val)
        elif opt == "-names":
            ssname_list.extend([x for x in val.split(",") if x])
        elif opt == "-namefile":
            with open(val, "r", encoding="utf-8") as handle:
                for line in handle:
                    line = (line.split("#", 1))[0].strip()
                    if line:
                        ssname_list.append(line)
        elif opt == "-owner":
            owner = val
        elif opt == "-et":
            et = val
        elif opt == "-lt":
            lt = val
        elif opt == "-index":
            indexarg = val
        elif opt == "-sk":
            session_key = val
        elif opt == "-auth":
            parts = val.split(":", 1)
            user = parts[0]
            if len(parts) == 2:
                password = parts[1]
        elif opt == "-trigger":
            trigger = parse_bool(opt, val)
        elif opt == "-sleep":
            sleep = float(val)
        elif opt == "-dedup":
            dedup = parse_bool(opt, val)
        elif opt == "-reverseorder":
            reverseorder = parse_bool(opt, val)
        elif opt == "-sched_start_time":
            sched_start_time = int(val)
            validate_hhmm("sched_start_time", sched_start_time)
        elif opt == "-sched_end_time":
            sched_end_time = int(val)
            validate_hhmm("sched_end_time", sched_end_time)
        elif opt == "-showprogress":
            showprogress = parse_bool(opt, val)
        elif opt == "-dedupsearch":
            dedupsearch = val
        elif opt == "-namefield":
            namefield = val
        elif opt == "-timefield":
            timefield = val
        elif opt == "-nolocal":
            if parse_bool(opt, val):
                dedupsearch = distdedupsearch
        elif opt == "-insecure":
            verify_ssl = not parse_bool(opt, val)
        elif opt == "-j":
            try:
                maxjobs = int(val)
            except ValueError:
                print_error("Invalid value '%s' for -j option. Integer between 1 and 16 required." % val)
            if maxjobs < 1 or maxjobs > 16:
                print_error("Maximum number of parallel jobs (-j) must be >=1 and <=16")
        else:
            print_error("Invalid option '%s'" % opt)


def prompt_missing_inputs():
    global app, host, user, password, et, lt

    if app is None:
        app = input("Please enter the app that contains the search(es): ").strip()
    if host is None:
        host = input("Please enter your Splunk management host (e.g. https://tenant.splunkcloud.com:8089): ").strip()

    if len(ssname_list) == 0:
        while True:
            n = input(
                "Please enter the name of saved search #%d (empty value to stop entering): "
                % (len(ssname_list) + 1)
            ).strip()
            if not n:
                break
            ssname_list.append(n)

    if session_key is None and user is None:
        user = input("Please enter your Splunk username: ").strip()
    if session_key is None and password is None:
        password = getpass.getpass("Please enter your Splunk password: ")

    if et is None:
        et = input("Please enter the earliest time (UTC or relative): ").strip()
    if lt is None:
        lt = input("Please enter the latest time (UTC or relative): ").strip()


def filter_star_saved_searches(payload):
    entries = payload.get("entry", []) if isinstance(payload, dict) else []
    names = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        content = entry.get("content", {}) if isinstance(entry.get("content"), dict) else {}

        disabled = str(content.get("disabled", "1")) == "0"
        scheduled = str(content.get("is_scheduled", "0")) == "1"
        actions = str(content.get("actions", ""))
        has_summary = "summary_index" in actions

        if name and disabled and scheduled and has_summary:
            names.append(name)
    return names


def get_summary_index_name(content):
    candidates = [
        "action.summary_index._name",
        "action.summary_index.index",
        "action.summary_index._index",
    ]
    for c in candidates:
        if c in content and content[c]:
            return content[c]
    return None


def main():
    parse_args()
    prompt_missing_inputs()

    norm_host = normalize_host(host)
    if not norm_host:
        print_error("Missing required -host value for Splunk REST API (Splunk Cloud supported).")

    if app is None:
        print_error("Missing required -app value")
    if et is None or lt is None:
        print_error("Both -et and -lt are required")

    create_lock_file(norm_host, app)

    client = SplunkRestClient(norm_host, verify=verify_ssl)

    sk = session_key
    if sk is not None:
        client.set_session_key(sk)
    else:
        if user is None or password is None:
            print_error("HTTP Basic Auth requires username and password (use -auth <user:pass> or prompts).")
        client.set_basic_auth(user, password)

    if "*" in ssname_list:
        print("\nGetting list of all saved searches for selected app=%s and owner=%s" % (app, owner))
        all_saved: Dict[str, Any] = {}
        try:
            all_saved = client.list_saved_searches(app, owner)
        except SplunkRestError as exc:
            print_error("Failed to list saved searches: %s" % str(exc))

        entries = all_saved.get("entry", []) if isinstance(all_saved, dict) else []
        print(" ... found %s saved searches" % len(entries))
        added_names = filter_star_saved_searches(all_saved)
        ssname_list.extend(added_names)
        print(
            " ... of those, %s will be added to list (enabled, scheduled, and has summary_index action)"
            % len(added_names)
        )

    st_list = []
    seen_names = {}

    for ssname in ssname_list:
        if ssname == "*":
            continue

        if ssname in seen_names:
            print("\n!!! Warning: saved search specified multiple times: '%s'" % ssname)
            continue
        seen_names[ssname] = 1

        print("\n*** For saved search '%s' ***" % ssname)
        cur_st_list = []
        index = indexarg

        try:
            ss_payload = client.get_saved_search(app, owner, ssname)
            content = extract_content(ss_payload)
            if index is None:
                index = get_summary_index_name(content)

            st_payload = client.get_scheduled_times(app, owner, ssname, et, lt)
            scheduled_times = get_scheduled_times_from_payload(st_payload)
            for st in scheduled_times:
                cur_st_list.append((ssname, st))
        except SplunkRestError as exc:
            print("Failed to get list of scheduled times for saved search '%s' (app='%s', error='%s')" % (ssname, app, str(exc)))
            continue

        if len(cur_st_list) < 1:
            print("No scheduled times for your time range.")
            continue

        if index is None:
            index = "summary"

        if dedup:
            cdsearch = dedupsearch.replace("$namefield$", namefield)
            cdsearch = cdsearch.replace("$timefield$", timefield)
            cdsearch = cdsearch.replace("$index$", index)
            cdsearch = cdsearch.replace("$name$", ssname)
            cdsearch = cdsearch.replace("$et$", et or "")
            cdsearch = cdsearch.replace("$lt$", lt or "")

            print("Executing search to find existing data: '%s'" % cdsearch)
            sid: Optional[str] = None
            try:
                sid = client.dispatch_search(app, owner, cdsearch)
            except SplunkRestError as exc:
                print_error("Failed to dispatch dedup search: %s" % str(exc))

            if sid is None:
                print_error("Failed to dispatch dedup search")

            sys.stdout.write("  waiting for job sid = '%s' " % sid)
            sys.stdout.flush()
            while True:
                time.sleep(sleep)
                is_done, done_progress = client.get_job_state(app, owner, sid)
                if showprogress:
                    sys.stdout.write(" ... %.1f%%" % (done_progress * 100.0))
                    sys.stdout.flush()
                if is_done:
                    break
            print(" ... finished")

            rows: List[Dict[str, Any]] = []
            try:
                rows = client.get_job_results(app, owner, sid)
            except SplunkRestError as exc:
                print_error("Failed to fetch dedup search results: %s" % str(exc))

            existmap = {}
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if timefield in row:
                    try:
                        existmap[str(math.trunc(float(str(row[timefield]))))] = 1
                    except (TypeError, ValueError):
                        continue

            new_list = []
            for pair in cur_st_list:
                st = pair[1]
                try:
                    stkey = str(math.trunc(float(st)))
                except (TypeError, ValueError):
                    stkey = str(st)
                if stkey not in existmap:
                    new_list.append(pair)

            if len(new_list) < len(cur_st_list):
                print(
                    "Out of %d scheduled times, %d will be skipped because they already exist."
                    % (len(cur_st_list), (len(cur_st_list) - len(new_list)))
                )
                cur_st_list = new_list
            else:
                print("All scheduled times will be executed.")

        st_list.extend(cur_st_list)

    if reverseorder:
        st_list.reverse()

    trigger_str = "1" if trigger else "0"

    if len(st_list) == 0:
        print_error("\nNo searches to run", code=0)

    print("\n*** Spawning a total of %d searches (max %d concurrent) ***" % (len(st_list), maxjobs))

    if maxjobs == 1:
        for ssname, st in st_list:
            enforce_schedule_window(sched_start_time, sched_end_time)

            print("\nExecuting %s for UTC = %s (%s)" % (ssname, st, time.ctime(int(float(st)))))
            sid = client.dispatch_saved_search(app, owner, ssname, st, trigger_str)

            print("  waiting for job sid = '%s' " % sid)
            sys.stdout.write(" ")
            while True:
                time.sleep(sleep)
                is_done, done_progress = client.get_job_state(app, owner, sid)
                if showprogress:
                    sys.stdout.write(" ... %.1f%%" % (done_progress * 100.0))
                    sys.stdout.flush()
                if is_done:
                    break

            if not trigger:
                print(" ... Finished (not triggering actions)")
            else:
                print(" ... Finished")
    else:
        cur_jobs = []
        for ssname, st in st_list:
            enforce_schedule_window(sched_start_time, sched_end_time)

            while len(cur_jobs) >= maxjobs:
                time.sleep(sleep)
                cur_jobs = update_job_list(client, app, owner, cur_jobs, show_progress=bool(showprogress), trigger_actions=bool(trigger))

            sid = client.dispatch_saved_search(app, owner, ssname, st, trigger_str)
            cur_jobs.append(sid)
            print("Started job '%s' for saved search '%s', UTC = %s (%s)" % (sid, ssname, st, time.ctime(int(float(st)))))

        while len(cur_jobs) > 0:
            time.sleep(sleep)
            cur_jobs = update_job_list(client, app, owner, cur_jobs, show_progress=bool(showprogress), trigger_actions=bool(trigger))


if __name__ == "__main__":
    main()
