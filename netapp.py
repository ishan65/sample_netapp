import json
import base64
import argparse
from getpass import getpass
import requests
import urllib3 as ur
import re
import time
from requests.auth import HTTPBasicAuth
ur.disable_warnings()

NETAPP_VSERVER_NAME_PREFIX = "ifs_prod"

def get_vservers(cluster: str, auth: str):
    """ Get vServer"""
    url = "https://{}/api/svm/svms".format(cluster)
    response = requests.get(url, auth=auth, verify=True)
    return response.json()

def get_cls(cluster: str, auth: str):
    """ Get vServer"""
    url = "https://{}/api/cluster".format(cluster)
    print(url)
    response = requests.get(url, auth=auth, verify=True)
    return response.json()

def get_snapmirror_lag_time(cluster: str, auth: str):
    url = "https://{}/api/snapmirror/relationships?fields=**".format(cluster)
    response = requests.get(url, auth=auth, verify=True)
    print(response.json())
    if not response.ok:
        return False
    snapmirrors_info = response.json()["records"]
    for snapmirror_info in snapmirrors_info:
        src_vserver = snapmirror_info["source"]["svm"]["name"]
        print(f"Source: {src_vserver}")
        dst_vserver = snapmirror_info["destination"]["svm"]["name"]
        print(f"Destination: {dst_vserver}")
        lag_time = snapmirror_info["lag_time"]
        print(f"LAG TIME| {lag_time}")
    return response.ok

def get_qtree_using_path(cluster_mgt, auth, volume_name, quota_target):
    pattern = re.compile(r'/[vV][oO][lL]')
    path = re.sub(pattern, '', quota_target)
    url = f"https://{cluster_mgt}/api/storage/qtrees?volume.name={volume_name}&path={path}"
    response = requests.get(url, auth=auth, verify=True)
    if not response.ok:
        print(f"API error in get_qtree_using_path : {response} \n {response.json()}")
        return False
    qtree_info = response.json()
    if len(qtree_info["records"]) == 0:
        print(f"get_qtree_using_path: No qtree path found for the quota target {quota_target}")
        return False
    return qtree_info["records"][0]["name"]

def modify_quota_size(cluster_mgt, auth, vserver_name, volume_name, quota_target,
                      disk_limit, threshold_ratio, storage_project_id):
    disk_limit_in_kb = int(disk_limit)*(1024**4)
    qtree_name = get_qtree_using_path(cluster_mgt, auth, volume_name, quota_target)
    quota_rule_uuid = get_quota_rule_uuid(cluster_mgt, auth, vserver_name, volume_name, qtree_name)
    url = f"https://{cluster_mgt}/api/storage/quota/rules/{quota_rule_uuid}"
    threshold_ratio = threshold_ratio  # No restAPI equivalent
    payload = {
        "space": {
            "hard_limit": disk_limit_in_kb,
            "soft_limit": disk_limit_in_kb
        }
    }
    response = requests.patch(url, json=payload, auth=auth, verify=True)
    if not response.ok:
        print(f"API error in modify_quota_size : {response} \n {response.json()}")
        return False
    job = response.json()
    return job_checker(cluster_mgt, auth, job)

def get_cifs_share(cluster_mgt, auth, share_name=None):
    if share_name:
       url = f"https://{cluster_mgt}/api/protocols/cifs/shares?name={share_name}"
    else:
       url = f"https://{cluster_mgt}/api/protocols/cifs/shares"
    response = requests.get(url, auth=auth, verify=True)
    if not response.ok:
        print(f"API error in get_cifs_share : {response} \n {response.json()}")
        return False
    share_info = response.json()
    share_details = {}
    for share in share_info["records"]:
        share_details[share["name"]]={}
        share_details[share["name"]]["name"] = share["name"]
        share_details[share["name"]]["svm"] = share["svm"]["name"]
        share_details[share["name"]]["svm_uuid"] = share["svm"]["uuid"]
    return share_details

def get_cifs_share_acl(cluster_mgt, auth, share_name=None):
    share_details = get_cifs_share(cluster_mgt, auth, share_name)
    for share in share_details.values():
        url = f"https://{cluster_mgt}/api/protocols/cifs/shares/{share['svm_uuid']}/{share['name']}/acls?fields=**"
        response = requests.get(url, auth=auth, verify=True)
        if not response.ok:
            print(f"API error in get_cifs_share_acl : {response} \n {response.json()}")
            return False
        share_acl_info = response.json()
        index = 0
        share_details[share["name"]]["acl"] = {}
        for share_acl in share_acl_info["records"]:
            share_details[share["name"]]["acl"][index] = {}
            share_details[share["name"]]["acl"][index]["user_group"] = share_acl["user_or_group"]
            share_details[share["name"]]["acl"][index]["type"] = share_acl["type"]
            share_details[share["name"]]["acl"][index]["permission"] = share_acl["permission"]
            index = index + 1
    return share_details

def job_checker(cluster_mgt, auth, job):
    job_uuid = job["job"]["uuid"]
    wait_for_job(cluster_mgt, auth,job_uuid)
    state = get_job_state(cluster_mgt, auth, job_uuid)
    return state == "success"

def wait_for_job(cluster_mgt, auth, job_uuid, timeout_duration=300, waiting_duration=10):
    if job_uuid is not None:
        job_state = get_job_state(cluster_mgt, auth, job_uuid)
        if job_state != 'success':
            timeout = time.time() + timeout_duration
            while (not timeout_passed(timeout) and job_state != 'success' and job_state == "running"):
                time.sleep(waiting_duration)
                job_state = get_job_state(cluster_mgt, auth, job_uuid)
            return job_state
        else:
            return job_state

def get_job_state(cluster_mgt, auth, job_uuid):
    url = f"https://{cluster_mgt}/api/cluster/jobs/{job_uuid}"
    response = requests.get(url, auth=auth, verify=True)
    if not response.ok:
        print(f"API error in get_job_state : {response} \n {response.json()}")
        return False
    job_info = response.json()
    return job_info["state"]

def get_quota_rule_uuid(cluster_mgt, auth, vserver_name, volume_name, qtree_name):
    url = f"https://{cluster_mgt}/api/storage/quota/rules?svm.name={vserver_name}&volume.name={volume_name}&qtree.name={qtree_name}"
    response = requests.get(url, auth=auth, verify=True)
    if not response.ok:
        print(f"API error in get_quota_rule_uuid : {response} \n {response.json()}")
        return False
    quota_info = response.json()
    if len(quota_info["records"]) == 0:
        print(f"get_quota_rule_uuid: No UUID record is found for the qtree {qtree_name}")
        return False
    return quota_info["records"][0]["uuid"]

def get_quota_size(cluster, auth, tree, volume, vserver):
    # creating the url
    url = "https://{}/api/storage/quota/reports".format(cluster)
    print(url)
    svm_url = None if vserver is None else f"svm.name={vserver}"
    vol_url = None if volume is None else f"volume.name={volume}"
    qt_url = None if tree is None else f"qtree.name={tree}"
    filter = ""
    for val in [svm_url, vol_url, qt_url]:
        if val:
            filter = filter + val + "&"
    url = url + "?" + filter
    url = url[:-1]
    response = requests.get(url, auth=auth, verify=True)
    if not response.ok:
        return False
    quota_report = response.json()["records"][0]
    index = quota_report["index"]
    volume_uuid = quota_report["volume"]["uuid"]
    quote_report_url = f"https://{cluster}/api/storage/quota/reports/{volume_uuid}/{index}"
    quote_report_response = requests.get(quote_report_url, auth=auth, verify=True)
    if not quote_report_response.ok:
        return False
    data = quote_report_response.json()
    quota = {}
    quota['total_space_gb'] = int(data["space"]['hard_limit'])/1024/1024/1024
    quota['used_space_gb'] = int(data['space']['used']['total'])/1024/1024/1024
    quota['free_space_gb'] = quota['total_space_gb']-quota['used_space_gb']
    return quota

def get_quota_report(cluster, auth, vserver_name=None, volume_name=None, qtree_name=None):
    url = f'https://{cluster}/api/storage/quota/reports?type="tree"'
    svm_url = None if vserver_name is None else f"svm.name={vserver_name}"
    vol_url = None if volume_name is None else f"volume.name={volume_name}"
    qt_url = None if qtree_name is None else f"qtree.name={qtree_name}"
    if vserver_name is None and qtree_name is None and qtree_name is None:
        url = url
    else:
        filter = ""
        for val in [svm_url, vol_url, qt_url]:
            if val:
                filter = filter + val + "&"
        url = url + "&" + filter
        url = url[:-1]

    print(url)

    response = requests.get(url, auth=auth, verify=True)
    print(response.json())
    if not response.ok:
        return False
    for quota_report in response.json()["records"]:
        index = quota_report["index"]
        volume_uuid = quota_report["volume"]["uuid"]
        print(volume_uuid)
        print(index)
        quote_report_url = f"https://{cluster}/api/storage/quota/reports/{volume_uuid}/{index}"
        quote_report_response = requests.get(quote_report_url, auth=auth, verify=True)
        if not quote_report_response.ok:
            continue
        quote_report_info = quote_report_response.json()
        disk_limit = quote_report_info["space"]["hard_limit"] \
            if "hard_limit" in quote_report_info["space"].keys() else 0
        disk_used = quote_report_info["space"]["used"]["total"] \
            if "space" in quote_report_info.keys() else 0
        disk_used_pct_disk_limit = quote_report_info["space"]["used"]["hard_limit_percent"] \
            if "hard_limit_percent" in quote_report_info["space"]["used"].keys() else 0
        disk_used_pct_threshold = quote_report_info["space"]["used"]["soft_limit_percent"] \
            if "soft_limit_percent" in quote_report_info["space"]["used"].keys() else 0
        files_used = quote_report_info["files"]["used"]["total"] \
            if "files" in quote_report_info.keys() else 0
        threshold = quote_report_info["space"]["soft_limit"] \
            if "soft_limit" in quote_report_info["space"].keys() else 0
        vserver = quote_report_info["svm"]["name"]
        volume = quote_report_info["volume"]["name"]
        tree = quote_report_info["qtree"]["name"]
        print(f"INFO HEAD : {vserver}, {volume}, {tree} ")
        try:
            quota_size_total = float(disk_limit)/(1024**2)
            quota_size_used = float(disk_used)/(1024**2)
            quota_threshold = float(threshold)/(1024**2)
            quota_disk_used_pct_disk_limit = float(disk_used_pct_disk_limit)
            quota_disk_used_pct_threshold = float(disk_used_pct_threshold)
            quota_number_files = float(files_used)
        except ValueError:
            continue

        print(f"INFO: {quota_size_total} , {quota_size_used} , {quota_threshold}, {quota_disk_used_pct_disk_limit}, {quota_disk_used_pct_threshold}, {quota_number_files}")
    return response.ok

def get_dns_uuid(cluster, auth, vserver):
    # creating the url
    url = f"https://{cluster}/api/name-services/dns?svm.name={vserver}"
    response = requests.get(url, auth=auth, verify=True)
    if not response.ok:
        return False
    dns_report = response.json()["records"][0]
    dns_uuid = dns_report["uuid"]
    return dns_uuid

def get_qtrees(cluster, auth, vserver_name=None, volume_name=None, qtree_name=None):
    url = f"https://{cluster}/api/storage/qtrees"
    svm_url = None if vserver_name is None else f"svm.name={vserver_name}"
    vol_url = None if volume_name is None else f"volume.name={volume_name}"
    qt_url = None if qtree_name is None else f"name={qtree_name}"
    # creating the url
    if vserver_name is None and qtree_name is None and qtree_name is None:
        url = url
    else:
        filter = ""
        for val in [svm_url, vol_url, qt_url]:
            if val:
                filter = filter + val + "&"
        url = url + "?" + filter
        url = url[:-1]

    # fetch DATA of all qtree
    response = requests.get(url, auth=auth, verify=True)
    if not response.ok:
        print(f"API error in get_qtrees : {response}")
        return False
    qtrees = response.json()["records"]
    qtree_pattern = re.compile(r'(%s)\d+_vol_\d+' % NETAPP_VSERVER_NAME_PREFIX)
    for qtree_obj in qtrees:
        # fetch URL specific to each qtree
        volume_uuid = qtree_obj["volume"]["uuid"]
        qtree_id = qtree_obj["id"]
        qtree_href = f"https://{cluster}/api/storage/qtrees/{volume_uuid}/{qtree_id}"
        print(qtree_href)
        qtree_response = requests.get(qtree_href, auth=auth, verify=True)
        if not qtree_response.ok:
            print(f"API error in get_qtrees for qtree specific {qtree_href} : {qtree_response}")
            return False
        return qtree_response.json()


def get_volumes(cluster, auth):
    # creating the url
    url = f"https://{cluster}/api/storage/volumes"
    response = requests.get(url, auth=auth, verify=True)
    if not response.ok:
        return False
    volume_report = response.json()["records"][0]
    return volume_report



def timeout_passed(timeout):
    return True if time.time() > timeout else False

def parse_args() -> argparse.Namespace:
    """Parse the command line arguments from the user"""
    parser = argparse.ArgumentParser(
        description="This script will list SVMs")
    parser.add_argument(
        "-c", "--cluster", required=True, help="API server IP:port details")
    parser.add_argument(
        "-u",
        "--api_user",
        default="admin",
        help="API Username")
    parser.add_argument("-p", "--api_pass", help="API Password")
    parsed_args = parser.parse_args()

    # collect the password without echo if not already provided
    if not parsed_args.api_pass:
        parsed_args.api_pass = getpass()
    return parsed_args

def pretty_json_output(input):
    return json.dumps(input, indent=6, sort_keys=False)

def redirect_to_file(input, filename):
    with open(filename, "w") as f:
        for line in input:
            f.write(line)

if __name__ == "__main__":
    ARGS = parse_args()
    basic = HTTPBasicAuth(ARGS.api_user, ARGS.api_pass)
    print(get_vservers(ARGS.cluster, basic))
    # print(get_cls(ARGS.cluster, basic))
    # print(get_snapmirror_lag_time(ARGS.cluster, basic))
    # print(get_quota_rule_uuid(ARGS.cluster, basic, "ifs_tim_svm_47", "ifs_tim_svm_47_vol", "ifs_tim_svm_47_vol_51"))
    # print(get_qtree_using_path(ARGS.cluster, basic, "ifs_tim_svm_47_vol", '/vol/ifs_tim_svm_47_vol/ifs_tim_svm_47_vol_51'))
    # print(get_quota_size(ARGS.cluster, basic, "ifs_prod_216_vol_246", "ifs_prod_216_vol", "ifs_prod_216"))
    # print(get_quota_report(ARGS.cluster, basic, None, None, None))
    # print(get_quota_report(ARGS.cluster, basic, "ifs_prod_216", "ifs_prod_216_vol", None))
    # modify_quota_size(ARGS.cluster, basic, "ifs_tim_svm_47", "ifs_tim_svm_47_vol", '/vol/ifs_tim_svm_47_vol/ifs_tim_svm_47_vol_51', 2, 0.95, 1)
    # get_cifs_share(ARGS.cluster, basic)
    # print(get_cifs_share_acl(ARGS.cluster, basic))
    # print(pretty_json_output(get_cifs_share_acl(ARGS.cluster, basic)))
    # redirect_to_file(pretty_json_output(get_cifs_share_acl(ARGS.cluster, basic)),"C:\\Users\\im530\\Videos\\TeamInfo\\sharepermission_sim.txt")
    # print(get_dns_uuid(ARGS.cluster, basic, "ifs_prod_1058"))
    # print(get_qtrees(ARGS.cluster, basic, "ifs_prod_1058", "ifs_prod_1058_vol", "ifs_prod_1058_vol_1682"))
    # print(get_volumes(ARGS.cluster, basic))