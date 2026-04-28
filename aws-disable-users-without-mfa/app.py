#!/usr/bin/env python3

import boto3
from botocore.exceptions import ClientError

# =========================
# CONFIGURATION
# =========================
DRY_RUN = True  # Set False to apply changes
IGNORE_USERS = {
    "admin",
    "root",
    "service-account"
}

# =========================
# AWS CLIENT
# =========================
iam = boto3.client('iam')

# =========================
# FUNCTIONS
# =========================
def has_console_access(username):
    try:
        iam.get_login_profile(UserName=username)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            return False
        raise

def has_mfa_enabled(username):
    paginator = iam.get_paginator('list_mfa_devices')
    for page in paginator.paginate(UserName=username):
        if page['MFADevices']:
            return True
    return False

def disable_console_access(username):
    try:
        iam.delete_login_profile(UserName=username)
        print(f"[OK] Console access disabled for: {username}")
        return True
    except ClientError as e:
        print(f"[ERROR] Failed to disable console access for {username}: {e}")
        return False

# =========================
# MAIN EXECUTION
# =========================
def main():
    total_users = 0
    users_with_console = 0
    users_without_mfa = 0
    actions_taken = 0

    paginator = iam.get_paginator('list_users')

    print("Starting IAM user audit...\n")

    for page in paginator.paginate():
        for user in page['Users']:
            username = user['UserName']
            total_users += 1

            if username in IGNORE_USERS:
                print(f"[SKIPPED] {username} (ignored)")
                continue

            try:
                console = has_console_access(username)
                mfa = has_mfa_enabled(username)

                if console:
                    users_with_console += 1

                if console and not mfa:
                    users_without_mfa += 1

                    print(f"[ACTION] {username} has console access without MFA")

                    if DRY_RUN:
                        print("  -> [DRY-RUN] Would disable console access")
                    else:
                        if disable_console_access(username):
                            actions_taken += 1
                else:
                    print(f"[OK] {username} | console={console} | mfa={mfa}")

            except Exception as e:
                print(f"[ERROR] {username}: {e}")

    # =========================
    # FINAL SUMMARY
    # =========================
    print("\n=========================")
    print("SUMMARY")
    print("=========================")
    print(f"Total users: {total_users}")
    print(f"Users with console access: {users_with_console}")
    print(f"Users with console and no MFA: {users_without_mfa}")
    print(f"Actions executed: {actions_taken}")
    print(f"Dry-run mode: {DRY_RUN}")
    print("=========================")


if __name__ == "__main__":
    main()