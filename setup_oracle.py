#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.9"
# dependencies = []
# ///
"""
Oracle setup script for GCP with OL8, CDB, and ASMFD.

This script automates the setup of an Oracle database instance on GCP,
including kernel configuration, Oracle installation, and ASMFD migration.

Instance naming convention: ora-<number> (e.g., ora-1, ora-2)
CDB disk group naming: DGORA<number> (e.g., DGORA1, DGORA2)
"""

import argparse
import json
import logging
import re
import subprocess
import sys
import time

# Configure logging with timestamps
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_cmd(cmd: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    """Run a command and optionally capture output."""
    logger.debug("+ %s", " ".join(cmd))
    return subprocess.run(cmd, check=check, capture_output=capture, text=True)


def get_gcloud_config(key: str) -> str:
    """Get a gcloud configuration value."""
    result = run_cmd(["gcloud", "config", "get-value", key], capture=True)
    return result.stdout.strip()


def gcloud_ssh(instance: str, zone: str, project: str, command: str) -> None:
    """SSH into a GCP instance and run a command."""
    run_cmd([
        "gcloud", "compute", "ssh", instance,
        "--zone", zone,
        "--project", project,
        "--command", command,
    ])


def wait_for_instance(instance: str, zone: str, project: str, timeout: int = 300) -> None:
    """Wait for an instance to be ready for SSH."""
    logger.info("Waiting for %s to be ready...", instance)
    start = time.time()
    while time.time() - start < timeout:
        result = run_cmd(
            ["gcloud", "compute", "ssh", instance,
             "--zone", zone, "--project", project,
             "--command", "true"],
            check=False, capture=True
        )
        if result.returncode == 0:
            logger.info("%s is ready", instance)
            return
        time.sleep(10)
    raise TimeoutError(f"Instance {instance} not ready after {timeout} seconds")


def parse_instance_number(instance: str) -> str:
    """Extract the number from an instance name like 'ora-1' -> '1'."""
    match = re.match(r"ora-(\d+)", instance)
    if not match:
        raise ValueError(f"Instance name '{instance}' must match pattern 'ora-<number>'")
    return match.group(1)


def get_cdb_diskgroup_name(instance: str) -> str:
    """Get the CDB disk group name based on instance (ora-1 -> DGORA1)."""
    num = parse_instance_number(instance)
    return f"DGORA{num}"


def get_db_name(instance: str) -> str:
    """Get the database name based on instance (ora-1 -> ORA1)."""
    num = parse_instance_number(instance)
    return f"ORA{num}"


def delete_instance(instance: str, zone: str, project: str) -> None:
    """Delete a GCP instance and its disks."""
    run_cmd([
        "gcloud", "compute", "instances", "delete", instance,
        "--zone", zone,
        "--project", project,
        "--quiet",
    ])


def create_instance(
    instance: str,
    zone: str,
    project: str,
    cdb_diskgroup: str,
    extra_diskgroups: list[str],
) -> None:
    """Create the GCP instance with required disks."""
    cdb_disk_name = cdb_diskgroup.lower()

    cmd = [
        "gcloud", "compute", "instances", "create", instance,
        "--zone", zone,
        "--project", project,
        "--machine-type=n2-standard-4",
        "--image-project=oracle-linux-cloud",
        "--image-family=oracle-linux-8",
        f"--create-disk=name={instance}-u01,device-name=u01,size=30GB",
        f"--create-disk=name={instance}-swap,device-name=swap,size=16GB",
        f"--create-disk=name={instance}-{cdb_disk_name},device-name={cdb_disk_name},size=20GB,type=pd-balanced",
    ]

    for dg in extra_diskgroups:
        disk_name = dg.lower()
        cmd.append(f"--create-disk=name={instance}-{disk_name},device-name={disk_name},size=20GB,type=pd-balanced")

    run_cmd(cmd)


def configure_kernel(instance: str, zone: str, project: str) -> None:
    """Configure the kernel to a version that supports ASMFD."""
    commands = """
sudo yum-config-manager --disable ol8_UEKR7
sudo yum-config-manager --enable ol8_UEKR6
sudo yum -y install kernel-uek-5.4.17-2136.326.6.el8uek
sudo grubby --set-default /boot/vmlinuz-5.4.17-*
"""
    gcloud_ssh(instance, zone, project, commands)

    # Reboot the instance
    logger.info("Rebooting %s...", instance)
    run_cmd([
        "gcloud", "compute", "instances", "reset", instance,
        "--zone", zone,
        "--project", project,
    ])


def setup_prerequisites(instance: str, zone: str, project: str) -> None:
    """Set up SSH keys, install packages, and clone oracle-toolkit."""
    commands = """
ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 .ssh/authorized_keys
ssh -o StrictHostKeyChecking=no $HOSTNAME true

wget https://rpmfind.net/linux/epel/8/Everything/x86_64/Packages/r/rlwrap-0.46.2-3.el8.x86_64.rpm
sudo dnf -y install rlwrap-*.rpm

sudo tee /etc/yum.repos.d/google-cloud-sdk.repo <<EOF >/dev/null
[google-cloud-cli]
name=Google Cloud CLI
baseurl=https://packages.cloud.google.com/yum/repos/cloud-sdk-el9-x86_64
enabled=1
gpgcheck=1
repo_gpgcheck=0
gpgkey=https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
EOF

sudo dnf -y install google-cloud-sdk git bind-utils
python3 -m pip install --user ansible jmespath
git clone https://github.com/google/oracle-toolkit
cd oracle-toolkit/
git checkout 6642001f07169aab29ddf2b298109707d63e394f
"""
    gcloud_ssh(instance, zone, project, commands)


def create_config_files(
    instance: str,
    zone: str,
    project: str,
    cdb_diskgroup: str,
    extra_diskgroups: list[str],
) -> None:
    """Create the software mount and ASM configuration files."""
    cdb_disk_name = cdb_diskgroup.lower()

    # Build ASM config
    asm_config = [
        {
            "diskgroup": cdb_diskgroup,
            "disks": [
                {
                    "blk_device": f"/dev/disk/by-id/google-{cdb_disk_name}",
                    "name": f"{cdb_diskgroup}V"
                }
            ]
        }
    ]

    for dg in extra_diskgroups:
        disk_name = dg.lower()
        asm_config.append({
            "diskgroup": dg,
            "disks": [
                {
                    "blk_device": f"/dev/disk/by-id/google-{disk_name}",
                    "name": f"{dg}V"
                }
            ]
        })

    asm_config_json = json.dumps(asm_config, indent=4)

    commands = f'''
cd oracle-toolkit

cat << 'EOF' > software_mount_config.json
[
    {{
        "purpose": "software",
        "blk_device": "/dev/disk/by-id/google-u01",
        "name": "u01",
        "fstype":"xfs",
        "mount_point":"/u01",
        "mount_opts":"nofail"
    }}
]
EOF

cat << 'EOF' > asm_config.json
{asm_config_json}
EOF
'''
    gcloud_ssh(instance, zone, project, commands)


def install_oracle(
    instance: str,
    zone: str,
    project: str,
    swlib_bucket: str,
    cdb_diskgroup: str,
    db_name: str,
) -> None:
    """Run the Oracle installation script."""
    commands = f'''
cd oracle-toolkit
./install-oracle.sh \\
  --ora-swlib-type gcs \\
  --ora-swlib-bucket {swlib_bucket} \\
  --backup-dest "+{cdb_diskgroup}" \\
  --ora-version 21 \\
  --no-patch \\
  --ora-data-mounts software_mount_config.json \\
  --ora-asm-disks asm_config.json \\
  --swap-blk-device "/dev/disk/by-id/google-swap" \\
  --ora-data-diskgroup {cdb_diskgroup} \\
  --ora-reco-diskgroup {cdb_diskgroup} \\
  --allow-install-on-vm \\
  --ora-db-container true \\
  --ora-db-name {db_name} \\
  --ora-pdb-name-prefix PDB0 \\
  --instance-hostname {instance} \\
  --instance-ip-addr $(dig +short +search {instance})
'''
    gcloud_ssh(instance, zone, project, commands)


def configure_oracle_sga(instance: str, zone: str, project: str) -> None:
    """Configure Oracle SGA settings."""
    commands = '''
sudo su - oracle -c "sqlplus -s / as sysdba << EOF
ALTER SYSTEM SET SGA_TARGET=0 SCOPE=SPFILE;
STARTUP FORCE
EOF"
'''
    gcloud_ssh(instance, zone, project, commands)


def configure_asmfd(
    instance: str,
    zone: str,
    project: str,
    cdb_diskgroup: str,
    extra_diskgroups: list[str],
) -> None:
    """Configure ASMFD and migrate from UDEV-based ASM."""
    # Build the afd_label commands for all disk groups
    # Disk devices start at /dev/sdd1 (sda=boot, sdb=u01, sdc=swap, sdd+=ASM disks)
    all_diskgroups = [cdb_diskgroup] + extra_diskgroups
    label_commands = []
    for i, dg in enumerate(all_diskgroups):
        # sdd=100, sde=101, etc.
        device_letter = chr(ord('d') + i)
        label_commands.append(f"asmcmd afd_label {dg} /dev/sd{device_letter}1 --migrate")

    label_commands_str = "\n".join(label_commands)

    commands = f'''
sudo su - <<'EOF'
export ORACLE_HOME=/u01/app/21.3.0/grid
export PATH=$ORACLE_HOME/bin:$PATH
export ORACLE_BASE=/u01/app

crsctl stop has
asmcmd afd_configure -e
asmcmd afd_state
{label_commands_str}
asmcmd afd_lsdsk

# Remove UDEV rules and reload
rm -f /etc/udev/rules.d/99-oracle-asmdevices.rules
udevadm control --reload-rules
udevadm trigger

crsctl start has
EOF
'''
    gcloud_ssh(instance, zone, project, commands)


def configure_asm_diskstring(
    instance: str,
    zone: str,
    project: str,
    cdb_diskgroup: str,
    extra_diskgroups: list[str],
) -> None:
    """Configure ASM disk string and verify ASMFD setup."""
    all_diskgroups = [cdb_diskgroup] + extra_diskgroups
    mount_commands = "\n".join(f"asmcmd mount {dg}" for dg in all_diskgroups)
    db_name = get_db_name(instance)

    commands = f"""
sudo su - grid <<'GRIDEOF'
# Fail fast on errors; print useful context.
set -euo pipefail

export ORACLE_HOME=/u01/app/21.3.0/grid
export ORACLE_BASE=/u01/app
export PATH="$ORACLE_HOME/bin:$PATH"

# Wait for ASM to be ready (up to 10 minutes)
ASM_WAIT_TIMEOUT_SECS=600
ASM_WAIT_INTERVAL_SECS=10
printf 'Waiting for ASM to start (timeout: %ss, interval: %ss)...\n' "$ASM_WAIT_TIMEOUT_SECS" "$ASM_WAIT_INTERVAL_SECS"
start_ts="$(date +%s)"
while true; do
    status_out="$(srvctl status asm 2>&1 || true)"
    if echo "$status_out" | grep -q "is running"; then
        echo "ASM is running: $status_out"
        break
    fi

    now_ts="$(date +%s)"
    elapsed="$((now_ts - start_ts))"
    if [ "$elapsed" -ge "$ASM_WAIT_TIMEOUT_SECS" ]; then
        echo "ERROR: ASM did not reach RUNNING state within $ASM_WAIT_TIMEOUT_SECS seconds"
        echo "Last 'srvctl status asm' output:"
        echo "$status_out"
        echo
        echo "crsctl check has:"
        crsctl check has || true
        echo
        echo "crsctl stat res -t:"
        crsctl stat res -t || true
        exit 1
    fi

    echo "Waiting for ASM... elapsed $elapsed seconds; last status: $status_out"
    sleep "$ASM_WAIT_INTERVAL_SECS"
done

# Set the AFD discovery string in both runtime (dsset) and spfile
# This must happen BEFORE mounting disk groups
echo "Setting ASM discovery string to AFD:*..."
asmcmd dsset 'AFD:*'

sqlplus -S / as sysasm <<'SQLEND'
ALTER SYSTEM SET ASM_DISKSTRING='AFD:*' SCOPE=BOTH;
SQLEND

# Verify the discovery string was set
asmcmd dsget
sleep 2

# Mount disk groups
echo "Mounting disk groups..."
{mount_commands}

# Verify and show ASMFD configuration
sqlplus -S / as sysasm <<'END'

show parameter asm_diskstring;
select name, state from v$asm_diskgroup;
select path from v$asm_disk;
set feed off
set pages 100
set lines 200

col group_number heading 'Grp' format 9
col name heading 'Name' format a18
col state heading 'State' format a7
col header_status heading 'Header' format a7
col mount_status heading 'Mount' format a7
col path heading 'Path' format a16
col library heading 'Library' format a44

Prompt ASMFD Configuration

select group_number, name, state, header_status, mount_status, path, library
from v$asm_disk order by group_number, name;
END
GRIDEOF

# Start the database after ASMFD configuration
sudo su - oracle <<'ORAEOF'
set -euo pipefail
echo "Starting database..."
srvctl start database -d {db_name} 2>/dev/null || echo "Database may already be running"
srvctl status database -d {db_name}
ORAEOF
"""
    gcloud_ssh(instance, zone, project, commands)


def validate_setup(
    instance: str,
    zone: str,
    project: str,
    cdb_diskgroup: str,
    extra_diskgroups: list[str],
    db_name: str,
) -> None:
    """Validate that disk groups are mounted and database is open."""
    all_diskgroups = [cdb_diskgroup] + extra_diskgroups
    dg_checks = " && ".join(
        f"asmcmd lsdg {dg} | grep -q MOUNTED"
        for dg in all_diskgroups
    )

    commands = f'''
echo "=== Validating ASMFD Setup ==="

# Check disk groups are mounted
sudo su - grid -c "{dg_checks}" && echo "OK: All disk groups are mounted" || {{ echo "FAIL: Disk groups not mounted"; exit 1; }}

# Check database is open
sudo su - oracle -c "srvctl status database -d {db_name}" | grep -q "is running" && echo "OK: Database {db_name} is running" || {{ echo "FAIL: Database not running"; exit 1; }}

# Check database is open (not just mounted)
sudo su - oracle -c "sqlplus -s / as sysdba <<EOF
SET HEADING OFF FEEDBACK OFF
SELECT 'DB_STATUS:' || STATUS FROM V\\\\\\$INSTANCE;
EXIT;
EOF" | grep -q "DB_STATUS:OPEN" && echo "OK: Database is OPEN" || {{ echo "FAIL: Database not OPEN"; exit 1; }}

echo "=== Validation Complete ==="
'''
    gcloud_ssh(instance, zone, project, commands)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Set up Oracle on GCP with OL8, CDB, and ASMFD"
    )
    parser.add_argument(
        "--instance",
        default="ora-1",
        help="Name of the GCP instance (format: ora-<number>, default: ora-1)"
    )
    parser.add_argument(
        "--project",
        default=None,
        help="GCP project (default: current gcloud config)"
    )
    parser.add_argument(
        "--zone",
        default=None,
        help="GCP zone (default: current gcloud config)"
    )
    parser.add_argument(
        "--swlib-bucket",
        default="gs://ocie-oracle-us-east4",
        help="GCS bucket containing Oracle software (default: gs://ocie-oracle-us-east4)"
    )
    parser.add_argument(
        "--extra-diskgroups",
        default="DG1",
        help="Comma-separated list of extra disk group names (default: DG1)"
    )
    parser.add_argument(
        "--skip-create",
        action="store_true",
        help="Skip instance creation (use existing instance)"
    )
    parser.add_argument(
        "--skip-kernel",
        action="store_true",
        help="Skip kernel configuration and reboot"
    )
    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="Skip Oracle installation"
    )
    parser.add_argument(
        "--skip-asmfd",
        action="store_true",
        help="Skip ASMFD configuration"
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete the instance and its disks"
    )

    args = parser.parse_args()

    # Validate and parse instance name
    try:
        cdb_diskgroup = get_cdb_diskgroup_name(args.instance)
        db_name = get_db_name(args.instance)
    except ValueError as e:
        logger.error("%s", e)
        sys.exit(1)

    # Parse extra disk groups
    extra_diskgroups = [dg.strip() for dg in args.extra_diskgroups.split(",") if dg.strip()]

    # Get defaults from gcloud config if not provided
    project = args.project or get_gcloud_config("project")
    zone = args.zone or get_gcloud_config("compute/zone")

    if not project:
        logger.error("No project specified and none configured in gcloud")
        sys.exit(1)
    if not zone:
        logger.error("No zone specified and none configured in gcloud")
        sys.exit(1)

    # Handle delete mode
    if args.delete:
        logger.info("Deleting instance '%s' in %s/%s", args.instance, project, zone)
        delete_instance(args.instance, zone, project)
        logger.info("Instance '%s' deleted", args.instance)
        return

    logger.info("Setting up Oracle on instance '%s' in %s/%s", args.instance, project, zone)
    logger.info("  CDB disk group: %s", cdb_diskgroup)
    logger.info("  Database name: %s", db_name)
    logger.info("  Extra disk groups: %s", ", ".join(extra_diskgroups))

    # Step 1: Create the instance
    if not args.skip_create:
        logger.info("=== Creating GCP instance ===")
        create_instance(args.instance, zone, project, cdb_diskgroup, extra_diskgroups)
        wait_for_instance(args.instance, zone, project)

    # Step 2: Configure kernel and reboot
    if not args.skip_kernel:
        logger.info("=== Configuring kernel ===")
        configure_kernel(args.instance, zone, project)
        # Wait for instance to come back up after reboot
        time.sleep(30)
        wait_for_instance(args.instance, zone, project)

    # Step 3: Install Oracle
    if not args.skip_install:
        logger.info("=== Setting up prerequisites ===")
        setup_prerequisites(args.instance, zone, project)

        logger.info("=== Creating configuration files ===")
        create_config_files(args.instance, zone, project, cdb_diskgroup, extra_diskgroups)

        logger.info("=== Installing Oracle ===")
        install_oracle(args.instance, zone, project, args.swlib_bucket, cdb_diskgroup, db_name)

        logger.info("=== Configuring Oracle SGA ===")
        configure_oracle_sga(args.instance, zone, project)

    # Step 4: Configure ASMFD
    if not args.skip_asmfd:
        logger.info("=== Configuring ASMFD ===")
        configure_asmfd(args.instance, zone, project, cdb_diskgroup, extra_diskgroups)

        logger.info("=== Configuring ASM disk string ===")
        configure_asm_diskstring(args.instance, zone, project, cdb_diskgroup, extra_diskgroups)

    # Step 5: Validate setup
    logger.info("=== Validating setup ===")
    validate_setup(args.instance, zone, project, cdb_diskgroup, extra_diskgroups, db_name)

    logger.info("=== Oracle setup complete on %s ===", args.instance)


if __name__ == "__main__":
    main()
