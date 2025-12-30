#!/usr/bin/env -S uv run --quiet
# /// script
# requires-python = ">=3.9"
# dependencies = []
# ///
"""
Data Guard setup script for Oracle on GCP.

This script configures Oracle Data Guard between two instances:
- Primary database (active)
- Standby database (passive replica)

Prerequisites:
- Both instances must be set up with setup_oracle.py
- Primary database must be running
- Standby instance must have Oracle Grid Infrastructure running

The script will:
1. Configure the primary database for Data Guard
2. Remove the existing database on the standby
3. Create a standby database via RMAN active duplication
"""

import argparse
import logging
import subprocess
import sys

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


def gcloud_ssh(instance: str, zone: str, project: str, command: str) -> subprocess.CompletedProcess:
    """SSH into a GCP instance and run a command."""
    return run_cmd([
        "gcloud", "compute", "ssh", instance,
        "--zone", zone,
        "--project", project,
        "--command", command,
    ], check=True)


def gcloud_ssh_capture(instance: str, zone: str, project: str, command: str) -> str:
    """SSH into a GCP instance and capture output."""
    result = run_cmd([
        "gcloud", "compute", "ssh", instance,
        "--zone", zone,
        "--project", project,
        "--command", command,
    ], capture=True)
    return result.stdout.strip()


def gcloud_scp(src: str, dst: str, zone: str, project: str) -> None:
    """Copy files between local and GCP instances."""
    run_cmd([
        "gcloud", "compute", "scp",
        "--zone", zone,
        "--project", project,
        src, dst,
    ])


def get_instance_ip(instance: str, zone: str, project: str) -> str:
    """Get the internal IP address of a GCP instance."""
    result = run_cmd([
        "gcloud", "compute", "instances", "describe", instance,
        "--zone", zone,
        "--project", project,
        "--format", "get(networkInterfaces[0].networkIP)",
    ], capture=True)
    return result.stdout.strip()


def configure_primary_for_dataguard(
    instance: str,
    zone: str,
    project: str,
    db_name: str,
    db_unique_name: str,
    standby_db_unique_name: str,
    primary_ip: str,
    standby_ip: str,
) -> None:
    """Configure the primary database for Data Guard."""
    logger.info("Configuring primary database %s for Data Guard", db_name)

    commands = f'''
sudo su - oracle <<'EOF'
sqlplus -s / as sysdba <<'SQLEND'
-- Enable force logging
ALTER DATABASE FORCE LOGGING;

-- Set Data Guard related parameters
ALTER SYSTEM SET DB_UNIQUE_NAME='{db_unique_name}' SCOPE=SPFILE;
ALTER SYSTEM SET LOG_ARCHIVE_CONFIG='DG_CONFIG=({db_unique_name},{standby_db_unique_name})' SCOPE=BOTH;
ALTER SYSTEM SET LOG_ARCHIVE_DEST_1='LOCATION=USE_DB_RECOVERY_FILE_DEST VALID_FOR=(ALL_LOGFILES,ALL_ROLES) DB_UNIQUE_NAME={db_unique_name}' SCOPE=BOTH;
ALTER SYSTEM SET LOG_ARCHIVE_DEST_2='SERVICE={standby_db_unique_name} ASYNC VALID_FOR=(ONLINE_LOGFILES,PRIMARY_ROLE) DB_UNIQUE_NAME={standby_db_unique_name} REOPEN=60 MAX_FAILURE=0' SCOPE=BOTH;
ALTER SYSTEM SET LOG_ARCHIVE_DEST_STATE_2=DEFER SCOPE=BOTH;
ALTER SYSTEM SET STANDBY_FILE_MANAGEMENT=AUTO SCOPE=BOTH;
ALTER SYSTEM SET FAL_SERVER='{standby_db_unique_name}' SCOPE=BOTH;

-- Create standby redo logs (same size as online redo logs + 1 extra group)
-- First, get the redo log size
SET SERVEROUTPUT ON
DECLARE
    v_log_size NUMBER;
    v_groups NUMBER;
BEGIN
    SELECT BYTES/1024/1024, COUNT(*) INTO v_log_size, v_groups FROM V$LOG GROUP BY BYTES;

    -- Create standby redo log groups (one more than online redo log groups)
    FOR i IN 1..v_groups+1 LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER DATABASE ADD STANDBY LOGFILE SIZE ' || v_log_size || 'M';
            DBMS_OUTPUT.PUT_LINE('Created standby redo log group ' || i);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Group ' || i || ' may already exist: ' || SQLERRM);
        END;
    END LOOP;
END;
/

-- Show configuration
SELECT FORCE_LOGGING, LOG_MODE FROM V$DATABASE;
SELECT GROUP#, THREAD#, BYTES/1024/1024 AS SIZE_MB, STATUS FROM V$STANDBY_LOG;
SQLEND
EOF
'''
    gcloud_ssh(instance, zone, project, commands)


def configure_bash_profiles(
    instance: str,
    zone: str,
    project: str,
):
    """
    Configure .bash_profile for default user to set ORACLE_HOME.
    Also set an environment variable with the ridiculous name ORACLE_ORACLE_HOME to allow the grid user to have
    the ORACLE_HOME of the oracle user.
    The purpose is to be able to use these values in later commands and not have to resort to hard-coded paths.
    """
    logger.info("Configuring .bash_profile on %s", instance)

    commands = '''
        ORACLE_HOME=$(sudo su - oracle -c 'echo $ORACLE_HOME')
        cat >> ~/.bash_profile << BASHEND
export ORACLE_HOME=$ORACLE_HOME
BASHEND

        sudo tee -a /home/grid/.bash_profile << BASHEND
export ORACLE_ORACLE_HOME=$ORACLE_HOME
BASHEND
'''
    gcloud_ssh(instance, zone, project, commands)


def configure_tns_entries(
    instance: str,
    zone: str,
    project: str,
    db_name: str,
    primary_db_unique_name: str,
    standby_db_unique_name: str,
    primary_ip: str,
    standby_ip: str,
) -> None:
    """Configure TNS entries on an instance for Data Guard.

    We update both ORACLE_HOME and GRID_HOME `tnsnames.ora` files and set proper
    file ownership.
    """
    logger.info("Configuring TNS entries on %s", instance)

    # Create TNS entries for the primary and standby unique names
    tns_entries = f'''
{primary_db_unique_name} =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = {primary_ip})(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = {db_name})
    )
  )

{standby_db_unique_name} =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = {standby_ip})(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = {db_name})
    )
  )
'''

    commands = f'''
# Add TNS entries to both grid and oracle tnsnames.ora
for user in oracle grid; do
    sudo su - $user <<'EOF'
        TNS_FILE="$ORACLE_HOME/network/admin/tnsnames.ora"

        # Remove existing entries if they exist
        sed -i '/{primary_db_unique_name}/,/^$/d' "$TNS_FILE" 2>/dev/null || true
        sed -i '/{standby_db_unique_name}/,/^$/d' "$TNS_FILE" 2>/dev/null || true

        # Add new entries
        cat << 'TNSEND' >> "$TNS_FILE"
{tns_entries}
TNSEND

        echo "Updated $TNS_FILE"
EOF
done
'''
    gcloud_ssh(instance, zone, project, commands)


def configure_static_listener(
    instance: str,
    zone: str,
    project: str,
    db_name: str,
    db_unique_name: str,
) -> None:
    """Configure static listener entry for Data Guard connections."""
    logger.info("Configuring static listener on %s", instance)

    commands = f'''
sudo su - grid <<'EOF'
LISTENER_ORA="$ORACLE_HOME/network/admin/listener.ora"

# Check if SID_LIST already exists
if ! grep -q "SID_LIST_LISTENER" "$LISTENER_ORA"; then
    cat >> "$LISTENER_ORA" << LISTEND

SID_LIST_LISTENER =
  (SID_LIST =
    (SID_DESC =
      (GLOBAL_DBNAME = {db_name})
      (ORACLE_HOME = $ORACLE_ORACLE_HOME)
      (SID_NAME = {db_name})
    )
  )
LISTEND
    echo "Added SID_LIST to listener.ora"
fi

# Reload listener
lsnrctl reload
lsnrctl status
EOF
'''
    gcloud_ssh(instance, zone, project, commands)


def setup_password_file(
    primary_instance: str,
    standby_instance: str,
    zone: str,
    project: str,
    db_name: str,
    sys_password: str,
) -> None:
    """Create password file on primary and copy to standby.

    This is done as the LAST step before Data Guard configuration to ensure
    the password file is not regenerated by any subsequent operations.
    """
    logger.info("Setting up password file for Data Guard")

    # Create password file on primary, update SYS password, and copy to /tmp for scp
    commands = f'''
sudo su - oracle <<'EOF'
# Create password file with known password
orapwd file=$ORACLE_HOME/dbs/orapw{db_name} password='{sys_password}' entries=10 force=y

# Update SYS password to match the password file
sqlplus -s / as sysdba <<'SQLEND'
ALTER USER SYS IDENTIFIED BY "{sys_password}";
SQLEND

ls -la $ORACLE_HOME/dbs/orapw{db_name}
EOF

source .bash_profile
# Copy password file to /tmp with read permissions for scp
sudo cp $ORACLE_HOME/dbs/orapw{db_name} /tmp/orapw{db_name}
sudo chmod 644 /tmp/orapw{db_name}

# Get md5sum for verification
md5sum /tmp/orapw{db_name} | awk '{{print $1}}'
'''
    gcloud_ssh(primary_instance, zone, project, commands)

    # Download password file from primary's /tmp
    logger.info("Copying password file from %s to %s", primary_instance, standby_instance)
    gcloud_scp(
        f"{primary_instance}:/tmp/orapw{db_name}",
        "/tmp/orapw_dataguard",
        zone, project
    )

    # Upload password file to standby
    gcloud_scp(
        "/tmp/orapw_dataguard",
        f"{standby_instance}:/tmp/orapw{db_name}",
        zone, project
    )

    # Move to correct location with correct permissions
    commands = f'''
source .bash_profile
sudo mv /tmp/orapw{db_name} $ORACLE_HOME/dbs/orapw{db_name}
sudo chown oracle:oinstall $ORACLE_HOME/dbs/orapw{db_name}
sudo chmod 640 $ORACLE_HOME/dbs/orapw{db_name}
ls -la $ORACLE_HOME/dbs/orapw{db_name}

# Get md5sum for verification
sudo md5sum $ORACLE_HOME/dbs/orapw{db_name} | awk '{{print $1}}'
'''
    gcloud_ssh(standby_instance, zone, project, commands)

    # Clean up /tmp on primary
    gcloud_ssh(primary_instance, zone, project, f"sudo rm -f /tmp/orapw{db_name}")

    # Restart primary database to ensure password file is loaded
    logger.info("Restarting primary database to load password file")
    restart_commands = f'''
sudo su - oracle <<'EOF'
srvctl stop database -d {db_name}
srvctl start database -d {db_name}
srvctl status database -d {db_name}
EOF
'''
    gcloud_ssh(primary_instance, zone, project, restart_commands)


def sync_password_file_after_duplication(
    primary_instance: str,
    standby_instance: str,
    zone: str,
    project: str,
    db_name: str,
) -> None:
    """Synchronize password file from primary to standby after RMAN duplication.

    RMAN duplication creates a new password file on the standby that doesn't
    match the primary. This function re-copies the password file to ensure
    they are identical, which is required for Data Guard redo transport and
    DG Broker to work correctly.
    """
    logger.info("Syncing password file after RMAN duplication")

    orapw_file = f"orapw{db_name}"
    oracle_home = gcloud_ssh_capture(
        primary_instance, zone, project,
        "sudo su - oracle -c 'echo $ORACLE_HOME'"
    )
    orapw_path = f"{oracle_home}/dbs/{orapw_file}"

    # Get md5sum of primary password file
    primary_md5 = gcloud_ssh_capture(
        primary_instance, zone, project,
        f"sudo md5sum {orapw_path} | awk '{{print $1}}'"
    )
    logger.info("Primary password file md5: %s", primary_md5)

    # Get md5sum of standby password file
    standby_md5 = gcloud_ssh_capture(
        standby_instance, zone, project,
        f"sudo md5sum {orapw_path} | awk '{{print $1}}'"
    )
    logger.info("Standby password file md5: %s", standby_md5)

    if primary_md5 == standby_md5:
        logger.info("Password files already match - no sync needed")
        return

    logger.info("Password files differ - syncing from primary to standby...")

    # Copy password file to /tmp on primary with read permissions
    gcloud_ssh(primary_instance, zone, project,
        f"sudo cp {orapw_path} /tmp/{orapw_file} && sudo chmod 644 /tmp/{orapw_file}")

    # Download from primary
    gcloud_scp(f"{primary_instance}:/tmp/{orapw_file}", f"/tmp/{orapw_file}", zone, project)

    # Upload to standby
    gcloud_scp(f"/tmp/{orapw_file}", f"{standby_instance}:/tmp/{orapw_file}", zone, project)

    # Move to correct location with proper permissions on standby
    gcloud_ssh(standby_instance, zone, project, f"""
sudo mv /tmp/{orapw_file} {orapw_path}
sudo chown oracle:oinstall {orapw_path}
sudo chmod 640 {orapw_path}
""")

    # Clean up /tmp on primary
    gcloud_ssh(primary_instance, zone, project, f"sudo rm -f /tmp/{orapw_file}")

    # Verify sync
    new_standby_md5 = gcloud_ssh_capture(
        standby_instance, zone, project,
        f"sudo md5sum {orapw_path} | awk '{{print $1}}'"
    )

    if primary_md5 == new_standby_md5:
        logger.info("Password file sync successful - md5: %s", new_standby_md5)
    else:
        logger.error("Password file sync FAILED - md5 mismatch!")
        logger.error("  Primary: %s", primary_md5)
        logger.error("  Standby: %s", new_standby_md5)
        raise RuntimeError("Password file synchronization failed")

    # Restart standby database to load the new password file
    # After RMAN duplication, the standby is running - we need to restart it
    logger.info("Restarting standby database to load new password file")
    gcloud_ssh(standby_instance, zone, project, f"""
sudo su - oracle <<'EOF'
export ORACLE_SID={db_name}
sqlplus -s / as sysdba <<'SQLEND'
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
SQLEND
EOF
""")
    logger.info("Standby database restarted")


def remove_standby_database(
    instance: str,
    zone: str,
    project: str,
    db_name: str,
    diskgroup: str,
) -> None:
    """Remove the existing database on the standby instance."""
    logger.info("Removing existing database %s on %s", db_name, instance)

    commands = f'''
sudo su - oracle <<'EOF'
# Stop the database if running
srvctl stop database -d {db_name} -f 2>/dev/null || true

# Remove from cluster registry
srvctl remove database -d {db_name} -f 2>/dev/null || true
EOF

sudo su - grid <<'EOF'
asmcmd rm -rf +{diskgroup}/{db_name} 2>/dev/null || true
asmcmd ls +{diskgroup}
EOF

# Clean up any leftover files
source .bash_profile
sudo rm -f $ORACLE_HOME/dbs/*{db_name}* 2>/dev/null || true
'''
    gcloud_ssh(instance, zone, project, commands)


def create_standby_pfile(
    instance: str,
    zone: str,
    project: str,
    db_name: str,
    db_unique_name: str,
    primary_db_unique_name: str,
    diskgroup: str,
) -> None:
    """Create a minimal pfile to start the standby instance for duplication."""
    logger.info("Creating standby pfile on %s", instance)

    commands = f'''
sudo su - oracle <<'EOF'
cat > $ORACLE_HOME/dbs/init{db_name}.ora << 'PFILEEND'
db_name='{db_name}'
db_unique_name='{db_unique_name}'
db_block_size=8192
sga_target=1G
pga_aggregate_target=256M
processes=300
audit_file_dest='/u01/app/oracle/admin/{db_name}/adump'
audit_trail='db'
compatible='19.0.0'
control_files='+{diskgroup}/{db_name}/controlfile/control01.ctl'
db_recovery_file_dest='+{diskgroup}'
db_recovery_file_dest_size=10G
diagnostic_dest='/u01/app/oracle'
dispatchers='(PROTOCOL=TCP) (SERVICE={db_name}XDB)'
enable_pluggable_database=true
log_archive_config='DG_CONFIG=({primary_db_unique_name},{db_unique_name})'
log_archive_dest_1='LOCATION=USE_DB_RECOVERY_FILE_DEST VALID_FOR=(ALL_LOGFILES,ALL_ROLES) DB_UNIQUE_NAME={db_unique_name}'
log_archive_dest_2='SERVICE={primary_db_unique_name} ASYNC VALID_FOR=(ONLINE_LOGFILES,PRIMARY_ROLE) DB_UNIQUE_NAME={primary_db_unique_name}'
fal_server='{primary_db_unique_name}'
remote_login_passwordfile='exclusive'
standby_file_management='AUTO'
PFILEEND

# Create audit directory
mkdir -p /u01/app/oracle/admin/{db_name}/adump

echo "Pfile created:"
cat $ORACLE_HOME/dbs/init{db_name}.ora
EOF
'''
    gcloud_ssh(instance, zone, project, commands)


def start_standby_nomount(
    instance: str,
    zone: str,
    project: str,
    db_name: str,
) -> None:
    """Start the standby instance in NOMOUNT mode for duplication.

    Also updates the oracle user's .bash_profile to set ORACLE_SID to the
    new database name, since the standby will use the primary's DB_NAME.
    """
    logger.info("Starting standby instance %s in NOMOUNT mode", db_name)

    commands = Rf'''
sudo su - oracle <<'EOF'

# Update oracle user's .bash_profile to use the new database name
# This ensures ORACLE_SID is correct for all subsequent operations
sed -i 's/^export ORACLE_SID=.*/export ORACLE_SID={db_name}/' /home/oracle/.bash_profile
echo "Updated ORACLE_SID in .bash_profile to {db_name}"
grep ORACLE_SID /home/oracle/.bash_profile

export ORACLE_SID={db_name}
sqlplus -s / as sysdba <<SQLEND
STARTUP NOMOUNT PFILE='$ORACLE_HOME/dbs/init{db_name}.ora';
SELECT STATUS FROM V\$INSTANCE;
SQLEND
EOF
'''
    gcloud_ssh(instance, zone, project, commands)


def duplicate_database(
    primary_instance: str,
    standby_instance: str,
    zone: str,
    project: str,
    db_name: str,
    primary_db_unique_name: str,
    standby_db_unique_name: str,
    primary_diskgroup: str,
    standby_diskgroup: str,
    primary_ip: str,
    standby_ip: str,
    sys_password: str,
) -> None:
    """Use RMAN to create the standby database via active duplication."""
    logger.info("Creating standby database via RMAN active duplication (this may take 10-15 minutes)...")

    # Use EZConnect strings to avoid TNS resolution issues in RMAN background processes
    primary_ezconnect = f"//{primary_ip}:1521/{db_name}"
    standby_ezconnect = f"//{standby_ip}:1521/{db_name}"

    commands = f'''
sudo su - oracle <<'EOF'
# Use EZConnect strings for both TARGET and AUXILIARY
rman TARGET sys/"{sys_password}"@{primary_ezconnect} AUXILIARY sys/"{sys_password}"@{standby_ezconnect} <<'RMANEND'
DUPLICATE TARGET DATABASE
  FOR STANDBY
  FROM ACTIVE DATABASE
  DORECOVER
  SPFILE
    SET db_unique_name='{standby_db_unique_name}'
    SET control_files='+{standby_diskgroup}'
    SET db_file_name_convert='+{primary_diskgroup}','+{standby_diskgroup}'
    SET log_file_name_convert='+{primary_diskgroup}','+{standby_diskgroup}'
    SET log_archive_dest_1='LOCATION=USE_DB_RECOVERY_FILE_DEST VALID_FOR=(ALL_LOGFILES,ALL_ROLES) DB_UNIQUE_NAME={standby_db_unique_name}'
    SET log_archive_dest_2='SERVICE={primary_ezconnect} ASYNC VALID_FOR=(ONLINE_LOGFILES,PRIMARY_ROLE) DB_UNIQUE_NAME={primary_db_unique_name}'
    SET fal_server='{primary_ezconnect}'
    SET db_recovery_file_dest='+{standby_diskgroup}'
  NOFILENAMECHECK;
RMANEND
EOF
'''
    gcloud_ssh(standby_instance, zone, project, commands)


def start_managed_recovery(
    instance: str,
    zone: str,
    project: str,
    db_name: str,
) -> None:
    """Start managed recovery process on the standby database."""
    logger.info("Starting managed recovery on standby")

    commands = f'''
sudo su - oracle <<'EOF'
export ORACLE_SID={db_name}
sqlplus -s / as sysdba <<'SQLEND'
-- Start managed recovery
ALTER DATABASE RECOVER MANAGED STANDBY DATABASE DISCONNECT FROM SESSION;

-- Verify standby status
SELECT DATABASE_ROLE, OPEN_MODE, PROTECTION_MODE FROM V$DATABASE;
SQLEND
EOF
'''
    gcloud_ssh(instance, zone, project, commands)


def fix_standby_redo_logs(
    instance: str,
    zone: str,
    project: str,
    db_name: str,
    diskgroup: str,
) -> None:
    """Fix redo log files that point to wrong disk group after duplication.

    After RMAN duplication, some redo log groups may have invalid paths like
    '+DGORA1' or '+DGORA2' instead of full ASM paths. This function identifies
    all log groups with invalid paths and recreates them.
    """
    logger.info("Fixing standby redo log files")

    commands = f'''
sudo su - oracle <<'EOF'
export ORACLE_SID={db_name}
sqlplus -s / as sysdba <<'SQLEND'
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200

-- Disable standby file management temporarily
ALTER SYSTEM SET STANDBY_FILE_MANAGEMENT=MANUAL SCOPE=BOTH;

-- Show current log files before fix
PROMPT === Current log files before fix ===
SELECT l.GROUP#, l.STATUS, l.BYTES/1024/1024 AS SIZE_MB, f.MEMBER
FROM V$LOG l, V$LOGFILE f
WHERE l.GROUP# = f.GROUP#
ORDER BY l.GROUP#;

-- Identify and fix online redo log groups with invalid paths
-- Invalid paths are those that don't contain a full ASM path (e.g., just '+DGORA1')
DECLARE
    v_log_size NUMBER;
    v_invalid_count NUMBER := 0;
BEGIN
    -- Get log size from existing logs
    SELECT MAX(BYTES)/1024/1024 INTO v_log_size FROM V$LOG;
    IF v_log_size IS NULL OR v_log_size = 0 THEN
        v_log_size := 100;
    END IF;

    DBMS_OUTPUT.PUT_LINE('Using log size: ' || v_log_size || 'M');

    -- Find and fix logs with invalid paths (paths without '/' after the diskgroup)
    FOR rec IN (
        SELECT DISTINCT l.GROUP#, l.STATUS, f.MEMBER
        FROM V$LOG l, V$LOGFILE f
        WHERE l.GROUP# = f.GROUP#
        AND (
            f.MEMBER NOT LIKE '+%/%' OR     -- No path separator after diskgroup
            f.MEMBER LIKE '+{diskgroup}' OR  -- Just diskgroup name
            LENGTH(f.MEMBER) < 10            -- Suspiciously short path
        )
        AND l.STATUS != 'CURRENT'
        ORDER BY l.GROUP#
    ) LOOP
        BEGIN
            DBMS_OUTPUT.PUT_LINE('Fixing invalid online log group ' || rec.GROUP# || ': ' || rec.MEMBER);
            EXECUTE IMMEDIATE 'ALTER DATABASE DROP LOGFILE GROUP ' || rec.GROUP#;
            EXECUTE IMMEDIATE 'ALTER DATABASE ADD LOGFILE GROUP ' || rec.GROUP# || ' (''+{diskgroup}'') SIZE ' || v_log_size || 'M';
            v_invalid_count := v_invalid_count + 1;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Could not recreate group ' || rec.GROUP# || ': ' || SQLERRM);
        END;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Fixed ' || v_invalid_count || ' invalid online redo log groups');
END;
/

-- Handle current log if it has invalid path (must use CLEAR instead of DROP)
DECLARE
    v_log_size NUMBER;
    v_current_group NUMBER;
    v_member VARCHAR2(256);
BEGIN
    SELECT MAX(BYTES)/1024/1024 INTO v_log_size FROM V$LOG;
    IF v_log_size IS NULL OR v_log_size = 0 THEN
        v_log_size := 100;
    END IF;

    -- Check if current log has invalid path
    BEGIN
        SELECT l.GROUP#, f.MEMBER INTO v_current_group, v_member
        FROM V$LOG l, V$LOGFILE f
        WHERE l.GROUP# = f.GROUP#
        AND l.STATUS = 'CURRENT'
        AND (
            f.MEMBER NOT LIKE '+%/%' OR
            LENGTH(f.MEMBER) < 10
        )
        AND ROWNUM = 1;

        -- Current log has invalid path - clear it first
        DBMS_OUTPUT.PUT_LINE('Current log group ' || v_current_group || ' has invalid path: ' || v_member);
        EXECUTE IMMEDIATE 'ALTER DATABASE CLEAR LOGFILE GROUP ' || v_current_group;
        EXECUTE IMMEDIATE 'ALTER DATABASE DROP LOGFILE GROUP ' || v_current_group;
        EXECUTE IMMEDIATE 'ALTER DATABASE ADD LOGFILE GROUP ' || v_current_group || ' (''+{diskgroup}'') SIZE ' || v_log_size || 'M';
        DBMS_OUTPUT.PUT_LINE('Fixed current log group ' || v_current_group);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('Current log group has valid path - no fix needed');
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error handling current log: ' || SQLERRM);
    END;
END;
/

-- Drop existing standby redo logs and recreate with proper count
-- Primary has (n+1) standby redo logs where n is the number of online redo log groups
DECLARE
    v_online_count NUMBER;
    v_max_group NUMBER;
    v_log_size NUMBER;
BEGIN
    -- Get count of online redo log groups and max group number
    SELECT COUNT(*), MAX(GROUP#), MAX(BYTES)/1024/1024
    INTO v_online_count, v_max_group, v_log_size
    FROM V$LOG;

    IF v_log_size IS NULL OR v_log_size = 0 THEN
        v_log_size := 100;
    END IF;

    DBMS_OUTPUT.PUT_LINE('Online redo log groups: ' || v_online_count);
    DBMS_OUTPUT.PUT_LINE('Standby redo logs needed: ' || (v_online_count + 1));

    -- Drop all existing standby redo logs
    FOR rec IN (SELECT GROUP# FROM V$STANDBY_LOG ORDER BY GROUP#) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER DATABASE DROP STANDBY LOGFILE GROUP ' || rec.GROUP#;
            DBMS_OUTPUT.PUT_LINE('Dropped standby log group ' || rec.GROUP#);
        EXCEPTION WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Could not drop standby log group ' || rec.GROUP# || ': ' || SQLERRM);
        END;
    END LOOP;

    -- Create (n+1) standby redo log groups starting after max online group
    FOR i IN 1..(v_online_count + 1) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER DATABASE ADD STANDBY LOGFILE GROUP ' ||
                (v_max_group + i) || ' (''+{diskgroup}'') SIZE ' || v_log_size || 'M';
            DBMS_OUTPUT.PUT_LINE('Created standby log group ' || (v_max_group + i));
        EXCEPTION WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Could not create standby log group ' || (v_max_group + i) || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/

-- Re-enable standby file management
ALTER SYSTEM SET STANDBY_FILE_MANAGEMENT=AUTO SCOPE=BOTH;

-- Verify log files after fix
PROMPT === Log files after fix ===
SELECT GROUP#, TYPE, MEMBER FROM V$LOGFILE ORDER BY TYPE, GROUP#;
SELECT GROUP#, THREAD#, BYTES/1024/1024 AS SIZE_MB, STATUS FROM V$STANDBY_LOG ORDER BY GROUP#;
SQLEND
EOF
'''
    gcloud_ssh(instance, zone, project, commands)


def enable_log_shipping(
    instance: str,
    zone: str,
    project: str,
    standby_ip: str,
    db_name: str,
    standby_db_unique_name: str,
) -> None:
    """Enable log shipping from primary to standby."""
    logger.info("Enabling log shipping on primary")

    standby_ezconnect = f"//{standby_ip}:1521/{db_name}"

    commands = f'''
sudo su - oracle <<'EOF'
sqlplus -s / as sysdba <<'SQLEND'
-- Configure log archive dest 2 using EZConnect with automatic retry
-- REOPEN=60: Retry failed connections every 60 seconds
-- MAX_FAILURE=0: Unlimited retries (0 means never give up)
ALTER SYSTEM SET LOG_ARCHIVE_DEST_2='SERVICE={standby_ezconnect} ASYNC VALID_FOR=(ONLINE_LOGFILES,PRIMARY_ROLE) DB_UNIQUE_NAME={standby_db_unique_name} REOPEN=60 MAX_FAILURE=0' SCOPE=BOTH;
ALTER SYSTEM SET LOG_ARCHIVE_DEST_STATE_2=ENABLE SCOPE=BOTH;
ALTER SYSTEM SET LOG_ARCHIVE_CONFIG='DG_CONFIG=(ORA1,{standby_db_unique_name})' SCOPE=BOTH;
ALTER SYSTEM SWITCH LOGFILE;

-- Verify log shipping status
SELECT DEST_ID, STATUS, DESTINATION FROM V$ARCHIVE_DEST WHERE DEST_ID <= 2;
SQLEND
EOF
'''
    gcloud_ssh(instance, zone, project, commands)


def register_standby_with_cluster(
    instance: str,
    zone: str,
    project: str,
    db_name: str,
    db_unique_name: str,
    diskgroup: str,
) -> None:
    """Register the standby database with Oracle Restart."""
    logger.info("Registering standby database with Oracle Restart")

    commands = f'''
sudo su - oracle <<'EOF'
# Add database to Oracle Restart
srvctl add database -d {db_name} -o $ORACLE_HOME -r PHYSICAL_STANDBY -s MOUNT -diskgroup {diskgroup}

# Start the database via srvctl
srvctl start database -d {db_name} -o mount

srvctl status database -d {db_name}
srvctl config database -d {db_name}
EOF
'''
    gcloud_ssh(instance, zone, project, commands)


def validate_dataguard(
    primary_instance: str,
    standby_instance: str,
    zone: str,
    project: str,
    db_name: str,
    primary_db_unique_name: str,
    standby_db_unique_name: str,
) -> None:
    """Validate Data Guard configuration."""
    logger.info("=== Validating Data Guard Configuration ===")

    # Check primary
    logger.info("Checking primary database status")
    commands = f'''
sudo su - oracle <<'EOF'
echo "=== Primary Database Status ==="
sqlplus -s / as sysdba <<'SQLEND'
SET LINESIZE 200
SELECT DATABASE_ROLE, OPEN_MODE, PROTECTION_MODE, SWITCHOVER_STATUS FROM V$DATABASE;
SELECT DEST_ID, STATUS, ERROR FROM V$ARCHIVE_DEST WHERE DEST_ID = 2;
SQLEND
EOF
'''
    gcloud_ssh(primary_instance, zone, project, commands)

    # Check standby
    logger.info("Checking standby database status")
    commands = f'''
sudo su - oracle <<'EOF'
echo "=== Standby Database Status ==="
sqlplus -s / as sysdba <<'SQLEND'
SET LINESIZE 200
SELECT DATABASE_ROLE, OPEN_MODE, PROTECTION_MODE FROM V$DATABASE;
SELECT PROCESS, STATUS, THREAD#, SEQUENCE# FROM V$MANAGED_STANDBY WHERE PROCESS LIKE 'MRP%' OR PROCESS LIKE 'RFS%';
SQLEND
EOF
'''
    gcloud_ssh(standby_instance, zone, project, commands)

    logger.info("=== Data Guard Validation Complete ===")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Set up Oracle Data Guard between two instances"
    )
    parser.add_argument(
        "--primary",
        default="ora-1",
        help="Primary instance name (default: ora-1)"
    )
    parser.add_argument(
        "--standby",
        default="ora-2",
        help="Standby instance name (default: ora-2)"
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
        "--skip-primary-config",
        action="store_true",
        help="Skip primary database configuration"
    )
    parser.add_argument(
        "--skip-duplication",
        action="store_true",
        help="Skip RMAN duplication (standby already created)"
    )
    parser.add_argument(
        "--sys-password",
        default="DataGu4rd#2025",
        help="SYS password for Data Guard connections (default: DataGu4rd#2025)"
    )

    args = parser.parse_args()

    # Get defaults from gcloud config if not provided
    project = args.project or get_gcloud_config("project")
    zone = args.zone or get_gcloud_config("compute/zone")

    if not project:
        logger.error("No project specified and none configured in gcloud")
        sys.exit(1)
    if not zone:
        logger.error("No zone specified and none configured in gcloud")
        sys.exit(1)

    # Derive database names from instance names
    # Primary: ora-1 -> ORA1, Standby: ora-2 -> use primary's DB_NAME
    primary_num = args.primary.split("-")[1]
    standby_num = args.standby.split("-")[1]

    db_name = f"ORA{primary_num}"  # Both use primary's DB_NAME
    primary_db_unique_name = f"ORA{primary_num}"
    standby_db_unique_name = f"ORA{primary_num}_STBY"

    # Disk groups
    primary_diskgroup = f"DGORA{primary_num}"
    standby_diskgroup = f"DGORA{standby_num}"

    # Get IP addresses
    primary_ip = get_instance_ip(args.primary, zone, project)
    standby_ip = get_instance_ip(args.standby, zone, project)

    logger.info("Setting up Data Guard")
    logger.info("  Primary: %s (%s) - DB: %s, DG: %s", args.primary, primary_ip, primary_db_unique_name, primary_diskgroup)
    logger.info("  Standby: %s (%s) - DB: %s, DG: %s", args.standby, standby_ip, standby_db_unique_name, standby_diskgroup)

    if not args.skip_primary_config:
        for inst in [args.primary, args.standby]:
            configure_bash_profiles(inst, zone, project)

        # Step 1: Configure TNS entries on both instances
        logger.info("=== Configuring TNS Entries ===")
        for inst in [args.primary, args.standby]:
            configure_tns_entries(
                inst, zone, project, db_name,
                primary_db_unique_name, standby_db_unique_name,
                primary_ip, standby_ip
            )

        # Step 2: Configure static listener entries
        logger.info("=== Configuring Static Listeners ===")
        configure_static_listener(args.primary, zone, project, db_name, primary_db_unique_name)
        configure_static_listener(args.standby, zone, project, db_name, standby_db_unique_name)

        # Step 3: Configure primary database for Data Guard
        logger.info("=== Configuring Primary Database ===")
        configure_primary_for_dataguard(
            args.primary, zone, project, db_name,
            primary_db_unique_name, standby_db_unique_name,
            primary_ip, standby_ip
        )

        # Step 4: Setup password file on primary and copy to standby
        logger.info("=== Setting Up Password File ===")
        setup_password_file(args.primary, args.standby, zone, project, db_name, args.sys_password)

    if not args.skip_duplication:
        # Step 5: Remove existing database on standby
        logger.info("=== Removing Existing Standby Database ===")
        standby_old_db = f"ORA{standby_num}"
        remove_standby_database(args.standby, zone, project, standby_old_db, standby_diskgroup)

        # Step 6: Create standby pfile
        logger.info("=== Creating Standby Pfile ===")
        create_standby_pfile(
            args.standby, zone, project, db_name,
            standby_db_unique_name, primary_db_unique_name,
            standby_diskgroup
        )

        # Step 7: Start standby in NOMOUNT mode
        logger.info("=== Starting Standby in NOMOUNT Mode ===")
        start_standby_nomount(args.standby, zone, project, db_name)

        # Step 8: Duplicate database using RMAN
        logger.info("=== Duplicating Database via RMAN ===")
        duplicate_database(
            args.primary, args.standby, zone, project,
            db_name, primary_db_unique_name, standby_db_unique_name,
            primary_diskgroup, standby_diskgroup,
            primary_ip, standby_ip, args.sys_password
        )

        # Step 8b: Sync password file after duplication
        # RMAN creates a new password file on standby that doesn't match primary
        logger.info("=== Syncing Password File After Duplication ===")
        sync_password_file_after_duplication(
            args.primary, args.standby, zone, project, db_name
        )

        # Step 9: Fix standby redo log files
        logger.info("=== Fixing Standby Redo Log Files ===")
        fix_standby_redo_logs(args.standby, zone, project, db_name, standby_diskgroup)

        # Step 10: Register standby with Oracle Restart
        logger.info("=== Registering Standby with Oracle Restart ===")
        register_standby_with_cluster(
            args.standby, zone, project, db_name,
            standby_db_unique_name, standby_diskgroup
        )

    # Step 11: Start managed recovery
    logger.info("=== Starting Managed Recovery ===")
    start_managed_recovery(args.standby, zone, project, db_name)

    # Step 12: Enable log shipping
    logger.info("=== Enabling Log Shipping ===")
    enable_log_shipping(args.primary, zone, project, standby_ip, db_name, standby_db_unique_name)

    # Step 13: Validate configuration
    validate_dataguard(
        args.primary, args.standby, zone, project,
        db_name, primary_db_unique_name, standby_db_unique_name,
    )

    logger.info("=== Data Guard Setup Complete ===")
    logger.info("Primary: %s (%s)", args.primary, primary_db_unique_name)
    logger.info("Standby: %s (%s)", args.standby, standby_db_unique_name)


if __name__ == "__main__":
    main()
