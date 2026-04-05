//! Role-Based Access Control (RBAC) for the wire protocol.
//!
//! Maps MongoDB builtin roles to the commands they grant and provides a
//! `check_privilege` function used by the auth gate in `wire_server.rs`.

use std::collections::HashSet;

fn read_commands() -> HashSet<&'static str> {
    HashSet::from([
        "find", "count", "distinct", "aggregate", "getMore", "killCursors",
        "listCollections", "listIndexes", "collStats", "dbStats",
        "serverStatus", "ping", "hello", "ismaster", "isMaster",
        "connectionStatus", "buildInfo", "buildinfo", "hostInfo",
        "whatsmyuri", "explain", "getLog", "getCmdLineOpts",
        "getFreeMonitoringStatus", "connPoolStats", "features",
        "listCommands", "listDatabases", "replSetGetStatus",
        "replSetGetConfig", "shardingState", "lockInfo",
    ])
}

fn read_write_commands() -> HashSet<&'static str> {
    let mut s = read_commands();
    s.extend([
        "insert", "update", "delete", "findAndModify",
        "createIndexes", "dropIndexes", "deleteIndexes",
        "create", "drop", "bulkWrite",
    ]);
    s
}

fn db_admin_commands() -> HashSet<&'static str> {
    let mut s = read_commands();
    s.extend([
        "createIndexes", "dropIndexes", "deleteIndexes",
        "create", "drop", "collMod", "compact", "validate",
        "reIndex", "renameCollection", "dropDatabase",
        "fsync", "getParameter", "setParameter",
        "logRotate", "setFreeMonitoring",
    ]);
    s
}

fn user_admin_commands() -> HashSet<&'static str> {
    HashSet::from([
        "createUser", "dropUser", "updateUser", "usersInfo",
        "grantRolesToUser", "revokeRolesFromUser", "rolesInfo",
    ])
}

fn db_owner_commands() -> HashSet<&'static str> {
    let mut s = read_write_commands();
    s.extend(db_admin_commands());
    s.extend(user_admin_commands());
    s
}

fn commands_for_role(role: &str) -> Option<HashSet<&'static str>> {
    match role {
        "read" => Some(read_commands()),
        "readWrite" => Some(read_write_commands()),
        "dbAdmin" => Some(db_admin_commands()),
        "userAdmin" => Some(user_admin_commands()),
        "dbOwner" => Some(db_owner_commands()),
        "root" => None, // signals "allow everything"
        _ => Some(HashSet::new()),
    }
}

/// Check whether a user's roles grant access to `command` on `target_db`.
///
/// `roles` is a slice of `(role_name, role_db)` pairs.  A role grants access
/// when its `role_db` matches `target_db` **or** the role is `root` scoped to
/// the `admin` database (which is effective globally).
pub fn check_privilege(roles: &[(String, String)], command: &str, target_db: &str) -> bool {
    for (role_name, role_db) in roles {
        let db_matches = role_db == target_db || role_db == "*";

        if role_name == "root" && role_db == "admin" {
            return true;
        }

        if !db_matches {
            continue;
        }

        match commands_for_role(role_name) {
            None => return true, // root (already checked above, but defensive)
            Some(allowed) => {
                if allowed.contains(command) {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn root_on_admin_allows_everything() {
        let roles = vec![("root".into(), "admin".into())];
        assert!(check_privilege(&roles, "insert", "mydb"));
        assert!(check_privilege(&roles, "createUser", "other"));
        assert!(check_privilege(&roles, "drop", "admin"));
    }

    #[test]
    fn read_role_allows_find_not_insert() {
        let roles = vec![("read".into(), "mydb".into())];
        assert!(check_privilege(&roles, "find", "mydb"));
        assert!(check_privilege(&roles, "count", "mydb"));
        assert!(!check_privilege(&roles, "insert", "mydb"));
        assert!(!check_privilege(&roles, "createUser", "mydb"));
    }

    #[test]
    fn read_role_wrong_db() {
        let roles = vec![("read".into(), "mydb".into())];
        assert!(!check_privilege(&roles, "find", "other"));
    }

    #[test]
    fn read_write_allows_crud() {
        let roles = vec![("readWrite".into(), "app".into())];
        assert!(check_privilege(&roles, "find", "app"));
        assert!(check_privilege(&roles, "insert", "app"));
        assert!(check_privilege(&roles, "update", "app"));
        assert!(check_privilege(&roles, "delete", "app"));
        assert!(!check_privilege(&roles, "createUser", "app"));
    }

    #[test]
    fn db_owner_includes_everything() {
        let roles = vec![("dbOwner".into(), "app".into())];
        assert!(check_privilege(&roles, "find", "app"));
        assert!(check_privilege(&roles, "insert", "app"));
        assert!(check_privilege(&roles, "createUser", "app"));
        assert!(check_privilege(&roles, "compact", "app"));
    }

    #[test]
    fn user_admin_allows_user_commands_only() {
        let roles = vec![("userAdmin".into(), "app".into())];
        assert!(check_privilege(&roles, "createUser", "app"));
        assert!(check_privilege(&roles, "dropUser", "app"));
        assert!(!check_privilege(&roles, "find", "app"));
        assert!(!check_privilege(&roles, "insert", "app"));
    }

    #[test]
    fn multiple_roles_combine() {
        let roles = vec![
            ("read".into(), "db1".into()),
            ("readWrite".into(), "db2".into()),
        ];
        assert!(check_privilege(&roles, "find", "db1"));
        assert!(!check_privilege(&roles, "insert", "db1"));
        assert!(check_privilege(&roles, "insert", "db2"));
    }

    #[test]
    fn unknown_role_denies() {
        let roles = vec![("nonexistent".into(), "db".into())];
        assert!(!check_privilege(&roles, "find", "db"));
    }

    #[test]
    fn empty_roles_denies() {
        let roles: Vec<(String, String)> = vec![];
        assert!(!check_privilege(&roles, "find", "db"));
    }
}
