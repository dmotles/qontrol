use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "qontrol", version, about = "Qumulo Data Fabric CLI")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Profile name to use (overrides default)
    #[arg(long, global = true, env = "QONTROL_PROFILE")]
    pub profile: Option<String>,

    #[command(flatten)]
    pub global_opts: GlobalOpts,
}

#[derive(Args)]
pub struct GlobalOpts {
    /// Output as JSON
    #[arg(long, global = true)]
    pub json: bool,

    /// Suppress non-error output
    #[arg(long, global = true)]
    pub quiet: bool,

    /// Increase verbosity (-v info, -vv debug, -vvv trace)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    pub verbose: u8,

    /// Request timeout in seconds
    #[arg(long, global = true, default_value = "30")]
    pub timeout: u64,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Manage connection profiles
    Profile {
        #[command(subcommand)]
        command: ProfileCommands,
    },
    /// Make raw API requests
    Api {
        #[command(subcommand)]
        command: ApiCommands,
    },
    /// Cluster information
    Cluster {
        #[command(subcommand)]
        command: ClusterCommands,
    },
    /// Cluster health dashboard
    Dashboard {
        /// Continuously refresh the dashboard
        #[arg(long)]
        watch: bool,
        /// Refresh interval in seconds (used with --watch)
        #[arg(long, default_value = "5")]
        interval: u64,
    },
    /// Snapshot management
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommands,
    },
    /// Filesystem browsing commands
    Fs {
        #[command(subcommand)]
        command: FsCommands,
    },
}

#[derive(Subcommand)]
pub enum ProfileCommands {
    /// Add a new profile (interactive login if --token is omitted)
    Add {
        /// Profile name
        name: String,
        /// Cluster hostname or IP
        #[arg(long)]
        host: Option<String>,
        /// REST API port
        #[arg(long, default_value = "8000")]
        port: u16,
        /// Bearer token for authentication (skips interactive login)
        #[arg(long)]
        token: Option<String>,
        /// Skip TLS certificate verification
        #[arg(long)]
        insecure: bool,
        /// Set as the default profile
        #[arg(long)]
        default: bool,
        /// Username for non-interactive login (requires --password and --host)
        #[arg(long, hide = true)]
        username: Option<String>,
        /// Password for non-interactive login (visible in process list; prefer interactive login)
        #[arg(long, hide = true)]
        password: Option<String>,
        /// Access token expiry for non-interactive login: 6months, 1year, never (default: 1year)
        #[arg(long, hide = true, default_value = "1year")]
        expiry: String,
    },
    /// List all profiles
    List,
    /// Remove a profile
    Remove {
        /// Profile name to remove
        name: String,
    },
    /// Show profile details
    Show {
        /// Profile name (uses default if omitted)
        name: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum ApiCommands {
    /// Send a raw API request
    Raw {
        /// HTTP method (GET, POST, PUT, DELETE)
        method: String,
        /// API path (e.g. /v1/cluster/nodes/)
        path: String,
        /// Request body as JSON string
        #[arg(long)]
        body: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum ClusterCommands {
    /// Show cluster information
    Info,
}

#[derive(Subcommand)]
pub enum SnapshotCommands {
    /// List all snapshots with capacity usage
    List,
    /// Show details for a specific snapshot
    Show {
        /// Snapshot ID
        id: u64,
    },
    /// List snapshot policies
    Policies,
    /// Recommend snapshots for deletion using GFS retention
    RecommendDelete {
        /// Number of daily snapshots to keep
        #[arg(long, default_value = "7")]
        keep_daily: u32,
        /// Number of weekly snapshots to keep
        #[arg(long, default_value = "4")]
        keep_weekly: u32,
        /// Number of monthly snapshots to keep
        #[arg(long, default_value = "3")]
        keep_monthly: u32,
    },
    /// Show changes between two snapshots
    Diff {
        /// Newer snapshot ID
        newer: u64,
        /// Older snapshot ID
        older: u64,
    },
}

#[derive(Subcommand)]
pub enum FsCommands {
    /// List directory contents
    Ls {
        /// Path to list (default: /)
        #[arg(default_value = "/")]
        path: String,
        /// Show detailed information (permissions, size, timestamps)
        #[arg(short, long)]
        long: bool,
        /// Sort by field: name, size, type (default: name)
        #[arg(short, long, default_value = "name")]
        sort: String,
        /// Maximum number of entries to return
        #[arg(long)]
        limit: Option<u32>,
    },
    /// Show recursive directory tree
    Tree {
        /// Path to show tree for (default: /)
        #[arg(default_value = "/")]
        path: String,
        /// Maximum depth to recurse (default: 3)
        #[arg(short = 'd', long, default_value = "3")]
        max_depth: u32,
    },
    /// Show detailed file/directory attributes
    Stat {
        /// Path to inspect
        path: String,
    },
}
