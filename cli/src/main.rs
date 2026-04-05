use clap::{Parser, Subcommand};
use colored::Colorize;
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::PathBuf;

const DEFAULT_API_URL: &str = "https://api.luxdb.dev";

#[derive(Parser)]
#[command(name = "luxctl", version, about = "CLI for Lux Cloud")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, global = true, env = "LUXCTL_API_URL")]
    api_url: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    Login,
    Logout,
    Projects,
    Status {
        #[arg(help = "Project name or ID")]
        project: String,
    },
    Exec {
        #[arg(help = "Project name or ID")]
        project: String,
        #[arg(
            trailing_var_arg = true,
            help = "Command to execute (quote wildcards: KEYS '*')"
        )]
        cmd: Vec<String>,
    },
    Logs {
        #[arg(help = "Project name or ID")]
        project: String,
        #[arg(short, long, default_value = "100")]
        lines: usize,
    },
    Create {
        #[arg(help = "Project name")]
        name: String,
        #[arg(
            short,
            long,
            default_value = "512",
            help = "Memory in MB (128, 512, 2048)"
        )]
        memory: u32,
        #[arg(long, help = "Acknowledge billing charges")]
        accept_charges: bool,
    },
    Restart {
        #[arg(help = "Project name or ID")]
        project: String,
    },
    Destroy {
        #[arg(help = "Project name or ID")]
        project: String,
        #[arg(long, help = "Acknowledge data will be permanently deleted")]
        accept_consequences: bool,
    },
    Connect {
        #[arg(help = "Project name, ID, or connection URL (lux://...)")]
        project: Option<String>,
        #[arg(short = 'H', long, help = "Host (for direct connection)")]
        host: Option<String>,
        #[arg(short, long, help = "Port (for direct connection)")]
        port: Option<u16>,
        #[arg(short = 'a', long, help = "Password (for direct connection)")]
        password: Option<String>,
    },
    Update {
        #[arg(long, help = "Check for updates without installing")]
        check: bool,
    },
    Migrate {
        #[command(subcommand)]
        action: MigrateAction,
    },
}

#[derive(Subcommand)]
enum MigrateAction {
    New {
        #[arg(help = "Migration name (e.g. create_users)")]
        name: String,
    },
    Status {
        #[arg(help = "Project name, ID, or connection URL")]
        project: Option<String>,
        #[arg(short = 'H', long, help = "Host for direct connection")]
        host: Option<String>,
        #[arg(short, long, help = "Port for direct connection")]
        port: Option<u16>,
        #[arg(short = 'a', long, help = "Password for direct connection")]
        password: Option<String>,
    },
    Run {
        #[arg(help = "Project name, ID, or connection URL")]
        project: Option<String>,
        #[arg(short = 'H', long, help = "Host for direct connection")]
        host: Option<String>,
        #[arg(short, long, help = "Port for direct connection")]
        port: Option<u16>,
        #[arg(short = 'a', long, help = "Password for direct connection")]
        password: Option<String>,
    },
}

#[derive(Serialize, Deserialize)]
struct Config {
    token: String,
    api_url: String,
}

#[derive(Deserialize)]
struct ApiResponse<T> {
    data: Option<T>,
    error: Option<String>,
}

#[derive(Deserialize, Debug)]
struct Instance {
    id: String,
    name: String,
    status: String,
    region: String,
    memory_mb: u32,
    port: Option<u16>,
    worker_host: Option<String>,
    password: String,
    #[serde(default)]
    #[allow(dead_code)]
    current_image: Option<String>,
}

#[derive(Deserialize, Debug)]
struct Metrics {
    keys: Option<u64>,
    used_memory_bytes: Option<u64>,
    ops_per_sec: Option<u64>,
    connected_clients: Option<u64>,
}

fn config_path() -> PathBuf {
    let dir = dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".lux");
    std::fs::create_dir_all(&dir).ok();
    dir.join("config.json")
}

fn load_config() -> Option<Config> {
    let path = config_path();
    let data = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&data).ok()
}

fn save_config(config: &Config) {
    let path = config_path();
    let data = serde_json::to_string_pretty(config).unwrap();
    std::fs::write(path, data).ok();
}

fn delete_config() {
    let path = config_path();
    std::fs::remove_file(path).ok();
}

fn get_client(api_url_override: &Option<String>) -> (reqwest::Client, String, String) {
    let config = load_config().unwrap_or_else(|| {
        eprintln!("{}", "Not logged in. Run `luxctl login` first.".red());
        std::process::exit(1);
    });

    let api_url = api_url_override.clone().unwrap_or(config.api_url.clone());
    let client = reqwest::Client::new();
    (client, api_url, config.token)
}

async fn find_project(
    client: &reqwest::Client,
    api_url: &str,
    token: &str,
    name_or_id: &str,
) -> Instance {
    let res = client
        .get(format!("{api_url}/instances"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap_or_else(|e| {
            eprintln!("{} {e}", "Failed to connect:".red());
            std::process::exit(1);
        });

    let body: ApiResponse<Vec<Instance>> = res.json().await.unwrap_or_else(|e| {
        eprintln!("{} {e}", "Failed to parse response:".red());
        std::process::exit(1);
    });

    if let Some(error) = body.error {
        eprintln!("{} {error}", "API error:".red());
        std::process::exit(1);
    }

    let instances = body.data.unwrap_or_default();
    instances
        .into_iter()
        .find(|i| i.id == name_or_id || i.name == name_or_id)
        .unwrap_or_else(|| {
            eprintln!("{} Project '{}' not found", "Error:".red(), name_or_id);
            std::process::exit(1);
        })
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let api_url_override = cli.api_url.clone();

    match cli.command {
        Commands::Login => {
            println!("{}", "Paste your Lux Cloud access token.".bold());
            println!(
                "Get one from: {}",
                "https://luxdb.dev/dashboard/settings".cyan()
            );
            print!("\n{} ", "Token:".bold());
            std::io::stdout().flush().ok();

            let mut token = String::new();
            std::io::stdin().read_line(&mut token).ok();
            let token = token.trim().to_string();

            if token.is_empty() {
                eprintln!("{}", "No token provided.".red());
                std::process::exit(1);
            }

            let api_url = api_url_override
                .clone()
                .unwrap_or_else(|| DEFAULT_API_URL.to_string());

            let client = reqwest::Client::new();
            let res = client
                .get(format!("{api_url}/instances"))
                .header("Authorization", format!("Bearer {token}"))
                .send()
                .await;

            match res {
                Ok(r) if r.status().is_success() => {
                    save_config(&Config { token, api_url });
                    println!("{}", "\nLogged in successfully.".green());
                }
                Ok(r) => {
                    eprintln!("{} HTTP {}", "Login failed:".red(), r.status());
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("{} {e}", "Connection failed:".red());
                    std::process::exit(1);
                }
            }
        }

        Commands::Logout => {
            delete_config();
            println!("{}", "Logged out.".green());
        }

        Commands::Projects => {
            let (client, api_url, token) = get_client(&api_url_override);

            let res = client
                .get(format!("{api_url}/instances"))
                .header("Authorization", format!("Bearer {token}"))
                .send()
                .await
                .unwrap_or_else(|e| {
                    eprintln!("{} {e}", "Failed:".red());
                    std::process::exit(1);
                });

            let body: ApiResponse<Vec<Instance>> = res.json().await.unwrap();

            if let Some(instances) = body.data {
                if instances.is_empty() {
                    println!("{}", "No projects found.".dimmed());
                    return;
                }

                println!(
                    "  {:<16}  {:<10}  {:<6}  {}",
                    "NAME".dimmed(),
                    "STATUS".dimmed(),
                    "REGION".dimmed(),
                    "MEMORY".dimmed()
                );

                for inst in &instances {
                    let status = match inst.status.as_str() {
                        "running" => inst.status.green().to_string(),
                        "error" => inst.status.red().to_string(),
                        _ => inst.status.yellow().to_string(),
                    };

                    println!(
                        "  {:<16}  {:<10}  {:<6}  {}MB",
                        inst.name, status, inst.region, inst.memory_mb,
                    );
                }
            }
        }

        Commands::Status { project } => {
            let (client, api_url, token) = get_client(&api_url_override);
            let inst = find_project(&client, &api_url, &token, &project).await;

            let status = match inst.status.as_str() {
                "running" => inst.status.green().to_string(),
                "error" => inst.status.red().to_string(),
                _ => inst.status.yellow().to_string(),
            };

            println!("{} {}", "Project:".bold(), inst.name);
            println!("{} {}", "ID:".bold(), inst.id.dimmed());
            println!("{} {status}", "Status:".bold());
            println!("{} {}", "Region:".bold(), inst.region);
            println!("{} {}MB", "Memory:".bold(), inst.memory_mb);

            if let (Some(host), Some(port)) = (&inst.worker_host, inst.port) {
                println!("{} lux://:****@{host}:{port}", "Connection:".bold());
            }

            if inst.status == "running" {
                let metrics_res = client
                    .get(format!("{api_url}/metrics/{}/latest", inst.id))
                    .header("Authorization", format!("Bearer {token}"))
                    .send()
                    .await;

                if let Ok(r) = metrics_res {
                    if let Ok(body) = r.json::<ApiResponse<Metrics>>().await {
                        if let Some(m) = body.data {
                            println!();
                            println!("{} {}", "Keys:".bold(), m.keys.unwrap_or(0));
                            println!(
                                "{} {}",
                                "Memory:".bold(),
                                format_bytes(m.used_memory_bytes.unwrap_or(0))
                            );
                            println!(
                                "{} {} ops/sec",
                                "Throughput:".bold(),
                                m.ops_per_sec.unwrap_or(0)
                            );
                            println!("{} {}", "Clients:".bold(), m.connected_clients.unwrap_or(0));
                        }
                    }
                }
            }
        }

        Commands::Exec { project, cmd } => {
            if cmd.is_empty() {
                eprintln!("{}", "No command provided.".red());
                std::process::exit(1);
            }

            let (client, api_url, token) = get_client(&api_url_override);
            let inst = find_project(&client, &api_url, &token, &project).await;
            let command = cmd.join(" ");

            let res = client
                .post(format!("{api_url}/console/{}/exec", inst.id))
                .header("Authorization", format!("Bearer {token}"))
                .json(&serde_json::json!({ "command": command }))
                .send()
                .await
                .unwrap_or_else(|e| {
                    eprintln!("{} {e}", "Failed:".red());
                    std::process::exit(1);
                });

            let body: serde_json::Value = res.json().await.unwrap();
            if let Some(error) = body.get("error").and_then(|v| v.as_str()) {
                eprintln!("{} {error}", "Error:".red());
            } else {
                println!("{}", format_json_value(&body));
            }
        }

        Commands::Logs { project, lines } => {
            let (client, api_url, token) = get_client(&api_url_override);
            let inst = find_project(&client, &api_url, &token, &project).await;

            let res = client
                .get(format!("{api_url}/logs/{}/logs?lines={lines}", inst.id))
                .header("Authorization", format!("Bearer {token}"))
                .send()
                .await
                .unwrap_or_else(|e| {
                    eprintln!("{} {e}", "Failed:".red());
                    std::process::exit(1);
                });

            let body: ApiResponse<serde_json::Value> = res.json().await.unwrap();
            if let Some(data) = body.data {
                if let Some(logs) = data.get("logs").and_then(|v| v.as_str()) {
                    print!("{logs}");
                }
            } else if let Some(error) = body.error {
                eprintln!("{} {error}", "Error:".red());
            }
        }

        Commands::Create {
            name,
            memory,
            accept_charges,
        } => {
            let (client, api_url, token) = get_client(&api_url_override);

            let sizes_res = client
                .get(format!("{api_url}/billing/sizes"))
                .header("Authorization", format!("Bearer {token}"))
                .send()
                .await
                .unwrap_or_else(|e| {
                    eprintln!("{} {e}", "Failed:".red());
                    std::process::exit(1);
                });

            let sizes_body: ApiResponse<Vec<serde_json::Value>> = sizes_res.json().await.unwrap();
            let sizes = sizes_body.data.unwrap_or_default();

            let size = sizes
                .iter()
                .find(|s| s.get("memory_mb").and_then(|v| v.as_u64()) == Some(memory as u64))
                .unwrap_or_else(|| {
                    let available: Vec<String> = sizes
                        .iter()
                        .filter_map(|s| {
                            let mb = s.get("memory_mb")?.as_u64()?;
                            let label = s.get("label")?.as_str()?;
                            Some(format!("{mb}MB ({label})"))
                        })
                        .collect();
                    eprintln!(
                        "{} No size with {}MB. Available: {}",
                        "Error:".red(),
                        memory,
                        available.join(", ")
                    );
                    std::process::exit(1);
                });

            let price_id = size.get("price_id").and_then(|v| v.as_str()).unwrap_or("");
            let price_cents = size
                .get("price_cents")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            if !accept_charges {
                eprintln!(
                    "{} This will create a {}MB instance at ${}/mo.",
                    "Billing:".yellow(),
                    memory,
                    price_cents / 100
                );
                eprintln!("Run with {} to confirm.", "--accept-charges".bold());
                std::process::exit(1);
            }

            println!("{} Creating project '{}'...", "...".dimmed(), name);

            let res = client
                .post(format!("{api_url}/instances"))
                .header("Authorization", format!("Bearer {token}"))
                .json(&serde_json::json!({
                    "name": name,
                    "price_id": price_id,
                }))
                .send()
                .await
                .unwrap_or_else(|e| {
                    eprintln!("{} {e}", "Failed:".red());
                    std::process::exit(1);
                });

            let body: ApiResponse<Instance> = res.json().await.unwrap_or_else(|e| {
                eprintln!("{} {e}", "Failed to parse:".red());
                std::process::exit(1);
            });

            if let Some(error) = body.error {
                eprintln!("{} {error}", "Error:".red());
                std::process::exit(1);
            }

            if let Some(inst) = body.data {
                println!("{} Project '{}' created", "Done.".green(), inst.name);
                println!("{} {}", "ID:".bold(), inst.id);
                println!("{} {}MB", "Memory:".bold(), inst.memory_mb);
                println!("{} {}", "Region:".bold(), inst.region);
                println!(
                    "\n{} Run {} to check when it's ready",
                    "Tip:".bold(),
                    format!("luxctl status {}", inst.name).cyan()
                );
            }
        }

        Commands::Restart { project } => {
            let (client, api_url, token) = get_client(&api_url_override);
            let inst = find_project(&client, &api_url, &token, &project).await;

            println!("{} Restarting '{}'...", "...".dimmed(), inst.name);

            let res = client
                .post(format!("{api_url}/instances/{}/restart", inst.id))
                .header("Authorization", format!("Bearer {token}"))
                .send()
                .await
                .unwrap_or_else(|e| {
                    eprintln!("{} {e}", "Failed:".red());
                    std::process::exit(1);
                });

            if res.status().is_success() {
                println!("{} Project '{}' is restarting.", "Done.".green(), inst.name);
            } else {
                let body: serde_json::Value = res.json().await.unwrap_or_default();
                let msg = body
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown error");
                eprintln!("{} {msg}", "Error:".red());
            }
        }

        Commands::Destroy {
            project,
            accept_consequences,
        } => {
            let (client, api_url, token) = get_client(&api_url_override);
            let inst = find_project(&client, &api_url, &token, &project).await;

            if !accept_consequences {
                eprintln!(
                    "{} This will permanently delete '{}' and all its data.",
                    "Warning:".red(),
                    inst.name
                );
                eprintln!("Run with {} to confirm.", "--accept-consequences".bold());
                std::process::exit(1);
            }

            println!("{} Destroying '{}'...", "...".dimmed(), inst.name);

            let res = client
                .delete(format!("{api_url}/instances/{}", inst.id))
                .header("Authorization", format!("Bearer {token}"))
                .send()
                .await
                .unwrap_or_else(|e| {
                    eprintln!("{} {e}", "Failed:".red());
                    std::process::exit(1);
                });

            if res.status().is_success() {
                println!("{} Project '{}' destroyed.", "Done.".green(), inst.name);
            } else {
                let body: serde_json::Value = res.json().await.unwrap_or_default();
                let msg = body
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Unknown error");
                eprintln!("{} {msg}", "Error:".red());
            }
        }

        Commands::Connect {
            project,
            host,
            port,
            password,
        } => {
            let project = project.unwrap_or_default();
            let (conn_host, conn_port, conn_pass, conn_name) =
                if project.starts_with("lux://") || project.starts_with("redis://") {
                    let url = project
                        .trim_start_matches("lux://")
                        .trim_start_matches("redis://");
                    let (auth, hostport) = if let Some(at) = url.find('@') {
                        (
                            Some(url[..at].trim_start_matches(':').to_string()),
                            &url[at + 1..],
                        )
                    } else {
                        (None, url)
                    };
                    let parts: Vec<&str> = hostport.split(':').collect();
                    let h = parts[0].to_string();
                    let p: u16 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(6379);
                    (
                        h,
                        p,
                        auth.unwrap_or_default(),
                        format!("{}:{}", parts[0], p),
                    )
                } else if host.is_some() || port.is_some() {
                    let h = host.unwrap_or_else(|| "localhost".to_string());
                    let p = port.unwrap_or(6379);
                    let pw = password.unwrap_or_default();
                    let name = format!("{h}:{p}");
                    (h, p, pw, name)
                } else if project.is_empty() {
                    eprintln!(
                        "{} Provide a project name, connection URL, or --host/--port flags",
                        "Error:".red()
                    );
                    std::process::exit(1);
                } else {
                    let (client, api_url, token) = get_client(&api_url_override);
                    let inst = find_project(&client, &api_url, &token, &project).await;

                    if inst.status != "running" {
                        eprintln!(
                            "{} Project '{}' is not running (status: {})",
                            "Error:".red(),
                            inst.name,
                            inst.status
                        );
                        std::process::exit(1);
                    }

                    let h = inst.worker_host.unwrap_or_else(|| "localhost".to_string());
                    let p = inst.port.unwrap_or(6379);
                    (h, p, inst.password, inst.name)
                };

            println!("{} {}:{}", "Connecting to".bold(), conn_host, conn_port);

            let mut stream =
                TcpStream::connect(format!("{conn_host}:{conn_port}")).unwrap_or_else(|e| {
                    eprintln!("{} {e}", "Connection failed:".red());
                    std::process::exit(1);
                });

            if !conn_pass.is_empty() {
                let auth_cmd = format!(
                    "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
                    conn_pass.len(),
                    conn_pass
                );
                stream.write_all(auth_cmd.as_bytes()).ok();

                let mut reader_tmp = BufReader::new(stream.try_clone().unwrap());
                let mut auth_response = String::new();
                reader_tmp.read_line(&mut auth_response).ok();

                if !auth_response.contains("+OK") {
                    eprintln!("{}", "Authentication failed.".red());
                    std::process::exit(1);
                }
            }

            let mut reader = BufReader::new(stream.try_clone().unwrap());

            println!("{} Type commands, Ctrl+C to exit.\n", "Connected.".green());

            loop {
                print!("{} ", format!("{conn_name}>").purple());
                std::io::stdout().flush().ok();

                let mut input = String::new();
                if std::io::stdin().read_line(&mut input).is_err() || input.is_empty() {
                    break;
                }

                let input = input.trim();
                if input.is_empty() {
                    continue;
                }
                if input.eq_ignore_ascii_case("quit") || input.eq_ignore_ascii_case("exit") {
                    break;
                }

                let parts: Vec<&str> = input.split_whitespace().collect();
                let mut resp_cmd = format!("*{}\r\n", parts.len());
                for part in &parts {
                    resp_cmd.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
                }

                stream.write_all(resp_cmd.as_bytes()).ok();

                let mut response = String::new();
                reader.read_line(&mut response).ok();
                let response = response.trim();

                if let Some(rest) = response.strip_prefix('+') {
                    println!("{rest}");
                } else if let Some(rest) = response.strip_prefix('-') {
                    println!("{}", rest.red());
                } else if let Some(rest) = response.strip_prefix(':') {
                    println!("(integer) {rest}");
                } else if let Some(rest) = response.strip_prefix('$') {
                    let len: i64 = rest.parse().unwrap_or(-1);
                    if len < 0 {
                        println!("{}", "(nil)".dimmed());
                    } else {
                        let mut val = String::new();
                        reader.read_line(&mut val).ok();
                        println!("\"{}\"", val.trim());
                    }
                } else if let Some(rest) = response.strip_prefix('*') {
                    let count: i64 = rest.parse().unwrap_or(-1);
                    if count < 0 {
                        println!("{}", "(empty array)".dimmed());
                    } else {
                        for i in 0..count {
                            let mut type_line = String::new();
                            reader.read_line(&mut type_line).ok();
                            let type_line = type_line.trim().to_string();

                            if let Some(rest) = type_line.strip_prefix('$') {
                                let len: i64 = rest.parse().unwrap_or(-1);
                                if len < 0 {
                                    println!("{}) {}", i + 1, "(nil)".dimmed());
                                } else {
                                    let mut val = String::new();
                                    reader.read_line(&mut val).ok();
                                    println!("{}) \"{}\"", i + 1, val.trim());
                                }
                            } else if let Some(rest) = type_line.strip_prefix(':') {
                                println!("{}) (integer) {rest}", i + 1);
                            } else {
                                println!("{}) {}", i + 1, type_line);
                            }
                        }
                    }
                } else {
                    println!("{response}");
                }
            }
        }

        Commands::Update { check } => {
            let current = env!("CARGO_PKG_VERSION");
            println!("{} v{current}", "Current version:".bold());
            println!("{}", "Checking for updates...".dimmed());

            let client = reqwest::Client::builder()
                .user_agent("luxctl")
                .build()
                .unwrap();

            let res = client
                .get("https://api.github.com/repos/lux-db/lux/releases")
                .send()
                .await
                .unwrap_or_else(|e| {
                    eprintln!("{} {e}", "Failed to check for updates:".red());
                    std::process::exit(1);
                });

            let releases: Vec<serde_json::Value> = res.json().await.unwrap_or_default();
            let latest_tag = releases
                .iter()
                .filter_map(|r| r.get("tag_name")?.as_str())
                .find(|t| t.starts_with("luxctl-v"));

            let latest_version = match latest_tag {
                Some(tag) => tag.trim_start_matches("luxctl-v"),
                None => {
                    eprintln!("{}", "No luxctl releases found.".yellow());
                    std::process::exit(1);
                }
            };

            if latest_version == current {
                println!("{}", "Already up to date.".green());
                return;
            }

            println!(
                "{} v{current} -> v{latest_version}",
                "Update available:".yellow()
            );

            if check {
                println!("Run {} to install.", "luxctl update".cyan());
                return;
            }

            let os = if cfg!(target_os = "macos") {
                "macos"
            } else if cfg!(target_os = "linux") {
                "linux"
            } else {
                eprintln!("{}", "Unsupported OS for self-update.".red());
                std::process::exit(1);
            };

            let arch = if cfg!(target_arch = "aarch64") {
                "arm64"
            } else if cfg!(target_arch = "x86_64") {
                "x86_64"
            } else {
                eprintln!("{}", "Unsupported architecture for self-update.".red());
                std::process::exit(1);
            };

            let artifact = format!("luxctl-{os}-{arch}");
            let download_url = format!(
                "https://github.com/lux-db/lux/releases/download/{}/{artifact}.tar.gz",
                latest_tag.unwrap()
            );

            println!("{} Downloading v{latest_version}...", "...".dimmed());

            let tar_bytes = client
                .get(&download_url)
                .send()
                .await
                .unwrap_or_else(|e| {
                    eprintln!("{} {e}", "Download failed:".red());
                    std::process::exit(1);
                })
                .bytes()
                .await
                .unwrap_or_else(|e| {
                    eprintln!("{} {e}", "Download failed:".red());
                    std::process::exit(1);
                });

            let current_exe = std::env::current_exe().unwrap_or_else(|e| {
                eprintln!("{} {e}", "Could not determine binary path:".red());
                std::process::exit(1);
            });

            let tmp_dir = std::env::temp_dir().join("luxctl-update");
            std::fs::create_dir_all(&tmp_dir).ok();
            let tar_path = tmp_dir.join("luxctl.tar.gz");
            std::fs::write(&tar_path, &tar_bytes).unwrap_or_else(|e| {
                eprintln!("{} {e}", "Failed to write temp file:".red());
                std::process::exit(1);
            });

            let status = std::process::Command::new("tar")
                .args([
                    "xzf",
                    tar_path.to_str().unwrap(),
                    "-C",
                    tmp_dir.to_str().unwrap(),
                ])
                .status()
                .unwrap_or_else(|e| {
                    eprintln!("{} {e}", "Failed to extract:".red());
                    std::process::exit(1);
                });

            if !status.success() {
                eprintln!("{}", "Failed to extract update.".red());
                std::process::exit(1);
            }

            let new_binary = tmp_dir.join(&artifact);
            if !new_binary.exists() {
                eprintln!("{} Binary not found in archive.", "Error:".red());
                std::process::exit(1);
            }

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&new_binary, std::fs::Permissions::from_mode(0o755)).ok();
            }

            std::fs::rename(&new_binary, &current_exe).unwrap_or_else(|_| {
                let copy_result = std::fs::copy(&new_binary, &current_exe);
                if copy_result.is_err() {
                    eprintln!(
                        "{} Could not replace binary. Try: sudo luxctl update",
                        "Permission denied:".red()
                    );
                    std::process::exit(1);
                }
            });

            std::fs::remove_dir_all(&tmp_dir).ok();
            println!("{} Updated to v{latest_version}", "Done.".green());
        }

        Commands::Migrate { action } => match action {
            MigrateAction::New { name } => {
                let dir = PathBuf::from("lux/migrations");
                std::fs::create_dir_all(&dir).unwrap_or_else(|e| {
                    eprintln!("{} {e}", "Failed to create migrations dir:".red());
                    std::process::exit(1);
                });
                let ts = chrono::Utc::now().format("%Y%m%d%H%M%S");
                let filename = format!("{}_{}.lux", ts, name);
                let path = dir.join(&filename);
                std::fs::write(&path, "").unwrap_or_else(|e| {
                    eprintln!("{} {e}", "Failed to create file:".red());
                    std::process::exit(1);
                });
                println!("{} {}", "Created:".green(), path.display());
            }

            MigrateAction::Status {
                project,
                host,
                port,
                password,
            } => {
                let mut target = resolve_migrate_target(
                    project.as_deref(),
                    host.as_deref(),
                    port,
                    password.as_deref(),
                    &api_url_override,
                )
                .await;

                let applied = get_applied_migrations(&mut target).await;
                let local = get_local_migrations();

                if local.is_empty() {
                    println!("{}", "No migration files found in lux/migrations/".dimmed());
                    return;
                }

                println!("  {:<40}  {}", "MIGRATION".dimmed(), "STATUS".dimmed());
                for (filename, _) in &local {
                    let status = if applied.contains(filename) {
                        "applied".green().to_string()
                    } else {
                        "pending".yellow().to_string()
                    };
                    println!("  {:<40}  {}", filename, status);
                }
            }

            MigrateAction::Run {
                project,
                host,
                port,
                password,
            } => {
                let mut target = resolve_migrate_target(
                    project.as_deref(),
                    host.as_deref(),
                    port,
                    password.as_deref(),
                    &api_url_override,
                )
                .await;

                ensure_migrations_table(&mut target).await;

                let applied = get_applied_migrations(&mut target).await;
                let local = get_local_migrations();

                let pending: Vec<_> = local
                    .iter()
                    .filter(|(name, _)| !applied.contains(name))
                    .collect();

                if pending.is_empty() {
                    println!("{}", "All migrations are applied.".green());
                    return;
                }

                println!(
                    "{} {} pending migration(s)",
                    "Running".bold(),
                    pending.len()
                );

                for (filename, content) in &pending {
                    print!("  {} {}...", "Applying".dimmed(), filename);
                    std::io::stdout().flush().ok();

                    let lines: Vec<&str> = content
                        .lines()
                        .map(|l| l.trim())
                        .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with("--"))
                        .collect();

                    let mut failed = false;
                    for line in &lines {
                        if let Err(e) = target.exec(line).await {
                            println!(" {}", "FAILED".red());
                            eprintln!("    {} {}", "Command:".dimmed(), line);
                            eprintln!("    {} {}", "Error:".red(), e);
                            failed = true;
                            break;
                        }
                    }

                    if failed {
                        eprintln!(
                            "\n{} Migration failed. Fix the issue and re-run.",
                            "Error:".red()
                        );
                        std::process::exit(1);
                    }

                    let checksum = simple_hash(content);
                    let record_cmd = format!(
                        "TINSERT __migrations filename {} checksum {} applied_at {}",
                        filename,
                        checksum,
                        chrono::Utc::now().timestamp()
                    );
                    if let Err(e) = target.exec(&record_cmd).await {
                        println!(" {}", "FAILED".red());
                        eprintln!("    {} Failed to record migration: {}", "Error:".red(), e);
                        std::process::exit(1);
                    }

                    println!(" {}", "OK".green());
                }

                println!(
                    "{} Applied {} migration(s).",
                    "Done.".green(),
                    pending.len()
                );
            }
        },
    }
}

async fn exec_command(
    client: &reqwest::Client,
    api_url: &str,
    token: &str,
    instance_id: &str,
    command: &str,
) -> Result<String, String> {
    let res = client
        .post(format!("{api_url}/console/{instance_id}/exec"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({ "command": command }))
        .send()
        .await
        .map_err(|e| format!("request failed: {e}"))?;

    let status = res.status();
    let body: serde_json::Value = res
        .json()
        .await
        .map_err(|e| format!("invalid response: {e}"))?;

    if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
        return Err(err.to_string());
    }

    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }

    Ok(format_json_value(&body))
}

async fn exec_command_json(
    client: &reqwest::Client,
    api_url: &str,
    token: &str,
    instance_id: &str,
    command: &str,
) -> Result<serde_json::Value, String> {
    let res = client
        .post(format!("{api_url}/console/{instance_id}/exec"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({ "command": command }))
        .send()
        .await
        .map_err(|e| format!("request failed: {e}"))?;

    let status = res.status();
    let body: serde_json::Value = res
        .json()
        .await
        .map_err(|e| format!("invalid response: {e}"))?;

    if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
        return Err(err.to_string());
    }

    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }

    Ok(body)
}

fn format_json_value(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "(nil)".to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Array(arr) => arr
            .iter()
            .map(format_json_value)
            .collect::<Vec<_>>()
            .join("\n"),
        serde_json::Value::Object(_) => val.to_string(),
    }
}

fn resp_encode(args: &[&str]) -> Vec<u8> {
    let mut cmd = format!("*{}\r\n", args.len());
    for a in args {
        cmd.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
    }
    cmd.into_bytes()
}

fn resp_read_line(reader: &mut BufReader<TcpStream>) -> Result<String, String> {
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|e| format!("read error: {e}"))?;
    Ok(line.trim_end().to_string())
}

const RESP_MAX_DEPTH: u8 = 8;

fn resp_read_response(reader: &mut BufReader<TcpStream>) -> Result<String, String> {
    resp_read_response_inner(reader, 0)
}

fn resp_read_response_inner(
    reader: &mut BufReader<TcpStream>,
    depth: u8,
) -> Result<String, String> {
    if depth > RESP_MAX_DEPTH {
        return Err("RESP nesting too deep".to_string());
    }
    let line = resp_read_line(reader)?;
    if line.is_empty() {
        return Err("empty response".to_string());
    }
    let prefix = line.as_bytes()[0];
    let rest = &line[1..];

    match prefix {
        b'+' => Ok(rest.to_string()),
        b'-' => Err(rest.to_string()),
        b':' => Ok(format!("(integer) {rest}")),
        b'$' => {
            let len: i64 = rest
                .parse()
                .map_err(|_| "invalid bulk length".to_string())?;
            if len < 0 {
                return Ok("(nil)".to_string());
            }
            let mut buf = vec![0u8; (len + 2) as usize];
            use std::io::Read;
            reader
                .read_exact(&mut buf)
                .map_err(|e| format!("read error: {e}"))?;
            Ok(String::from_utf8_lossy(&buf[..len as usize]).to_string())
        }
        b'*' => {
            let count: i64 = rest
                .parse()
                .map_err(|_| "invalid array length".to_string())?;
            if count < 0 {
                return Ok("(empty array)".to_string());
            }
            let mut lines = Vec::new();
            for i in 0..count {
                let elem = resp_read_response_inner(reader, depth + 1)?;
                lines.push(format!("{}) {elem}", i + 1));
            }
            Ok(lines.join("\n"))
        }
        _ => Ok(line),
    }
}

struct DirectConn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl DirectConn {
    fn connect(host: &str, port: u16, password: &str) -> Result<Self, String> {
        let stream = TcpStream::connect(format!("{host}:{port}"))
            .map_err(|e| format!("connection failed: {e}"))?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .ok();
        let reader = BufReader::new(stream.try_clone().unwrap());
        let mut conn = DirectConn { stream, reader };

        if !password.is_empty() {
            let result = conn.exec(&format!("AUTH {password}"));
            if let Err(e) = result {
                return Err(format!("authentication failed: {e}"));
            }
        }
        Ok(conn)
    }

    fn exec(&mut self, command: &str) -> Result<String, String> {
        let parts: Vec<&str> = command.split_whitespace().collect();
        if parts.is_empty() {
            return Err("empty command".to_string());
        }
        self.stream
            .write_all(&resp_encode(&parts))
            .map_err(|e| format!("write error: {e}"))?;
        resp_read_response(&mut self.reader)
    }

    /// Execute TQUERY and return rows as Vec<Vec<String>> (each row is [id, field, val, ...])
    fn exec_tquery(&mut self, command: &str) -> Result<Vec<Vec<String>>, String> {
        let parts: Vec<&str> = command.split_whitespace().collect();
        self.stream
            .write_all(&resp_encode(&parts))
            .map_err(|e| format!("write error: {e}"))?;

        // Read outer array (rows)
        let header = resp_read_line(&mut self.reader)?;
        if let Some(err) = header.strip_prefix('-') {
            return Err(err.to_string());
        }
        let row_count: i64 = header
            .strip_prefix('*')
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if row_count <= 0 {
            return Ok(vec![]);
        }

        let mut rows = Vec::new();
        for _ in 0..row_count {
            // Read inner array (row fields)
            let row_header = resp_read_line(&mut self.reader)?;
            let field_count: i64 = row_header
                .strip_prefix('*')
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let mut fields = Vec::new();
            for _ in 0..field_count {
                let val = resp_read_response(&mut self.reader)?;
                // Strip "(integer) " prefix from integer values
                let val = val.strip_prefix("(integer) ").unwrap_or(&val).to_string();
                fields.push(val);
            }
            rows.push(fields);
        }
        Ok(rows)
    }
}

enum MigrateTarget {
    Cloud {
        client: reqwest::Client,
        api_url: String,
        token: String,
        instance_id: String,
    },
    Direct(DirectConn),
}

impl MigrateTarget {
    async fn exec(&mut self, command: &str) -> Result<String, String> {
        match self {
            MigrateTarget::Cloud {
                client,
                api_url,
                token,
                instance_id,
            } => exec_command(client, api_url, token, instance_id, command).await,
            MigrateTarget::Direct(conn) => conn.exec(command),
        }
    }
}

async fn resolve_migrate_target(
    project: Option<&str>,
    host: Option<&str>,
    port: Option<u16>,
    password: Option<&str>,
    api_url_override: &Option<String>,
) -> MigrateTarget {
    if host.is_some() || port.is_some() {
        let h = host.unwrap_or("localhost");
        let p = port.unwrap_or(6379);
        let pw = password.unwrap_or("");
        match DirectConn::connect(h, p, pw) {
            Ok(conn) => return MigrateTarget::Direct(conn),
            Err(e) => {
                eprintln!("{} {}", "Error:".red(), e);
                std::process::exit(1);
            }
        }
    }

    let project = match project {
        Some(p) if !p.is_empty() => p,
        _ => {
            // No project and no host/port: default to localhost
            match DirectConn::connect("localhost", 6379, password.unwrap_or("")) {
                Ok(conn) => return MigrateTarget::Direct(conn),
                Err(e) => {
                    eprintln!(
                        "{} No project specified and local connection failed: {}",
                        "Error:".red(),
                        e
                    );
                    eprintln!(
                        "Usage: {} or {}",
                        "luxctl migrate run <project>".bold(),
                        "luxctl migrate run --host <host> --port <port>".bold()
                    );
                    std::process::exit(1);
                }
            }
        }
    };

    // Check if it's a connection URL
    if project.starts_with("lux://") || project.starts_with("redis://") {
        let url = project
            .trim_start_matches("redis://")
            .trim_start_matches("lux://");
        let (auth, hostport) = if let Some(at) = url.find('@') {
            (
                Some(url[..at].trim_start_matches(':').to_string()),
                &url[at + 1..],
            )
        } else {
            (None, url)
        };
        let parts: Vec<&str> = hostport.split(':').collect();
        let h = parts[0];
        let p: u16 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(6379);
        let pw = auth.unwrap_or_default();
        match DirectConn::connect(h, p, &pw) {
            Ok(conn) => return MigrateTarget::Direct(conn),
            Err(e) => {
                eprintln!("{} {}", "Error:".red(), e);
                std::process::exit(1);
            }
        }
    }

    // Cloud project
    let (client, api_url, token) = get_client(api_url_override);
    let inst = find_project(&client, &api_url, &token, project).await;
    MigrateTarget::Cloud {
        client,
        api_url,
        token,
        instance_id: inst.id,
    }
}

async fn ensure_migrations_table(target: &mut MigrateTarget) {
    let needs_create = target.exec("TSCHEMA __migrations").await.is_err();
    if needs_create {
        if let Err(e) = target
            .exec("TCREATE __migrations filename:str checksum:str applied_at:int")
            .await
        {
            eprintln!(
                "{} Failed to create __migrations table: {}",
                "Error:".red(),
                e
            );
            std::process::exit(1);
        }
    }
}

async fn get_applied_migrations(target: &mut MigrateTarget) -> std::collections::HashSet<String> {
    let mut applied = std::collections::HashSet::new();

    match target {
        MigrateTarget::Direct(conn) => {
            if let Ok(rows) = conn.exec_tquery("TQUERY __migrations ORDER BY id ASC LIMIT 1000") {
                // Each row: [id, "filename", value, "checksum", value, "applied_at", value]
                for row in &rows {
                    for i in 0..row.len().saturating_sub(1) {
                        if row[i] == "filename" {
                            let name = &row[i + 1];
                            if !name.is_empty() {
                                applied.insert(name.clone());
                            }
                        }
                    }
                }
            }
        }
        MigrateTarget::Cloud {
            client,
            api_url,
            token,
            instance_id,
        } => {
            if let Ok(body) = exec_command_json(
                client,
                api_url,
                token,
                instance_id,
                "TQUERY __migrations ORDER BY id ASC LIMIT 1000",
            )
            .await
            {
                // API returns [[id, "field", "val", ...], ...]
                if let Some(rows) = body.as_array() {
                    for row in rows {
                        if let Some(fields) = row.as_array() {
                            for i in 0..fields.len().saturating_sub(1) {
                                if fields[i].as_str() == Some("filename") {
                                    if let Some(name) = fields[i + 1].as_str() {
                                        if !name.is_empty() {
                                            applied.insert(name.to_string());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    applied
}

fn get_local_migrations() -> Vec<(String, String)> {
    let dir = PathBuf::from("lux/migrations");
    if !dir.exists() {
        return vec![];
    }
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|_| {
            eprintln!("{}", "Failed to read lux/migrations/".red());
            std::process::exit(1);
        })
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "lux")
                .unwrap_or(false)
        })
        .map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            let content = std::fs::read_to_string(e.path()).unwrap_or_default();
            (name, content)
        })
        .collect();
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

fn simple_hash(content: &str) -> String {
    let mut hash: u64 = 5381;
    for byte in content.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
    }
    format!("{:016x}", hash)
}
