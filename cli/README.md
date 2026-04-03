# luxctl

CLI for [Lux Cloud](https://luxdb.dev). Manage projects, run commands, stream logs, and connect to instances from the terminal.

## Install

One-line install:
```bash
curl -fsSL https://raw.githubusercontent.com/lux-db/lux/main/cli/install.sh | sh
```

From source (requires Rust):
```bash
git clone https://github.com/lux-db/lux && cargo install --path lux/cli
```

From GitHub Releases (manual download):
```bash
# macOS (Apple Silicon)
curl -fsSL https://github.com/lux-db/lux/releases/latest/download/luxctl-macos-arm64.tar.gz | tar xz
mv luxctl-macos-arm64 /usr/local/bin/luxctl

# macOS (Intel)
curl -fsSL https://github.com/lux-db/lux/releases/latest/download/luxctl-macos-x86_64.tar.gz | tar xz
mv luxctl-macos-x86_64 /usr/local/bin/luxctl

# Linux (x86_64)
curl -fsSL https://github.com/lux-db/lux/releases/latest/download/luxctl-linux-x86_64.tar.gz | tar xz
mv luxctl-linux-x86_64 /usr/local/bin/luxctl

# Linux (ARM64)
curl -fsSL https://github.com/lux-db/lux/releases/latest/download/luxctl-linux-arm64.tar.gz | tar xz
mv luxctl-linux-arm64 /usr/local/bin/luxctl
```

## Auth

Create a token at [luxdb.dev/dashboard/tokens](https://luxdb.dev/dashboard/tokens), then:

```bash
luxctl login
```

Token and API URL are stored in `~/.lux/config.json`.

## Commands

```bash
luxctl login                                  # authenticate
luxctl logout                                 # clear credentials
luxctl projects                               # list all projects
luxctl create my-app --accept-charges         # create a project (default 512MB)
luxctl create my-app -m 128 --accept-charges  # create with specific memory
luxctl status my-app                          # show status and live metrics
luxctl exec my-app SET hello world            # execute a command
luxctl exec my-app KEYS '*'                   # wildcards need quotes
luxctl logs my-app                            # fetch recent logs
luxctl logs my-app -l 500                     # fetch 500 lines
luxctl restart my-app                         # restart instance
luxctl destroy my-app --accept-consequences   # permanently delete
luxctl connect my-app                         # interactive REPL via Lux Cloud
luxctl migrate new create_users               # create a migration file
luxctl migrate status                         # check status (local instance)
luxctl migrate run                            # run pending migrations (local instance)
luxctl migrate run my-app                     # run against a cloud project
```

## Local Connections

Connect directly to any Lux or Redis instance without going through the cloud API:

```bash
luxctl connect redis://localhost:6379
luxctl connect lux://:password@localhost:6379
luxctl connect -H localhost -p 6379 -a mypassword
```

## Migrations

Manage schema changes with versioned `.lux` files:

```bash
# Create a new migration
luxctl migrate new create_users
# Creates lux/migrations/{timestamp}_create_users.lux

# Check migration status (defaults to localhost:6379)
luxctl migrate status
luxctl migrate status my-app              # cloud project
luxctl migrate status --host 10.0.0.5     # specific host

# Run all pending migrations
luxctl migrate run                               # local instance
luxctl migrate run my-app                        # cloud project
luxctl migrate run lux://:pass@myhost:6379       # connection string
luxctl migrate run --host 10.0.0.5 --port 6379   # specific host
```

Migration files contain Lux commands (one per line). Lines starting with `#` or `--` are comments. Applied migrations are tracked in a `__migrations` table on your project.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `LUXCTL_API_URL` | Override the API URL (default: https://api.luxdb.dev) |

For local development:
```bash
export LUXCTL_API_URL=http://localhost:3000
```
