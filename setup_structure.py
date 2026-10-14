from pathlib import Path

folders = [
    "src",
    "dags",
    "dbt",
    "sql",
    "tests",
    "infra",
    "scripts",
    "docs",
    "data",
    ".github/workflows",
]

for folder in folders:
    path = Path(folder)
    path.mkdir(parents=True, exist_ok=True)

    # Git doesn't track empty directories.
    # Keep placeholders out of the local data folder.
    if folder != "data":
        (path / ".gitkeep").touch(exist_ok=True)

for filename in [
    "docker-compose.yml",
    "pyproject.toml",
    ".env.example",
]:
    Path(filename).touch(exist_ok=True)

print("Project structure created successfully!")

