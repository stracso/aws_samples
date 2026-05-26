# Athena JDBC Connection (Python)

Connect to Amazon Athena from Python using the official Athena JDBC 3.x driver via [JayDeBeApi](https://github.com/baztian/jaydebeapi).

## Prerequisites

1. **Java 8+** installed with `JAVA_HOME` set
2. **Athena JDBC 3.x uber jar** – download from [AWS docs](https://docs.aws.amazon.com/athena/latest/ug/jdbc-v3-driver.html) and place in this directory as `athena-jdbc-3.x.jar`
3. **Python 3.8+**
4. **AWS credentials** configured in `~/.aws/credentials`

## Setup

```bash
pip install -r requirements.txt
```

## Configuration

Edit the constants at the top of `athena_jdbc_connect.py`:

| Parameter | Description |
|-----------|-------------|
| `JDBC_JAR_PATH` | Path to the Athena JDBC uber jar |
| `REGION` | AWS region (e.g. `us-east-1`) |
| `WORKGROUP` | Athena workgroup name |
| `DATABASE` | Default database to query |
| `OUTPUT_LOCATION` | S3 bucket for query results |
| `AWS_PROFILE` | (Optional) AWS profile name from `~/.aws/credentials` |

## Usage

```bash
python athena_jdbc_connect.py
```

The script will:
1. Connect to Athena using the JDBC driver
2. Run `SHOW DATABASES` to verify connectivity
3. Run `SHOW TABLES` on the configured database

## How It Works

- Uses **JayDeBeApi** to bridge Python → JVM → Athena JDBC driver
- Authentication uses `DefaultChain` (reads `~/.aws/credentials`, env vars, instance profile)
- Set `AWS_PROFILE` to use a specific profile from your credentials file
- Connection URL format: `jdbc:athena://Region=...;Workgroup=...;...;`
