# Airflow Study

F-Lab Airflow 강의 실습 레포입니다. **Apache Airflow 3.3.2** 기준으로 작성되어 있습니다.

- 강의 노트: [LECTURE.md](LECTURE.md)

**학습 순서**: [LECTURE.md](LECTURE.md)로 개념을 익히고 → 아래 [실행 방법](#실행-방법-고르기) 중 하나로 Airflow를 띄운 뒤 → `dags/`의 1번부터 12번까지 순서대로 실행해 봅니다.

```bash
git clone https://github.com/f-lab-daniel/airflow_study.git
cd airflow_study   # 이후 모든 명령은 이 레포 루트에서 실행합니다
```

## 목차

- [레포 구성](#레포-구성)
- [실행 방법 고르기](#실행-방법-고르기)
- [A. Mac 로컬 설치 (LocalExecutor + PostgreSQL)](#a-mac-로컬-설치-localexecutor--postgresql)
- [B. Mac 로컬에서 CeleryExecutor로 실행](#b-mac-로컬에서-celeryexecutor로-실행)
- [C. Docker Compose (CeleryExecutor)](#c-docker-compose-celeryexecutor)
- [DAG 실행해 보기](#dag-실행해-보기)
- [코드 검사 (Ruff)](#코드-검사-ruff)
- [정리 (Clean up)](#정리-clean-up)
- [트러블슈팅](#트러블슈팅)

## 레포 구성

| 경로 | 내용 | 강의 노트 |
|---|---|---|
| `dags/1_first_dag.py` | 첫 DAG: `PythonOperator` → `EmptyOperator` | [4. DAG](LECTURE.md#4-dag) |
| `dags/2_postgres_loader.py` | `SQLExecuteQueryOperator`로 INSERT, `SqlSensor`로 조건 대기 | [8. Sensor](LECTURE.md#8-sensor) |
| `dags/3_http_dag.py` | `HttpOperator`로 SWAPI 호출 → XCom → CSV → `PostgresHook.copy_expert` | [9. Hook](LECTURE.md#9-hook) |
| `dags/4_task_group.py` | `@task_group`, `Label` | [12. TaskGroup](LECTURE.md#12-taskgroup--label) |
| `dags/5_xcom_dag.py` | `xcom_push` / `xcom_pull` | [13. XCom](LECTURE.md#13-xcom) |
| `dags/6_branch_dag.py` | `BranchPythonOperator` | [14. Branch](LECTURE.md#14-branch-operator) |
| `dags/7_trigger_rule.py` | `TriggerRule.ONE_SUCCESS` | [15. Trigger Rule](LECTURE.md#15-trigger-rule) |
| `dags/8_params_args.py` | DAG/Task `params` 템플릿 | [16. 템플릿](LECTURE.md#16-logical-date와-템플릿) |
| `dags/9_access_variable.py` | `Variable.get()` | [17. Tip](LECTURE.md#17-tip) |
| `dags/10_logical_date.py` | `logical_date`, `ds`, `data_interval_start/end` 컨텍스트 | [16. Logical Date](LECTURE.md#16-logical-date와-템플릿) |
| `dags/11_templating.py` | Jinja 템플릿, `dag_run.conf`, `render_template_as_native_obj` | [16. 템플릿](LECTURE.md#16-logical-date와-템플릿) |
| `dags/12_spark_k8s.py`, `dags/sample.yaml` | `SparkKubernetesOperator`로 SparkApplication 제출 (EKS 필요) | [7. Provider](LECTURE.md#7-provider) |
| `docker-compose.yaml` | 공식 Airflow 3.3.2 Docker Compose (CeleryExecutor) | |
| `postgres-docker/` | Docker용 실습 PostgreSQL 17 (포트 5433) | |
| `.env` (직접 생성) | Docker Compose 실행에 필요. [C-1](#c-1-env-파일-만들기-필수) 참고 | |

DAG에서 사용하는 Connection / Variable:

| ID | 종류 | 사용 DAG |
|---|---|---|
| `my_postgres_connection` | Postgres Connection → 실습 DB `study_db` | 2, 3 |
| `my_http_connection` | HTTP Connection → `https://swapi.dev` | 3 |
| `eks-flab` | Kubernetes Connection | 12 |
| `var1`, `var2`, `var_dict` | Variable | 9 |

## 실행 방법 고르기

| 방법 | Executor | 필요한 것 | 추천 상황 |
|---|---|---|---|
| **A. Mac 로컬** | LocalExecutor | Homebrew, uv, PostgreSQL | 처음 시작할 때, 코드 수정·디버깅 |
| **B. Mac 로컬 + Celery** | CeleryExecutor | A + Redis | 워커/큐 구조를 직접 띄워 보고 싶을 때 |
| **C. Docker Compose** | CeleryExecutor | Docker Desktop (메모리 4GB 이상) | 운영과 비슷한 멀티 컨테이너 구성 |

Airflow는 아래 프로세스들로 동작합니다. 어떤 방법이든 이 구성 요소가 모두 떠 있어야 합니다.

| 프로세스 | 명령 | 역할 |
|---|---|---|
| API 서버 | `airflow api-server` | UI(http://localhost:8080) + REST API + 워커용 Task Execution API |
| 스케줄러 | `airflow scheduler` | DAG Run 생성, 실행할 태스크를 Executor에 전달 |
| DAG 프로세서 | `airflow dag-processor` | `dags/` 폴더의 파이썬 파일을 파싱해 DB에 저장 |
| 트리거러 | `airflow triggerer` | Deferrable Operator의 대기 처리 |
| 워커 (Celery일 때) | `airflow celery worker` | 큐에서 태스크를 꺼내 실행 |

---

## A. Mac 로컬 설치 (LocalExecutor + PostgreSQL)

Docker 없이 Mac(Apple Silicon)에서 직접 실행합니다. Airflow 메타데이터 DB와 실습 DB 모두 Homebrew PostgreSQL을 사용합니다.

### A-0. 요구 사항

| 항목 | 지원 범위 (Airflow 3.3.2) | 이 가이드 |
|---|---|---|
| Python | 3.10 – 3.14 | 3.12 |
| PostgreSQL | 14 – 18 | 17 |
| 메모리 | 4GB 이상 권장 | |

```bash
# Homebrew가 없다면 https://brew.sh 참고
brew install uv
```

`uv`가 Python 설치와 가상환경 생성을 함께 처리하므로 Python을 따로 설치하지 않아도 됩니다.

### A-1. PostgreSQL 설치

```bash
brew install postgresql@17

# postgresql@17은 keg-only라 PATH에 직접 추가해야 합니다
echo 'export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# 백그라운드 서비스로 시작 (로그인 시 자동 시작)
brew services start postgresql@17

# 확인
psql --version            # psql (PostgreSQL) 17.x
pg_isready -h localhost   # localhost:5432 - accepting connections
```

> 5432 포트를 다른 프로세스가 쓰고 있다면 `lsof -i :5432`로 확인하고 종료하세요.

### A-2. PostgreSQL 초기화 (사용자 · DB 생성)

Homebrew PostgreSQL은 Mac 로그인 사용자 이름으로 슈퍼유저를 만들어 두므로 바로 접속할 수 있습니다.

```bash
psql postgres
```

DB를 두 개 만듭니다.

- `airflow_db`: Airflow 메타데이터 DB (DAG Run, Task Instance, XCom, Connection, Variable 등)
- `study_db`: DAG 실습에서 데이터를 읽고 쓰는 DB (`my_postgres_connection`이 가리키는 곳)

```sql
-- 1) Airflow 메타데이터 DB
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
CREATE DATABASE airflow_db OWNER airflow_user ENCODING 'UTF8';
ALTER USER airflow_user SET search_path = public;

-- 2) 실습 DB
CREATE USER study_user WITH PASSWORD 'study_pass';
CREATE DATABASE study_db OWNER study_user ENCODING 'UTF8';

\q
```

> PostgreSQL 15부터는 `public` 스키마의 CREATE 권한이 DB 소유자에게만 있습니다. 위처럼 `OWNER`를 지정해 DB를 만들면 별도 GRANT 없이 테이블을 만들 수 있습니다.

실습 테이블을 만듭니다.

```bash
psql -h localhost -U study_user -d study_db   # 비밀번호: study_pass
```

```sql
-- 2_postgres_loader.py
CREATE TABLE public.sample_table (
    id    serial PRIMARY KEY,
    key   VARCHAR(50) NOT NULL,
    value VARCHAR(50) NOT NULL
);

-- 3_http_dag.py (DAG가 CREATE TABLE IF NOT EXISTS로 만들기도 합니다)
CREATE TABLE IF NOT EXISTS public.starwars_character (
    name   TEXT NOT NULL,
    height TEXT NOT NULL,
    mass   TEXT NOT NULL
);

\dt
\q
```

### A-3. Airflow 설치

레포 루트에서 실행합니다.

```bash
# Python 3.12 가상환경 생성 후 활성화
uv venv --python 3.12
source .venv/bin/activate

AIRFLOW_VERSION=3.3.2
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# postgres : psycopg2 드라이버 + Postgres provider (2, 3번 DAG)
# http     : HttpOperator (3번 DAG)
# celery   : CeleryExecutor (B 방법에서 사용)
# cncf.kubernetes : SparkKubernetesOperator (12번 DAG)
uv pip install "apache-airflow[postgres,http,celery,cncf.kubernetes]==${AIRFLOW_VERSION}" \
  --constraint "${CONSTRAINT_URL}"

airflow version   # 3.3.2
```

> **constraints 파일**은 Airflow가 테스트를 마친 의존성 버전 목록입니다. 빼고 설치하면 의존성 충돌이 날 수 있으니 항상 함께 지정하세요.

`PythonOperator`, `BashOperator`, `EmptyOperator` 같은 기본 오퍼레이터는 `apache-airflow-providers-standard`에, SQL 실행/센서는 `apache-airflow-providers-common-sql`에 들어 있고 둘 다 기본으로 설치됩니다.

### A-4. 환경 변수 설정

Airflow 설정은 `airflow.cfg` 파일 대신 `AIRFLOW__<섹션>__<키>` 형식의 환경 변수로도 지정할 수 있습니다. 매번 입력하지 않도록 파일로 만들어 둡니다 (`airflow.env`, `airflow-celery.env`는 `.gitignore`에 포함되어 있습니다).

**레포 루트에서** 실행하세요. `$PWD`가 현재 경로로 바뀌어 DAG 폴더가 이 레포의 `dags/`로 지정됩니다.

```bash
cat > airflow.env <<EOF
# Airflow가 airflow.cfg, 로그, 관리자 비밀번호 파일을 두는 위치
export AIRFLOW_HOME="\$HOME/airflow"

# 이 레포의 dags/ 폴더를 DAG 폴더로 사용
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# 메타데이터 DB (지정하지 않으면 SQLite를 사용)
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow_user:airflow_pass@localhost:5432/airflow_db"
export AIRFLOW__CORE__EXECUTOR=LocalExecutor

# macOS에서 fork된 태스크 프로세스가 죽거나 멈추는 문제 방지
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export NO_PROXY="*"
EOF

source airflow.env
echo $AIRFLOW__CORE__DAGS_FOLDER   # .../airflow_study/dags 가 나와야 합니다
```

### A-5. 메타데이터 DB 초기화

```bash
airflow db migrate   # airflow_db에 테이블 생성 (버전 업그레이드 시에도 같은 명령)
airflow db check     # 연결 확인

psql -h localhost -U airflow_user -d airflow_db -c '\dt' | head   # dag, dag_run, task_instance ... 확인
```

### A-6. Connection · Variable 등록

DAG 코드에 쓰인 ID 그대로 등록합니다. UI의 **Admin → Connections / Variables**에서도 등록할 수 있습니다.

```bash
# 2_postgres_loader.py, 3_http_dag.py
airflow connections add my_postgres_connection \
  --conn-type postgres \
  --conn-host localhost --conn-port 5432 \
  --conn-login study_user --conn-password study_pass \
  --conn-schema study_db

# 3_http_dag.py → https://swapi.dev/api/people/1/
airflow connections add my_http_connection \
  --conn-type http \
  --conn-host swapi.dev \
  --conn-schema https

# 9_access_variable.py
airflow variables set var1 value1
airflow variables set var2 value2
airflow variables set var_dict '{"var1": "value1", "var2": "value2"}'

airflow connections list
airflow variables list
```

### A-7. 실행

**방법 1. `airflow standalone` (가장 간단)**

```bash
source .venv/bin/activate && source airflow.env
airflow standalone
```

`standalone`은 `db migrate`와 관리자 계정 생성을 하고, API 서버 · 스케줄러 · DAG 프로세서 · 트리거러를 한 번에 띄웁니다.

- UI: http://localhost:8080
- 계정: `admin` / 비밀번호는 실행 로그의 `Password for user 'admin': ...` 또는 아래 파일에서 확인
  ```bash
  cat "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
  ```

**방법 2. 프로세스별로 실행 (구조 이해용)**

터미널 4개에서 각각 `source .venv/bin/activate && source airflow.env` 후 실행합니다.

```bash
airflow api-server -p 8080   # UI + REST API
airflow scheduler            # 스케줄링 + LocalExecutor로 태스크 실행
airflow dag-processor        # DAG 파일 파싱
airflow triggerer            # Deferrable Operator 대기 처리
```

**상태 확인**

```bash
curl -s localhost:8080/api/v2/monitor/health
# {"metadatabase":{"status":"healthy"},"scheduler":{"status":"healthy",...},"triggerer":{...},"dag_processor":{...}}

airflow dags list                 # 12개 DAG가 보여야 합니다
airflow dags list-import-errors   # No data found 이면 정상
```

이제 [DAG 실행해 보기](#dag-실행해-보기)로 넘어가세요. 종료와 삭제는 [정리 (Clean up)](#정리-clean-up)에 모아 두었습니다.

---

## B. Mac 로컬에서 CeleryExecutor로 실행

A를 마친 상태에서 Redis만 추가하면 Mac에서도 CeleryExecutor로 실행할 수 있습니다. 스케줄러가 태스크를 Redis 큐에 넣고, 별도 프로세스인 Celery 워커가 꺼내서 실행합니다.

```mermaid
flowchart LR
    S[scheduler] -->|태스크 전달| R[(Redis 큐)]
    R --> W[celery worker]
    W -->|상태 보고| API[api-server]
    API --> DB[(airflow_db)]
    S --> DB
```

### B-1. Redis 설치 · 실행

```bash
brew install redis
brew services start redis
redis-cli ping   # PONG
```

### B-2. Celery 설정 파일 추가

A-3에서 `celery` extra를 함께 설치했으므로 추가 설치는 필요 없습니다 (`airflow providers list | grep celery`로 확인).

Celery 설정은 별도 파일로 두고, A의 `airflow.env` **다음에** 읽어서 Executor 설정을 덮어씁니다. 이렇게 하면 LocalExecutor로 돌아갈 때 파일을 고칠 필요가 없습니다.

```bash
cat > airflow-celery.env <<'EOF'
export AIRFLOW__CORE__EXECUTOR=CeleryExecutor
export AIRFLOW__CELERY__BROKER_URL="redis://localhost:6379/0"
export AIRFLOW__CELERY__RESULT_BACKEND="db+postgresql://airflow_user:airflow_pass@localhost:5432/airflow_db"
EOF
```

### B-3. 실행

터미널 5개에서 각각 아래처럼 환경을 불러온 뒤 실행합니다. `standalone`은 Celery 워커를 띄우지 않으므로 프로세스별로 실행합니다.

```bash
source .venv/bin/activate && source airflow.env && source airflow-celery.env
```

```bash
airflow api-server -p 8080
airflow scheduler
airflow dag-processor
airflow triggerer
airflow celery worker        # "celery@<호스트명> ready" 가 보이면 준비 완료
```

- 로그인 계정은 A와 같습니다 (`$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`).
- LocalExecutor로 돌아가려면 `airflow-celery.env`를 불러오지 않으면 됩니다.

---

## C. Docker Compose (CeleryExecutor)

공식 Airflow 3.3.2 `docker-compose.yaml`에 이 레포용 설정(예제 DAG 끄기, `kubeconfig/` 마운트, AWS 자격 증명, `awscli`)을 더한 파일입니다. Mac에 Python, PostgreSQL을 설치하지 않아도 됩니다.

| 컨테이너 | 역할 |
|---|---|
| `postgres` (16) | Airflow 메타데이터 DB |
| `redis` | Celery 브로커 |
| `airflow-apiserver` | UI + API, http://localhost:8080 |
| `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer` | 위 표와 동일 |
| `airflow-worker` | Celery 워커 |
| `airflow-init` | 최초 1회 DB 마이그레이션, 관리자 계정 생성 |

### C-1. `.env` 파일 만들기 (필수)

`docker-compose.yaml`은 모든 Airflow 컨테이너에 `env_file: .env`를 지정하고 있어서, **`.env` 파일이 없으면 `docker compose up`이 바로 실패**합니다 (`open .../.env: no such file or directory`).

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
mkdir -p logs plugins config kubeconfig
```

- `AIRFLOW_UID`: 컨테이너 안의 Airflow가 이 UID로 실행되어, `logs/` 등에 만들어지는 파일의 소유자가 내 계정이 됩니다.
- `.env`는 `.gitignore`에 포함되어 있어 커밋되지 않습니다. 한 번만 만들면 됩니다.
- 필요하면 `.env`에 `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `_AIRFLOW_WWW_USER_PASSWORD` 등을 추가로 넣을 수 있습니다.

### C-2. 실행

```bash
docker compose up -d
docker compose ps    # 모든 컨테이너가 healthy가 될 때까지 1~2분
```

- UI: http://localhost:8080 (`airflow` / `airflow`)
- CLI: `docker compose exec airflow-scheduler airflow dags list`

### C-3. 실습 DB 준비 (`postgres-docker/`)

Airflow 메타데이터 DB와 별도로 실습용 PostgreSQL(포트 5433)을 띄우고 초기화합니다. Mac에 `psql`이 없어도 되도록 컨테이너 안의 `psql`을 사용합니다.

```bash
docker compose -f postgres-docker/docker-compose.yaml up -d

docker compose -f postgres-docker/docker-compose.yaml exec -T db psql -U postgres <<'EOF'
CREATE USER study_user WITH PASSWORD 'study_pass';
CREATE DATABASE study_db OWNER study_user ENCODING 'UTF8';
EOF

docker compose -f postgres-docker/docker-compose.yaml exec -T db psql -U study_user -d study_db <<'EOF'
CREATE TABLE public.sample_table (
    id    serial PRIMARY KEY,
    key   VARCHAR(50) NOT NULL,
    value VARCHAR(50) NOT NULL
);
CREATE TABLE IF NOT EXISTS public.starwars_character (
    name   TEXT NOT NULL,
    height TEXT NOT NULL,
    mass   TEXT NOT NULL
);
EOF
```

### C-4. Connection · Variable 등록

Airflow 컨테이너에서 실습 DB로 접속하므로 host는 `host.docker.internal`(Mac 호스트), port는 `5433`입니다. 레포 루트에서 실행합니다.

```bash
docker compose exec airflow-scheduler airflow connections add my_postgres_connection \
  --conn-type postgres \
  --conn-host host.docker.internal --conn-port 5433 \
  --conn-login study_user --conn-password study_pass \
  --conn-schema study_db

docker compose exec airflow-scheduler airflow connections add my_http_connection \
  --conn-type http --conn-host swapi.dev --conn-schema https

docker compose exec airflow-scheduler airflow variables set var1 value1
docker compose exec airflow-scheduler airflow variables set var2 value2
docker compose exec airflow-scheduler airflow variables set var_dict '{"var1": "value1", "var2": "value2"}'
```

---

## DAG 실행해 보기

**UI에서**: DAG 목록에서 토글을 켜고(unpause) ▶ 버튼으로 실행합니다. DAG는 기본적으로 일시정지 상태로 등록됩니다.

> 스케줄이 있는 DAG는 unpause하는 순간 가장 최근 스케줄 1회가 바로 실행됩니다. 여기에 수동 실행까지 하면 2번 실행되므로, `http_dag`처럼 데이터를 쌓는 DAG는 결과 행이 2개가 될 수 있습니다.

**CLI에서**

```bash
# 스케줄러 없이 DAG 전체를 한 번 실행 (디버깅용, 결과가 바로 터미널에 출력)
airflow dags test sample_dag

# 스케줄러를 통해 실행
airflow dags unpause sample_dag
airflow dags trigger sample_dag
airflow dags list-runs sample_dag

# 태스크 하나만 실행
airflow tasks test sample_dag print_hello_task
```

> **C(Docker)에서는** 위 명령 앞에 `docker compose exec airflow-scheduler`를 붙입니다. 예: `docker compose exec airflow-scheduler airflow dags test sample_dag`

**DAG별 메모**

| DAG | 확인할 것 |
|---|---|
| `postgres_loader` | `SqlSensor`가 `key='hello1'` 행이 생길 때까지 대기합니다. 아래 [실습 DB 접속](#실습-db-접속)으로 `INSERT INTO sample_table (key, value) VALUES ('hello1', 'world1');` 실행 |
| `http_dag` | 실행 후 `SELECT * FROM starwars_character;` → `Luke Skywalker \| 172 \| 77` |
| `xcom_dag` | 로그에서 `Generated number: N` 과 `Received number: N` 이 같은 값 |
| `branch_operator_dag` | 짝수/홀수 경로 중 하나만 실행되고 나머지는 `skipped` |
| `params_argument` | 로그에 `v1`, `v2 v3` |
| `templating` | conf를 넣어 실행: `airflow dags trigger templating -c '{"numbers": [1, 2, 3]}'` → `sum_numbers1`, `sum_numbers2` 반환값 6 |
| `print_logical_date` | 2분마다 실행되며 `logical_date`, `data_interval` 출력 |
| `spark_submit_sample` | EKS 클러스터와 `eks-flab` Connection, `kubeconfig/`가 있어야 실행됩니다 (강의 환경 전용) |

### 실습 DB 접속

```bash
# A, B (Homebrew PostgreSQL)
psql -h localhost -U study_user -d study_db

# C (postgres-docker 컨테이너)
docker compose -f postgres-docker/docker-compose.yaml exec db psql -U study_user -d study_db
```

---

## 코드 검사 (Ruff)

[Ruff](https://docs.astral.sh/ruff/)는 파이썬 린터입니다. Airflow 전용 규칙 중 `AIR3`은 Airflow 3에서 지원하지 않는 import 경로나 인자를 찾아 줍니다. 인터넷 예제나 오래된 블로그 코드를 가져왔을 때 확인용으로 쓰세요.

```bash
uvx ruff check dags/ --select AIR3 --preview          # 검사 → All checks passed!
uvx ruff check dags/ --select AIR3 --preview --fix    # 자동 수정 가능한 항목 수정
```

---

## 정리 (Clean up)

필요한 단계까지만 실행하세요. 위에서 아래로 갈수록 더 많이 지웁니다.

### A · B (Mac 로컬)

**1) 멈추기** — 데이터는 그대로 남습니다.

```bash
# Airflow: 실행 중인 각 터미널에서 Ctrl+C

brew services stop postgresql@17
brew services stop redis            # B를 했다면
```

다시 시작할 때는 `brew services start postgresql@17` (B는 `redis`도) 후 [A-7](#a-7-실행) / [B-3](#b-3-실행)부터 하면 됩니다.

**2) 데이터 초기화** — 설치는 유지하고 DAG 실행 기록, Connection, 실습 데이터를 지운 뒤 처음 상태로 되돌립니다.

```bash
# PostgreSQL이 실행 중이어야 합니다
psql postgres -c 'DROP DATABASE IF EXISTS airflow_db;' -c 'DROP DATABASE IF EXISTS study_db;'
rm -rf "$HOME/airflow"              # AIRFLOW_HOME (airflow.cfg, 로그, 관리자 비밀번호 파일)
redis-cli FLUSHALL                  # B를 했다면: 큐에 남은 태스크 삭제
```

이후 [A-2](#a-2-postgresql-초기화-사용자--db-생성)의 `CREATE DATABASE` 부분부터 다시 진행합니다 (사용자는 남아 있으므로 `CREATE USER`는 생략).

**3) 완전 삭제** — 이 레포 실습을 위해 설치한 것을 모두 지웁니다.

```bash
# Airflow 가상환경, 설정 파일 (레포 루트에서)
deactivate 2>/dev/null
rm -rf .venv airflow.env airflow-celery.env "$HOME/airflow"
rm -rf logs/*

# PostgreSQL 사용자 · DB 삭제 후 PostgreSQL 제거
psql postgres -c 'DROP DATABASE IF EXISTS airflow_db;' -c 'DROP DATABASE IF EXISTS study_db;' \
              -c 'DROP USER IF EXISTS airflow_user;' -c 'DROP USER IF EXISTS study_user;'
brew services stop postgresql@17
brew uninstall postgresql@17
rm -rf /opt/homebrew/var/postgresql@17   # PostgreSQL 데이터 디렉토리 (다른 DB도 함께 삭제됨)

# Redis (B를 했다면)
brew services stop redis
brew uninstall redis
```

`~/.zshrc`에 추가한 `export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"` 줄도 지웁니다. `uv`는 다른 프로젝트에서 쓰지 않는다면 `brew uninstall uv`로 지웁니다.

> `/opt/homebrew/var/postgresql@17` 삭제는 이 PostgreSQL에 있는 **모든 DB**를 지웁니다. 다른 프로젝트에서도 쓰고 있다면 이 줄은 건너뛰세요.

### C (Docker Compose)

**1) 멈추기** — 컨테이너를 지우지만 DB 볼륨은 남습니다. 다음 `up`에서 이어서 사용합니다.

```bash
# 레포 루트에서
docker compose down
docker compose -f postgres-docker/docker-compose.yaml down
```

**2) 데이터 초기화** — DB 볼륨까지 지웁니다. 다음 `up`에서 `airflow-init`이 DB를 새로 만듭니다.

```bash
docker compose down -v
docker compose -f postgres-docker/docker-compose.yaml down -v
rm -rf logs/*
```

이후 [C-2](#c-2-실행)부터 다시 진행합니다. 실습 DB도 지웠으므로 C-3, C-4도 다시 해야 합니다.

**3) 완전 삭제** — 이미지와 로컬 파일까지 지웁니다.

> `--rmi all`은 `apache/airflow:3.3.2`, `postgres:16`, `postgres:17`, `redis` 이미지를 지웁니다. 다른 프로젝트에서 같은 이미지를 쓰고 있다면 `--rmi all`을 빼세요.

```bash
docker compose down -v --rmi all
docker compose -f postgres-docker/docker-compose.yaml down -v --rmi all
rm -rf logs/* plugins config kubeconfig .env
```

남은 것이 없는지 확인합니다.

```bash
docker ps -a --filter name=airflow_study
docker ps -a --filter name=postgres-docker
docker volume ls --filter name=airflow_study
```

---

## 트러블슈팅

| 증상 | 원인 / 해결 |
|---|---|
| 태스크가 시작 직후 로그 없이 죽음, `objc[...]: ... fork() was called` | macOS fork 안전성 검사. `export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` |
| HTTP 호출 태스크가 macOS에서 멈춤 | fork 후 시스템 프록시 조회가 멈추는 문제. `export NO_PROXY="*"` |
| `permission denied for schema public` | DB 소유자가 아닌 사용자로 테이블 생성. `OWNER`로 DB를 만들거나 해당 DB에서 `GRANT ALL ON SCHEMA public TO <user>;` |
| `connection refused ... port 5432` | `brew services list`로 PostgreSQL 상태 확인 → `brew services restart postgresql@17` |
| DAG가 UI에 안 보임 | `AIRFLOW__CORE__DAGS_FOLDER` 경로, `dag-processor` 실행 여부, `airflow dags list-import-errors` 확인 |
| `psql`이 다른 버전을 가리킴 | `which psql` 확인. `/opt/homebrew/opt/postgresql@17/bin`이 PATH 앞에 오도록 수정 |
| Celery 태스크가 `queued`에서 멈춤 | `airflow celery worker` 실행 여부, `redis-cli ping` 확인 |
| Docker `postgres` 컨테이너 unhealthy, 로그에 `database files are incompatible with server` | 다른 PostgreSQL 버전으로 만든 볼륨이 남아 있음. 데이터가 필요 없다면 `docker compose down -v` 후 다시 `up` |
| `docker compose up`이 `open .../.env: no such file or directory`로 실패 | [C-1](#c-1-env-파일-만들기-필수)의 `.env` 생성 |
| SWAPI 호출 실패 | `swapi.dev`가 간헐적으로 응답하지 않습니다. host를 미러 `swapi.info`로 바꿔 보세요 |

## 참고 자료

- [Airflow 3.3.2 문서](https://airflow.apache.org/docs/apache-airflow/stable/index.html) · [Release Notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html)
- [Quick Start](https://airflow.apache.org/docs/apache-airflow/stable/start.html) · [Installation from PyPI](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html) · [Prerequisites](https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html)
- [Set up a Database Backend](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html)
- [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Celery Executor](https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html)
