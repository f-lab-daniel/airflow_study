Docker Compose(C 방법)에서 사용하는 실습용 PostgreSQL 17 (호스트 포트 5433)입니다.

실행, 초기화(study_user / study_db / 테이블 생성), Connection 등록, 정리 방법은
레포 루트 README.md의 "C-3. 실습 DB 준비"와 "정리 (Clean up)"를 참고하세요.

접속:
  docker compose -f postgres-docker/docker-compose.yaml exec db psql -U study_user -d study_db
