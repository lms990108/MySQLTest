# MySQL Practice

## 프로젝트 설명

이 프로젝트는 MySQL 성능 테스트 및 Docker 환경 설정을 다루는 예제입니다. Docker와 Docker Compose를 활용하여 간단히 실행할 수 있습니다.

---

## 실행 방법

### 1. 프로젝트 준비

- Git 저장소를 클론하거나 제공된 압축 파일을 다운로드 후 압축을 해제합니다.

#### Git Clone

```bash
git clone <repository_url>
cd MYSQL_PRACTICE
```

#### 압축 파일 다운로드

1. 압축 파일을 다운로드합니다.
2. 압축을 해제합니다.
3. 폴더로 이동합니다.

```bash
cd MYSQL_PRACTICE
```

---

### 2. Docker 설치

먼저 Docker와 Docker Compose가 설치되어 있는지 확인합니다. 설치가 필요하면 아래 명령어를 사용하세요:

#### Ubuntu

```bash
sudo apt update
sudo apt install docker.io docker-compose
```

#### macOS

```bash
brew install --cask docker
```

#### Windows

[Docker 공식 웹사이트](https://www.docker.com/products/docker-desktop/)에서 Docker Desktop을 설치하세요.

---

### 3. 프로젝트 실행

Docker Compose를 이용하여 서비스를 실행합니다.

```bash
docker-compose up --build
```

이 명령어는 Docker 이미지를 빌드하고 컨테이너를 실행합니다.

---

## 주요 파일 설명

- `docker-compose.yml`: Docker Compose 설정 파일입니다.
- `mysql-df.dockerfile`: MySQL 컨테이너 설정 파일입니다.
- `python-df.dockerfile`: Python 컨테이너 설정 파일입니다.
- `index_performance_test.py`: 성능 테스트를 위한 Python 스크립트입니다.
- `requirements.txt`: Python 종속성 패키지 목록입니다.

## 테스트 결과 요약

- 인덱싱은 예상처럼 동작하지 않음
- 파티션을 통한 최적화는 유효했음

## TODO

- 인덱싱이 예상처럼 동작하지 않은 이유 분석 필요
