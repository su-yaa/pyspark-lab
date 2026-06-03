# pyspark-lab

PySpark, Jenkins, Airflow, Spark Operator를 한 흐름으로 연습하기 위한 예제 저장소입니다.

## 전체 흐름

```text
개발자
-> pyspark-lab repo push
-> Jenkins Pipeline
-> Python test
-> Spark 실행 이미지 build/push
-> Airflow DAG 실행
-> SparkApplication 생성
-> Spark driver/executor pod 실행
-> Spark History Server에서 실행 이력 확인
```

이 예제는 작은 주문 데이터를 Spark DataFrame으로 만들고, 일자/지역/채널 단위 매출 지표를 계산합니다. 로컬 테스트에서는 순수 Python 함수로 비즈니스 규칙을 검증하고, 클러스터 실행에서는 같은 설정을 PySpark job이 사용합니다.

## 주요 파일

- `src/pyspark_lab/config.py`: job 설정과 입력 파라미터 정의
- `src/pyspark_lab/sample_data.py`: 예제 주문 데이터
- `src/pyspark_lab/metrics.py`: 순수 Python 기준 집계 로직
- `jobs/daily_sales_metrics.py`: SparkApplication에서 실행되는 PySpark entrypoint
- `tests/test_metrics.py`: Jenkins에서 실행되는 빠른 단위 테스트
- `Dockerfile`: Spark runtime 위에 이 repo의 job 코드를 올리는 이미지
- `Jenkinsfile`: 테스트 후 이미지 build/push까지 가는 Pipeline 초안

## 로컬 테스트

```bash
python -m pip install -e ".[dev]"
pytest -q
```

## Spark job 파라미터

```bash
python jobs/daily_sales_metrics.py \
  --run-date 2026-06-03 \
  --output-uri file:/tmp/pyspark-lab/daily-sales
```

Kubernetes에서는 Airflow DAG가 위 job을 SparkApplication으로 제출합니다.

## GHCR 이미지

Airflow DAG는 다음 이미지를 실행하도록 준비되어 있습니다.

```text
ghcr.io/su-yaa/pyspark-lab:main
```

Jenkins credential `GHCR`에 GitHub token(classic)을 Secret text로 등록하면 Pipeline이 Kaniko로 이미지를 build/push합니다.

Pipeline이 push하는 태그:

- `ghcr.io/su-yaa/pyspark-lab:<commit-sha>`
- `ghcr.io/su-yaa/pyspark-lab:main`

`main` 태그는 Airflow 예제 DAG가 바로 실행할 수 있도록 쓰는 연습용 moving tag이고, 운영형 배포에서는 commit SHA 태그를 DAG 또는 manifest에 고정하는 방식으로 바꾸는 것이 좋습니다.
