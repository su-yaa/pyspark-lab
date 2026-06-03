pipeline {
  agent {
    kubernetes {
      defaultContainer 'python'
      yaml '''
apiVersion: v1
kind: Pod
spec:
  nodeSelector:
    kubernetes.io/hostname: ubuntu
  containers:
    - name: python
      image: python:3.12-bookworm
      command: ["sleep"]
      args: ["99d"]
      tty: true
      resources:
        requests:
          cpu: 500m
          memory: 1Gi
        limits:
          cpu: "2"
          memory: 3Gi
'''
    }
  }

  options {
    disableConcurrentBuilds()
  }

  environment {
    IMAGE_NAME = 'ghcr.io/su-yaa/pyspark-lab'
  }

  stages {
    stage('Install') {
      steps {
        sh '''
          set -eux
          # Jenkins agent pod는 매 빌드마다 새로 뜨므로 필요한 도구를 명시적으로 설치한다.
          apt-get update
          apt-get install -y --no-install-recommends git ca-certificates
          python -m pip install --upgrade pip
          python -m pip install -e ".[dev]"
        '''
      }
    }

    stage('Test') {
      steps {
        sh '''
          set -eux
          # Spark cluster에 올리기 전, 빠른 단위 테스트로 metric 규칙을 먼저 검증한다.
          pytest -q
        '''
      }
    }

    stage('Image Build Readiness') {
      steps {
        sh '''
          set -eux
          # GHCR credential과 Kaniko executor를 붙이면 이 Dockerfile로 Spark 실행 이미지를 만든다.
          # Airflow DAG는 ghcr.io/su-yaa/pyspark-lab:main 이미지를 SparkApplication에 사용한다.
          test -f Dockerfile
          echo "Ready to build ${IMAGE_NAME}:${GIT_COMMIT} with Kaniko once GHCR credentials are attached."
        '''
      }
    }
  }
}
