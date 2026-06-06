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
          cpu: 100m
          memory: 512Mi
        limits:
          cpu: "1"
          memory: 2Gi
    - name: kaniko
      image: gcr.io/kaniko-project/executor:v1.24.0-debug
      command: ["/busybox/sleep"]
      args: ["99d"]
      tty: true
      resources:
        requests:
          cpu: 250m
          memory: 1Gi
        limits:
          cpu: "1"
          memory: 2Gi
'''
    }
  }

  options {
    disableConcurrentBuilds()
  }

  environment {
    IMAGE_NAME = 'ghcr.io/su-yaa/pyspark-lab'
    GHCR_USERNAME = 'su-yaa'
  }

  stages {
    stage('Build and Push Image') {
      steps {
        container('kaniko') {
          withCredentials([string(credentialsId: 'GHCR', variable: 'GHCR_TOKEN')]) {
            sh '''
              set -eu
              test -f Dockerfile

              # Kaniko는 Docker daemon 없이 이미지를 빌드한다. k3s 노드의 containerd와
              # 충돌하지 않으면서 Jenkins agent pod 안에서 GHCR로 직접 push할 수 있다.
              mkdir -p /kaniko/.docker
              AUTH="$(printf '%s:%s' "${GHCR_USERNAME}" "${GHCR_TOKEN}" | /busybox/base64 | /busybox/tr -d '\\n')"
              cat > /kaniko/.docker/config.json <<EOF
{"auths":{"ghcr.io":{"auth":"${AUTH}"}}}
EOF

              # commit SHA 태그는 재현 가능한 배포용, main 태그는 Airflow 예제 DAG가
              # 바로 따라갈 수 있는 연습용 moving tag다.
              /kaniko/executor \
                --context "${WORKSPACE}" \
                --dockerfile "${WORKSPACE}/Dockerfile" \
                --destination "${IMAGE_NAME}:${GIT_COMMIT}" \
                --destination "${IMAGE_NAME}:main" \
                --cleanup
            '''
          }
        }
      }
    }
  }
}
