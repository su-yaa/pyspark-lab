from __future__ import annotations

from dataclasses import dataclass, field

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException


def build_core_api() -> client.CoreV1Api:
    """Airflow pod의 ServiceAccount 권한으로 Kubernetes core API client를 만든다."""

    config.load_incluster_config()
    return client.CoreV1Api()


@dataclass
class PodLogRelay:
    """Kubernetes pod 로그를 Airflow task 로그로 중계한다.

    Airflow task가 외부 pod를 제출하고 기다리는 구조에서는 실제 실행 로그가
    제출 대상 pod에 남는다. 이 helper는 해당 pod 로그를 읽어 Airflow 로그에
    다시 출력해, Airflow UI 한 화면에서 orchestration과 실행 로그를 같이 볼
    수 있게 한다.
    """

    namespace: str
    container: str | None = None
    prefix: str = "[pod]"
    tail_lines: int = 200
    _emitted_lines: set[str] = field(default_factory=set, init=False)
    _last_waiting_error: str | None = field(default=None, init=False)

    def reset(self) -> None:
        """새 pod 로그를 읽기 시작할 때 기존 중복 방지 상태를 초기화한다."""

        self._emitted_lines.clear()

    def relay(self, api: client.CoreV1Api, pod_name: str) -> None:
        """pod 로그 중 아직 Airflow에 출력하지 않은 줄만 중계한다."""

        raw_logs = self._read_logs(api=api, pod_name=pod_name)
        if raw_logs is None:
            return

        for line in raw_logs.splitlines():
            if not line or line in self._emitted_lines:
                continue
            self._emitted_lines.add(line)
            print(f"{self.prefix} {line}", flush=True)

    def _read_logs(self, api: client.CoreV1Api, pod_name: str) -> str | None:
        try:
            return api.read_namespaced_pod_log(
                name=pod_name,
                namespace=self.namespace,
                container=self.container,
                timestamps=True,
                tail_lines=self.tail_lines,
            )
        except ApiException as exc:
            if exc.status == 404:
                # pod가 아직 생성 중이면 다음 polling에서 다시 시도한다.
                return None
            if exc.status == 400 and self.container:
                if _is_container_waiting_error(exc):
                    self._print_waiting_error_once(exc)
                    return None
                # 이미지나 operator 버전에 따라 container 이름이 다를 수 있어 pod 기본 로그로 재시도한다.
                try:
                    return api.read_namespaced_pod_log(
                        name=pod_name,
                        namespace=self.namespace,
                        timestamps=True,
                        tail_lines=self.tail_lines,
                    )
                except ApiException as retry_exc:
                    if retry_exc.status == 400 and _is_container_waiting_error(retry_exc):
                        self._print_waiting_error_once(retry_exc)
                        return None
                    raise
            raise

    def _print_waiting_error_once(self, exc: ApiException) -> None:
        message = str(exc.body or exc.reason or "container is waiting to start")
        if message == self._last_waiting_error:
            return
        self._last_waiting_error = message
        print(f"{self.prefix} 컨테이너가 아직 시작되지 않아 로그를 읽지 않습니다 | {message}", flush=True)


def _is_container_waiting_error(exc: ApiException) -> bool:
    body = str(exc.body or exc.reason or "")
    return "waiting to start" in body or "image can't be pulled" in body
