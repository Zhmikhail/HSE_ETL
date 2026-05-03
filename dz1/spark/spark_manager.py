"""
SparkManager — динамическое управление Spark-кластером через Docker.

Поднимает worker-контейнеры с заданными ресурсами при создании сессии,
убивает их при завершении. Работает из Airflow DAG и Jupyter одинаково.

Использование:
    from spark_manager import SparkManager

    with SparkManager(
        app_name="my_etl",
        num_executors=2,
        executor_memory="5g",
        executor_cores=5,
    ) as spark:
        df = spark.read.csv("/opt/spark_data/data.csv", header=True)
        df.show()
    # worker-контейнеры автоматически убиты
"""

import os
import time
import logging
import docker
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SparkManager:
    """Управление lifecycle Spark-кластера: workers поднимаются/умирают динамически."""

    def __init__(
        self,
        app_name: str,
        num_executors: int = 2,
        executor_memory: str = "2g",
        executor_cores: int = 2,
        master_url: str | None = None,
        network: str = "etl_net",
        spark_image: str = "etl-spark:latest",
        shared_volume: str = "dz1_spark_data",
        extra_spark_conf: dict | None = None,
        worker_startup_timeout: int = 30,
    ):
        self.app_name = app_name
        self.num_executors = num_executors
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.network = network
        self.spark_image = spark_image
        self.shared_volume = shared_volume
        self.extra_spark_conf = extra_spark_conf or {}
        self.worker_startup_timeout = worker_startup_timeout

        self.master_url = master_url or self._detect_master_url()

        self._docker: docker.DockerClient | None = None
        self._worker_containers: list = []
        self._spark: SparkSession | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> SparkSession:
        """Поднять worker-контейнеры и создать SparkSession."""
        logger.info(
            "SparkManager: запуск %d workers (%s mem, %d cores каждый)",
            self.num_executors,
            self.executor_memory,
            self.executor_cores,
        )

        client = self._get_docker()
        mem_limit = self._mem_with_overhead(self.executor_memory, overhead=0.2)

        for i in range(self.num_executors):
            name = f"spark-worker-{self.app_name}-{i}"
            self._remove_stale_container(client, name)

            # apache/spark image: запуск worker через spark-class (foreground)
            worker_cmd = (
                f"/opt/spark/bin/spark-class "
                f"org.apache.spark.deploy.worker.Worker "
                f"spark://spark-master:7077 "
                f"--memory {self.executor_memory} "
                f"--cores {self.executor_cores}"
            )

            container = client.containers.run(
                image=self.spark_image,
                command=worker_cmd,
                detach=True,
                name=name,
                hostname=name,
                network=self.network,
                volumes={
                    self.shared_volume: {"bind": "/opt/spark_data", "mode": "rw"},
                },
                mem_limit=mem_limit,
                nano_cpus=int(self.executor_cores * 1e9),
                labels={
                    "managed_by": "spark_manager",
                    "spark_app": self.app_name,
                },
            )
            self._worker_containers.append(container)
            logger.info("  worker %s запущен (id=%s)", name, container.short_id)

        self._wait_for_workers()

        self._spark = self._create_session()
        logger.info("SparkSession '%s' создана", self.app_name)
        return self._spark

    def stop(self):
        """Остановить SparkSession и убить worker-контейнеры."""
        if self._spark:
            try:
                self._spark.stop()
            except Exception as e:
                logger.warning("Ошибка при остановке SparkSession: %s", e)
            self._spark = None

        for container in self._worker_containers:
            try:
                container.stop(timeout=10)
                logger.info("worker %s остановлен", container.name)
            except Exception:
                pass
            try:
                container.remove(force=True)
            except Exception:
                pass

        self._worker_containers.clear()
        logger.info("SparkManager: все ресурсы освобождены")

    @property
    def spark(self) -> SparkSession | None:
        return self._spark

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> SparkSession:
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False

    # ------------------------------------------------------------------
    # Class-level cleanup
    # ------------------------------------------------------------------

    @classmethod
    def cleanup_all_workers(cls, app_name: str | None = None):
        """Удалить все осиротевшие spark-worker контейнеры.

        Если app_name задано — удалить только для данного приложения.
        """
        client = docker.from_env()
        filters = {"label": "managed_by=spark_manager"}
        if app_name:
            filters["label"] = [
                "managed_by=spark_manager",
                f"spark_app={app_name}",
            ]
        containers = client.containers.list(all=True, filters=filters)
        for c in containers:
            try:
                c.stop(timeout=5)
            except Exception:
                pass
            try:
                c.remove(force=True)
            except Exception:
                pass
        logger.info("cleanup: удалено %d контейнеров", len(containers))

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get_docker(self) -> docker.DockerClient:
        if self._docker is None:
            self._docker = docker.from_env()
        return self._docker

    def _detect_master_url(self) -> str:
        """Авто-определение URL мастера: внутри Docker или на хосте."""
        if os.path.exists("/.dockerenv"):
            return "spark://spark-master:7077"
        return "spark://localhost:7077"

    def _create_session(self) -> SparkSession:
        builder = (
            SparkSession.builder.appName(self.app_name)
            .master(self.master_url)
            .config("spark.executor.memory", self.executor_memory)
            .config("spark.executor.cores", str(self.executor_cores))
            .config(
                "spark.cores.max",
                str(self.num_executors * self.executor_cores),
            )
            .config("spark.driver.host", self._get_driver_host())
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.ui.enabled", "false")
        )

        for key, value in self.extra_spark_conf.items():
            builder = builder.config(key, value)

        return builder.getOrCreate()

    def _get_driver_host(self) -> str:
        """Hostname драйвера, видимый для worker-контейнеров."""
        if os.path.exists("/.dockerenv"):
            import socket
            return socket.gethostname()
        return "host.docker.internal"

    def _wait_for_workers(self):
        """Ждём, пока все worker-контейнеры перейдут в running."""
        client = self._get_docker()
        deadline = time.time() + self.worker_startup_timeout
        while time.time() < deadline:
            all_running = True
            for c in self._worker_containers:
                c.reload()
                if c.status != "running":
                    all_running = False
                    break
            if all_running:
                logger.info("Все workers в статусе running")
                # Даём Spark workers зарегистрироваться на мастере
                time.sleep(5)
                return
            time.sleep(2)
        raise TimeoutError(
            f"Workers не поднялись за {self.worker_startup_timeout} секунд"
        )

    @staticmethod
    def _remove_stale_container(client: docker.DockerClient, name: str):
        """Удалить контейнер с таким именем, если он уже существует."""
        try:
            old = client.containers.get(name)
            old.stop(timeout=5)
            old.remove(force=True)
        except docker.errors.NotFound:
            pass

    @staticmethod
    def _mem_with_overhead(mem_str: str, overhead: float = 0.2) -> str:
        """Добавить overhead к memory string для Docker mem_limit.

        '5g' -> '6g' (при overhead=0.2)
        """
        mem_str = mem_str.lower().strip()
        if mem_str.endswith("g"):
            val = float(mem_str[:-1])
            return f"{int(val * (1 + overhead) * 1024)}m"
        elif mem_str.endswith("m"):
            val = float(mem_str[:-1])
            return f"{int(val * (1 + overhead))}m"
        # fallback — считаем байты
        return str(int(float(mem_str) * (1 + overhead)))
