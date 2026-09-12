import math
import os
import re
import time
import logging
from typing import Any, Dict, List, Optional

import docker
import requests


# ============================================================
# Configuration
# ============================================================

FLINK_API = os.getenv("FLINK_API", "http://jobmanager:8081")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")

TASKMANAGER_SERVICE = os.getenv("TASKMANAGER_SERVICE", "taskmanager")
TASKMANAGER_SLOTS = int(os.getenv("TASKMANAGER_SLOTS", "8"))

MIN_TASKMANAGERS = int(os.getenv("MIN_TASKMANAGERS", "1"))
MAX_TASKMANAGERS = int(os.getenv("MAX_TASKMANAGERS", "10"))

MIN_PARALLELISM = int(os.getenv("MIN_PARALLELISM", "1"))
MAX_PARALLELISM = int(os.getenv("MAX_PARALLELISM", "8"))

SCALE_UP_BUSY = float(os.getenv("SCALE_UP_BUSY", "30"))
SCALE_DOWN_BUSY = float(os.getenv("SCALE_DOWN_BUSY", "5"))

SCALE_UP_STABLE = int(os.getenv("SCALE_UP_STABLE", "3"))
SCALE_DOWN_STABLE = int(os.getenv("SCALE_DOWN_STABLE", "10"))

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "300"))

VERIFY_ATTEMPTS = int(os.getenv("VERIFY_ATTEMPTS", "10"))
VERIFY_INTERVAL = int(os.getenv("VERIFY_INTERVAL", "5"))

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger("autoscaler")


# ============================================================
# Docker
# ============================================================

docker_client = docker.from_env()


# ============================================================
# Flink REST API helpers
# ============================================================

def flink_get(path: str) -> Optional[Dict[str, Any]]:
    try:
        response = requests.get(
            f"{FLINK_API}{path}",
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    except Exception as exc:
        logger.error(
            "Flink GET failed: %s: %s",
            path,
            exc,
        )
        return None


def flink_rescale(
    job_id: str,
    parallelism: int,
) -> bool:
    try:
        job = flink_get(f"/jobs/{job_id}")

        if not job:
            logger.error(
                "Flink rescale failed: unable to get job: %s",
                job_id,
            )
            return False

        vertices = job.get("vertices", [])

        if not vertices:
            logger.error(
                "Flink rescale failed: no vertices found for job=%s",
                job_id,
            )
            return False

        resource_requirements = {}

        for vertex in vertices:
            vertex_id = vertex.get("id")

            if not vertex_id:
                logger.error(
                    "Flink rescale failed: vertex without id "
                    "for job=%s",
                    job_id,
                )
                return False

            resource_requirements[vertex_id] = {
                "parallelism": {
                    "lowerBound": parallelism,
                    "upperBound": parallelism,
                }
            }

        response = requests.put(
            f"{FLINK_API}/jobs/{job_id}/resource-requirements",
            json=resource_requirements,
            timeout=30,
        )
        response.raise_for_status()

        logger.info(
            "Flink resource requirements updated: "
            "job=%s parallelism=%s vertices=%s",
            job_id,
            parallelism,
            len(resource_requirements),
        )

        return True

    except Exception as exc:
        logger.error(
            "Flink resource requirements update failed: "
            "job=%s parallelism=%s: %s",
            job_id,
            parallelism,
            exc,
        )
        return False      

# ============================================================
# Prometheus
# ============================================================

def prometheus_query(query: str) -> Optional[Dict[str, Any]]:
    try:
        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    except Exception as exc:
        logger.error(
            "Prometheus query failed: %s: %s",
            query,
            exc,
        )
        return None


def get_normalizer_jobs(
    jobs: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    if jobs is None:
        jobs = get_jobs()

    normalizer_jobs: Dict[str, Dict[str, Any]] = {}

    for job in jobs:
        name = str(job.get("name", ""))
        normalized_name = name.lower().replace("_", "-")

        if not normalized_name.startswith("normalizer-"):
            continue

        if str(job.get("state", "")).upper() != "RUNNING":
            continue

        normalizer_jobs[normalized_name] = job

    return normalizer_jobs


def get_normalizer_busy() -> Dict[str, float]:
    query = """
        max by (job_name) (
            flink_taskmanager_job_task_busyTimeMsPerSecond{
                job_name=~"normalizer_.*"
            }
        )
    """

    data = prometheus_query(query)

    if not data:
        return {}

    busy_by_job: Dict[str, float] = {}

    try:
        result = data.get("data", {}).get("result", [])

        for item in result:
            metric = item.get("metric", {})
            job_name = str(metric.get("job_name", "")).lower().replace("_", "-")
            value = item.get("value")

            if not job_name or not value or len(value) < 2:
                continue

            if job_name in get_normalizer_jobs():
                busy_by_job[job_name] = float(value[1])

        return busy_by_job

    except Exception as exc:
        logger.error(
            "Could not parse Prometheus busy metrics: %s",
            exc,
        )
        return {}

def get_normalizer_throughput() -> Dict[str, Dict[str, float]]:
    query = """
        max by (job_name) (
            flink_taskmanager_job_task_numRecordsInPerSecond{
                job_name=~"normalizer_.*"
            }
        )
        or
        max by (job_name) (
            flink_taskmanager_job_task_numRecordsOutPerSecond{
                job_name=~"normalizer_.*"
            }
        )
    """

    data = prometheus_query(query)

    if not data:
        return {}

    throughput: Dict[str, Dict[str, float]] = {}

    try:
        result = data.get("data", {}).get("result", [])

        for item in result:
            metric = item.get("metric", {})
            job_name = str(metric.get("job_name", "")).lower().replace("_", "-")
            value = item.get("value")

            if not job_name or not value or len(value) < 2:
                continue

            if job_name not in get_normalizer_jobs():
                continue

            metric_name = str(metric.get("__name__", ""))

            if metric_name == "flink_taskmanager_job_task_numRecordsInPerSecond":
                throughput.setdefault(job_name, {})["records_in"] = float(value[1])

            elif metric_name == "flink_taskmanager_job_task_numRecordsOutPerSecond":
                throughput.setdefault(job_name, {})["records_out"] = float(value[1])

        return throughput

    except Exception as exc:
        logger.error(
            "Could not parse Prometheus throughput metrics: %s",
            exc,
        )
        return {}

def get_jobs() -> List[Dict[str, Any]]:
    data = flink_get("/jobs/overview")

    if not data:
        return []

    return data.get("jobs", [])


def find_job(
    job_name: str,
    jobs: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:

    if jobs is None:
        jobs = get_jobs()

    normalized_target = job_name.lower().replace("_", "-")

    for job in jobs:
        name = str(job.get("name", ""))
        normalized_name = name.lower().replace("_", "-")

        if normalized_name == normalized_target:
            return job

    return None


def get_job_id(
    job_name: str,
    jobs: Optional[List[Dict[str, Any]]] = None,
) -> Optional[str]:

    job = find_job(
        job_name,
        jobs,
    )

    if not job:
        return None

    jid = job.get("jid")

    if not jid:
        return None

    return str(jid)


# ============================================================
# Job Parallelism
# ============================================================

def get_job_parallelism(
    job_id: str,
) -> Optional[int]:

    # IMPORTANT:
    # Do not use /jobs/{job_id}/vertices here.
    #
    # In the current Flink REST API/environment that endpoint
    # returns 404.
    #
    # Instead, get the job details from /jobs/{job_id}.
    data = flink_get(f"/jobs/{job_id}")

    if not data:
        return None

    vertices = data.get("vertices", [])

    if not vertices:
        return None

    parallelisms = []

    for vertex in vertices:
        parallelism = vertex.get("parallelism")

        if parallelism is None:
            continue

        try:
            parallelism = int(parallelism)
        except (TypeError, ValueError):
            continue

        if parallelism > 0:
            parallelisms.append(parallelism)

    if not parallelisms:
        return None

    return max(parallelisms)


def get_normalizer_parallelisms() -> Dict[str, int]:
    normalizer_jobs = get_normalizer_jobs()
    parallelisms: Dict[str, int] = {}

    for normalized_name, job in normalizer_jobs.items():
        jid = job.get("jid")

        if not jid:
            continue

        parallelism = get_job_parallelism(str(jid))

        if parallelism is not None:
            parallelisms[normalized_name] = parallelism

    return parallelisms

def get_taskmanager_containers() -> List[Any]:
    try:
        return docker_client.containers.list(
            all=True,
            filters={
                "label": "com.docker.compose.service=taskmanager"
            },
        )

    except Exception as exc:
        logger.error(
            "Could not get TaskManager containers: %s",
            exc,
        )
        return []


def get_taskmanager_count() -> int:
    return len(get_taskmanager_containers())


def get_taskmanager_slots() -> Dict[str, Any]:

    data = flink_get("/taskmanagers")

    if not data:
        return {
            "total_slots": 0,
            "free_slots": 0,
            "used_slots": 0,
            "taskmanagers": [],
        }

    taskmanagers = data.get("taskmanagers", [])

    total_slots = 0
    free_slots = 0

    for tm in taskmanagers:
        try:
            slots = int(tm.get("slotsNumber", 0))
            free = int(tm.get("freeSlots", 0))
        except (TypeError, ValueError):
            continue

        total_slots += slots
        free_slots += free

    used_slots = total_slots - free_slots

    return {
        "total_slots": total_slots,
        "free_slots": free_slots,
        "used_slots": used_slots,
        "taskmanagers": taskmanagers,
    }


def get_empty_taskmanagers() -> List[Dict[str, Any]]:

    data = flink_get("/taskmanagers")

    if not data:
        return []

    taskmanagers = data.get("taskmanagers", [])

    empty = []

    for tm in taskmanagers:

        try:
            slots_number = int(tm.get("slotsNumber", 0))
            free_slots = int(tm.get("freeSlots", 0))
        except (TypeError, ValueError):
            continue

        used_slots = slots_number - free_slots

        # A TaskManager is removable ONLY when its own used
        # slot count is exactly zero.
        if slots_number > 0 and used_slots == 0:
            empty.append(tm)

    return empty


# ============================================================
# Required capacity
# ============================================================

def get_required_slots(parallelisms: Dict[str, int]) -> int:
    normalizer_slots = sum(
        max(0, int(parallelism))
        for parallelism in parallelisms.values()
    )

    return normalizer_slots + 2

def get_required_taskmanagers(parallelisms: Dict[str, int]) -> int:
    required_slots = get_required_slots(parallelisms)

    required_taskmanagers = math.ceil(
        required_slots / TASKMANAGER_SLOTS
    )

    return max(
        MIN_TASKMANAGERS,
        min(
            required_taskmanagers,
            MAX_TASKMANAGERS,
        ),
    )

def get_reference_taskmanager() -> Optional[Any]:

    containers = get_taskmanager_containers()

    running = [
        container
        for container in containers
        if container.status == "running"
    ]

    if running:
        return running[0]

    if containers:
        return containers[0]

    return None


def get_next_taskmanager_number() -> int:
    containers = get_taskmanager_containers()
    numbers = []

    pattern = re.compile(
        r"data-collector-taskmanager-(\d+)$"
    )

    for container in containers:
        name = container.name
        match = pattern.search(name)

        if match:
            try:
                numbers.append(int(match.group(1)))
            except ValueError:
                pass

    if not numbers:
        return 1

    return max(numbers) + 1
    

def create_taskmanager() -> bool:
    reference = get_reference_taskmanager()

    if reference is None:
        logger.error(
            "Cannot create TaskManager: no existing TaskManager found."
        )
        return False

    try:
        reference.reload()

        attrs = reference.attrs
        config = attrs.get("Config", {})
        host_config = attrs.get("HostConfig", {})

        image = config.get("Image")

        if not image:
            logger.error(
                "Cannot create TaskManager: reference image not found."
            )
            return False

        name = (
            f"data-collector-{TASKMANAGER_SERVICE}-"
            f"{get_next_taskmanager_number()}"
        )

        environment = config.get("Env", [])
        command = config.get("Cmd")
        entrypoint = config.get("Entrypoint")
        volumes = config.get("Volumes")

        port_bindings = host_config.get("PortBindings") or {}

        ports = {}

        for container_port, bindings in port_bindings.items():
            if bindings:
                binding = bindings[0]

                host_ip = binding.get("HostIp", "")
                host_port = binding.get("HostPort", "")

                if host_port:
                    if host_ip:
                        ports[container_port] = (
                            host_ip,
                            int(host_port),
                        )
                    else:
                        ports[container_port] = int(host_port)
                else:
                    ports[container_port] = None
            else:
                ports[container_port] = None

        restart_policy = host_config.get(
            "RestartPolicy",
            {},
        )

        network_mode = host_config.get(
            "NetworkMode"
        )

        container = docker_client.containers.create(
            image=image,
            name=name,
            environment=environment,
            command=command,
            entrypoint=entrypoint,
            volumes=volumes,
            ports=ports,
            restart_policy=restart_policy,
            network_mode=network_mode,
            labels={
                "com.docker.compose.service": TASKMANAGER_SERVICE,
            },
        )

        container.start()

        logger.info(
            "Created new TaskManager from existing TaskManager: %s",
            name,
        )

        return True

    except Exception as exc:
        logger.error(
            "Failed to create TaskManager: %s",
            exc,
        )
        return False

def remove_taskmanager() -> bool:

    empty_taskmanagers = get_empty_taskmanagers()

    if not empty_taskmanagers:
        logger.warning(
            "SAFE SCALE_DOWN: no completely empty "
            "TaskManager found. Skipping removal."
        )
        return False

    target_tm = empty_taskmanagers[-1]

    target_id = target_tm.get("id", "")

    logger.info(
        "SAFE SCALE_DOWN: selected empty Flink TaskManager: %s",
        target_id,
    )

    containers = get_taskmanager_containers()

    if not containers:
        logger.warning(
            "SAFE SCALE_DOWN: no Docker TaskManager containers found."
        )
        return False

    # Try to match the Flink TaskManager IP with the Docker
    # container IP.
    tm_path = str(target_tm.get("path", ""))

    ip_match = re.search(
        r"(\d+\.\d+\.\d+\.\d+)",
        tm_path,
    )

    target_ip = ip_match.group(1) if ip_match else None

    target_container = None

    if target_ip:

        for container in containers:

            if container.status != "running":
                continue

            try:
                container.reload()

                networks = (
                    container.attrs
                    .get("NetworkSettings", {})
                    .get("Networks", {})
                )

                for network in networks.values():

                    ip_address = network.get(
                        "IPAddress"
                    )

                    if ip_address == target_ip:
                        target_container = container
                        break

                if target_container:
                    break

            except Exception:
                continue

    if target_container is None:

        logger.warning(
            "SAFE SCALE_DOWN: could not match empty "
            "Flink TaskManager to Docker container."
        )

        return False

    # Re-check the Flink TaskManager list immediately before
    # removal. The selected TaskManager must still have zero
    # used slots.
    empty_taskmanagers = get_empty_taskmanagers()

    still_empty = False

    for tm in empty_taskmanagers:
        if tm.get("id") == target_id:
            still_empty = True
            break

    if not still_empty:
        logger.warning(
            "SAFE SCALE_DOWN: selected TaskManager is no "
            "longer completely empty. Skipping removal."
        )
        return False

    try:
        logger.info(
            "Removing empty TaskManager container: %s",
            target_container.name,
        )

        target_container.remove(
            force=True
        )

        return True

    except Exception as exc:

        logger.error(
            "Failed to remove TaskManager %s: %s",
            target_container.name,
            exc,
        )

        return False


def scale_taskmanagers(target: int) -> bool:

    current = get_taskmanager_count()

    if current == target:
        return True

    if current < target:

        while current < target:

            logger.info(
                "SCALE UP: TaskManagers=%s->%s",
                current,
                target,
            )

            created = create_taskmanager()

            if not created:
                return False

            current += 1

        return True

    while current > target:

        logger.info(
            "SCALE DOWN: TaskManagers=%s->%s",
            current,
            target,
        )

        removed = remove_taskmanager()

        if not removed:

            logger.warning(
                "SAFE SCALE_DOWN: TaskManager count remains %s; "
                "target=%s",
                current,
                target,
            )

            return False

        current -= 1

    return True


# ============================================================
# Verification
# ============================================================

def verify_parallelism(
    job_name: str,
    expected: int,
) -> bool:
    for attempt in range(
        1,
        VERIFY_ATTEMPTS + 1,
    ):
        current = get_normalizer_parallelisms().get(job_name)

        if current == expected:
            logger.info(
                "VERIFY PARALLELISM: %s=%s confirmed",
                job_name,
                expected,
            )
            return True

        logger.info(
            "VERIFY PARALLELISM: job=%s expected=%s current=%s "
            "attempt=%s/%s",
            job_name,
            expected,
            current,
            attempt,
            VERIFY_ATTEMPTS,
        )

        time.sleep(VERIFY_INTERVAL)

    logger.error(
        "VERIFY PARALLELISM FAILED: job=%s expected=%s",
        job_name,
        expected,
    )
    return False

def verify_free_slots(
    required_slots: int,
) -> bool:

    for attempt in range(
        1,
        VERIFY_ATTEMPTS + 1,
    ):

        slots = get_taskmanager_slots()

        used = slots["used_slots"]
        free = slots["free_slots"]
        total = slots["total_slots"]

        logger.info(
            "VERIFY FREE SLOTS: used=%s required=%s total=%s",
            used,
            required_slots,
            total,
        )

        if total >= required_slots and free >= 0:
            return True

        time.sleep(VERIFY_INTERVAL)

    return False


# ============================================================
# Scale UP
# ============================================================

def scale_up(
    job_name: str,
    target_parallelism: int,
    current_parallelisms: Dict[str, int],
) -> bool:
    target_parallelisms = dict(current_parallelisms)
    target_parallelisms[job_name] = target_parallelism

    target_tms = get_required_taskmanagers(
        target_parallelisms
    )

    current_tms = get_taskmanager_count()

    logger.info(
        "SCALE UP: job=%s parallelism=%s->%s, "
        "TaskManagers=%s->%s",
        job_name,
        current_parallelisms.get(job_name),
        target_parallelism,
        current_tms,
        target_tms,
    )

    if current_tms < target_tms:
        if not scale_taskmanagers(target_tms):
            return False
        time.sleep(5)

    job = find_job(
        job_name,
        get_jobs(),
    )

    if not job:
        logger.warning(
            "SCALE UP: job not found: %s",
            job_name,
        )
        return False

    jid = job.get("jid")
    if not jid:
        return False

    logger.info(
        "SCALE UP: rescaling %s to parallelism=%s",
        job_name,
        target_parallelism,
    )

    if not flink_rescale(
        str(jid),
        target_parallelism,
    ):
        logger.error(
            "SCALE UP: failed to rescale %s",
            job_name,
        )
        return False

    return verify_parallelism(
        job_name,
        target_parallelism,
    )

def scale_down(
    job_name: str,
    target_parallelism: int,
    current_parallelisms: Dict[str, int],
) -> bool:
    target_parallelisms = dict(current_parallelisms)
    target_parallelisms[job_name] = target_parallelism

    target_tms = get_required_taskmanagers(
        target_parallelisms
    )

    current_tms = get_taskmanager_count()
    current_parallelism = current_parallelisms.get(job_name)

    logger.info(
        "SCALE DOWN: job=%s parallelism=%s->%s, "
        "TaskManagers=%s->%s",
        job_name,
        current_parallelism,
        target_parallelism,
        current_tms,
        target_tms,
    )

    # First reduce only the selected Normalizer.
    if (
        current_parallelism is not None
        and current_parallelism > target_parallelism
    ):
        job = find_job(
            job_name,
            get_jobs(),
        )

        if not job:
            logger.warning(
                "SCALE DOWN: job not found: %s",
                job_name,
            )
            return False

        jid = job.get("jid")
        if not jid:
            return False

        logger.info(
            "SCALE DOWN: rescaling %s to parallelism=%s",
            job_name,
            target_parallelism,
        )

        if not flink_rescale(
            str(jid),
            target_parallelism,
        ):
            logger.error(
                "SCALE DOWN: failed to rescale %s",
                job_name,
            )
            return False

        if not verify_parallelism(
            job_name,
            target_parallelism,
        ):
            return False

    # Remove TaskManagers only when an individual TM is empty.
    current_tms = get_taskmanager_count()

    if current_tms <= target_tms:
        logger.info(
            "SAFE SCALE_DOWN: no TaskManager removal required."
        )
        return True

    removed_any = False

    while current_tms > target_tms:
        empty_taskmanagers = get_empty_taskmanagers()

        if not empty_taskmanagers:
            logger.warning(
                "SAFE SCALE_DOWN: no completely empty "
                "TaskManager available. Skipping removal."
            )
            break

        removed = remove_taskmanager()

        if not removed:
            logger.warning(
                "SAFE SCALE_DOWN: TaskManager removal failed."
            )
            break

        removed_any = True
        current_tms -= 1

    if current_tms != target_tms:
        logger.warning(
            "SAFE SCALE_DOWN: TaskManager count remains %s; "
            "target=%s",
            current_tms,
            target_tms,
        )

    if (
        current_parallelism == target_parallelism
        and not removed_any
        and current_tms > target_tms
    ):
        logger.warning(
            "SAFE SCALE_DOWN: parallelism was already at target "
            "but TaskManager removal was skipped because no "
            "completely empty TaskManager was available."
        )
        return False

    return True
    
def main():

    up_cycles: Dict[str, int] = {}
    down_cycles: Dict[str, int] = {}
    last_scale_time: Dict[str, float] = {}

    logger.info(
        "Autoscaler started."
    )

    logger.info(
        "Configuration: "
        "MIN_P=%s MAX_P=%s "
        "MIN_TM=%s MAX_TM=%s "
        "UP_BUSY=%s DOWN_BUSY=%s",
        MIN_PARALLELISM,
        MAX_PARALLELISM,
        MIN_TASKMANAGERS,
        MAX_TASKMANAGERS,
        SCALE_UP_BUSY,
        SCALE_DOWN_BUSY,
    )

    while True:
        try:
            busy_by_job = get_normalizer_busy()
            throughput_by_job = get_normalizer_throughput()

            if not busy_by_job:
                logger.warning(
                    "Normalizer busy time unavailable; continuing with throughput metrics."
                )

            current_parallelisms = get_normalizer_parallelisms()

            if not current_parallelisms:
                logger.warning(
                    "Could not determine current normalizer parallelism."
                )
                time.sleep(POLL_INTERVAL)
                continue

            current_tms = get_taskmanager_count()
            normalizer_jobs = get_normalizer_jobs()

            for job_name in sorted(normalizer_jobs):
                up_cycles.setdefault(job_name, 0)
                down_cycles.setdefault(job_name, 0)
                last_scale_time.setdefault(job_name, 0.0)

                if job_name not in current_parallelisms:
                    continue

                busy = busy_by_job.get(job_name)

                current_p = current_parallelisms[job_name]
                now = time.time()

                in_cooldown = (
                    now - last_scale_time[job_name]
                    < COOLDOWN_SECONDS
                )

                records_in = throughput_by_job.get(
                    job_name, {}
                ).get("records_in")

                records_out = throughput_by_job.get(
                    job_name, {}
                ).get("records_out")

                busy_up = (
                    busy is not None
                    and busy >= SCALE_UP_BUSY
                )

                throughput_backlog = (
                    records_in is not None
                    and records_out is not None
                    and records_in > records_out
                )

                # BusyTime OR sustained Input > Output can trigger
                # scale-up. They are independent signals.
                if busy_up or throughput_backlog:
                    up_cycles[job_name] = min(
                        up_cycles[job_name] + 1,
                        SCALE_UP_STABLE,
                    )
                    down_cycles[job_name] = 0

                elif (
                    busy is not None
                    and busy <= SCALE_DOWN_BUSY
                ):
                    up_cycles[job_name] = 0
                    down_cycles[job_name] = min(
                        down_cycles[job_name] + 1,
                        SCALE_DOWN_STABLE,
                    )

                else:
                    up_cycles[job_name] = 0
                    down_cycles[job_name] = 0

                action = "HOLD"
                target_p = current_p
                target_tms = current_tms

                # ------------------------------------------------
                # SCALE UP: only this Normalizer.
                # ------------------------------------------------
                if (
                    not in_cooldown
                    and up_cycles[job_name] >= SCALE_UP_STABLE
                    and current_p < MAX_PARALLELISM
                ):
                    action = "SCALE_UP"
                    target_p = min(
                        current_p + 1,
                        MAX_PARALLELISM,
                    )

                    target_parallelisms = dict(
                        current_parallelisms
                    )
                    target_parallelisms[job_name] = target_p

                    target_tms = get_required_taskmanagers(
                        target_parallelisms
                    )

                # ------------------------------------------------
                # SCALE DOWN: only this Normalizer.
                # ------------------------------------------------
                elif (
                    not in_cooldown
                    and down_cycles[job_name] >= SCALE_DOWN_STABLE
                    and busy is not None
                    and busy <= SCALE_DOWN_BUSY
                    and (
                        records_in is None
                        or records_out is None
                        or records_out >= records_in
                    )
                ):
                    if current_p > MIN_PARALLELISM:
                        action = "SCALE_DOWN"
                        target_p = max(
                            current_p - 1,
                            MIN_PARALLELISM,
                        )

                        target_parallelisms = dict(
                            current_parallelisms
                        )
                        target_parallelisms[job_name] = target_p

                        target_tms = get_required_taskmanagers(
                            target_parallelisms
                        )

                    elif current_tms > MIN_TASKMANAGERS:
                        # At minimum P, only remove a TM if one is
                        # completely empty.
                        if get_empty_taskmanagers():
                            action = "SCALE_DOWN"
                            target_p = current_p
                            target_tms = max(
                                MIN_TASKMANAGERS,
                                current_tms - 1,
                            )
                        else:
                            action = "HOLD"
                            down_cycles[job_name] = 0

                    else:
                        action = "HOLD"
                        down_cycles[job_name] = 0

                logger.info(
                    "%s: busy=%.2fms/s records_in=%s records_out=%s "
                    "busy_up=%s throughput_backlog=%s "
                    "parallelism=%s TaskManagers=%s desired_TMs=%s "
                    "up_cycles=%s/%s down_cycles=%s/%s "
                    "action=%s target_p=%s target_TMs=%s",
                    job_name,
                    busy if busy is not None else float("nan"),
                    (
                        f"{records_in:.2f}"
                        if records_in is not None
                        else "N/A"
                    ),
                    (
                        f"{records_out:.2f}"
                        if records_out is not None
                        else "N/A"
                    ),
                    busy_up,
                    throughput_backlog,
                    current_p,
                    current_tms,
                    target_tms,
                    up_cycles[job_name],
                    SCALE_UP_STABLE,
                    down_cycles[job_name],
                    SCALE_DOWN_STABLE,
                    action,
                    target_p,
                    target_tms,
                )

                if action == "HOLD":
                    continue

                if DRY_RUN:
                    logger.info(
                        "DRY_RUN: would execute %s for %s "
                        "P %s->%s TM %s->%s",
                        action,
                        job_name,
                        current_p,
                        target_p,
                        current_tms,
                        target_tms,
                    )
                    continue

                # Re-check only the selected job before scaling.
                latest_parallelisms = get_normalizer_parallelisms()

                if not latest_parallelisms:
                    logger.warning(
                        "Could not re-check current normalizer "
                        "parallelisms before scaling."
                    )
                    continue

                latest_p = latest_parallelisms.get(job_name)

                if latest_p is None:
                    logger.warning(
                        "Could not determine current parallelism "
                        "for %s before scaling.",
                        job_name,
                    )
                    continue

                if latest_p != current_p:
                    logger.info(
                        "%s: parallelism changed from %s to %s "
                        "before scaling; skipping this cycle.",
                        job_name,
                        current_p,
                        latest_p,
                    )
                    continue

                if action == "SCALE_UP":
                    logger.info(
                        "STARTING REAL SCALE_UP: job=%s "
                        "P %s->%s, TM %s->%s",
                        job_name,
                        current_p,
                        target_p,
                        current_tms,
                        target_tms,
                    )

                    success = scale_up(
                        job_name,
                        target_p,
                        latest_parallelisms,
                    )

                else:
                    logger.info(
                        "STARTING REAL SCALE_DOWN: job=%s "
                        "P %s->%s, TM %s->%s",
                        job_name,
                        current_p,
                        target_p,
                        current_tms,
                        target_tms,
                    )

                    success = scale_down(
                        job_name,
                        target_p,
                        latest_parallelisms,
                    )

                if success:
                    logger.info(
                        "%s: SCALING COMPLETE. Cooldown=%ss",
                        job_name,
                        COOLDOWN_SECONDS,
                    )
                    last_scale_time[job_name] = time.time()
                else:
                    logger.warning(
                        "%s: SCALING FAILED or PARTIALLY COMPLETED.",
                        job_name,
                    )

                up_cycles[job_name] = 0
                down_cycles[job_name] = 0

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            logger.info(
                "Autoscaler stopped."
            )
            break

        except Exception as exc:
            logger.exception(
                "Unexpected autoscaler error: %s",
                exc,
            )
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
