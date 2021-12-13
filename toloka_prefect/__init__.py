import attr
import inspect
import json
import logging
import pandas as pd
import pickle
import requests
import time
from datetime import datetime, timedelta
from functools import partial, wraps
from typing import Any, Callable, Dict, Iterable, List, Type, Union

import prefect
from prefect import task

import toloka.client as _toloka_client_lib  # To avoid `structure` pickling errors.
from toloka.client import Assignment, Pool, Project, Task, TolokaClient, Training
from toloka.client.analytics_request import CompletionPercentagePoolAnalytics
from toloka.client.batch_create_results import TaskBatchCreateResult


def _update_signature(
    func: Callable,
    wrapper: Callable,
    *,
    add_wrapper_args: Iterable[str] = (),
    remove_func_args: Iterable[str] = (),
) -> Callable:
    """Prefect tasks relate on function signature,
    so we need to change it since we use some args-changing wrappers.
    """

    remove_func_args = set(remove_func_args)

    signature_func = inspect.signature(func)
    parameters_keep = [
        param
        for param in signature_func.parameters.values()
        if param.name not in remove_func_args and param.kind != inspect.Parameter.VAR_KEYWORD
    ]

    parameters_wrapper = inspect.signature(wrapper).parameters
    parameters_add = [parameters_wrapper[arg_name] for arg_name in add_wrapper_args]

    last_param = next(reversed(signature_func.parameters.values()), inspect.Parameter)
    if last_param.kind == inspect.Parameter.VAR_KEYWORD:
        parameters_add.append(last_param)

    res = wraps(func)(wrapper)
    res.__signature__ = signature_func.replace(parameters=parameters_keep + parameters_add)

    return res


def _with_toloka_client(func: Callable) -> Callable:
    """Allow function to pass `token` and `env` args and operate with `toloka_client` instance."""

    def _wrapper(*args, token: str, env: str, **kwargs):
        toloka_client = TolokaClient(token, env)
        return partial(func, toloka_client=toloka_client)(*args, **kwargs)

    return _update_signature(func, _wrapper, add_wrapper_args=('token', 'env'), remove_func_args={'toloka_client'})


def with_logger(func: Callable) -> Callable:
    """Allow function to use Prefect logger instance.
    Enrich function signature with keyword-only argument "logger".
    Use it to write logs from Prefect task.
    """

    def _wrapper(*args, **kwargs):
        return partial(func, logger=prefect.context.get('logger'))(*args, **kwargs)

    return _update_signature(func, _wrapper, remove_func_args={'logger'})


def _task_with_self_credentials(func):
    """Allow to redirect assigned `self.token` and `self.env` tasks outputs into wrapped method."""

    func = _with_toloka_client(func)
    func = task(func)

    def _wrapper(self, *args, **kwargs):
        return partial(func, token=self.token, env=self.env)(*args, **kwargs)

    res = _update_signature(func, _wrapper, remove_func_args={'token', 'env'})
    return res


def _structure_from_conf(obj: Any, cl: Type) -> object:
    if isinstance(obj, cl):
        return obj
    if isinstance(obj, bytes):
        try:
            return pickle.loads(obj)
        except Exception:
            pass
        obj = obj.decode()
    if isinstance(obj, str):
        obj = json.loads(obj)
    return _toloka_client_lib.structure(obj, cl)


def _extract_id(obj: Any, cl: Type) -> str:
    if isinstance(obj, str):
        try:
            obj = json.loads(obj)
        except Exception:
            return obj
    return _structure_from_conf(obj, cl).id


def create_project(
    obj: Union[Project, Dict, str, bytes],
    *,
    toloka_client: TolokaClient,
) -> Project:
    """Create a Project object from given config.

    Args:
        obj: Either a `Project` object itself or a config to make a `Project`.

    Returns:
        Project object.
    """
    obj = _structure_from_conf(obj, Project)
    return toloka_client.create_project(obj)


def create_exam_pool(
    obj: Union[Training, Dict, str, bytes],
    *,
    project: Union[Project, str, None] = None,
    toloka_client: TolokaClient,
) -> Training:
    """Create a Training pool object from given config.

    Args:
        obj: Either a `Training` object itself or a config to make a `Training`.
        project: Project to assign a training pool to. May pass either an object, config or project_id.

    Returns:
        Training object.
    """
    obj = _structure_from_conf(obj, Training)
    if project is not None:
        obj.project_id = _extract_id(project, Project)
    return toloka_client.create_training(obj)


def create_pool(
    obj: Union[Pool, Dict, str, bytes],
    *,
    project: Union[Project, str, None] = None,
    exam_pool: Union[Training, str, None] = None,
    expiration: Union[datetime, timedelta, None] = None,
    toloka_client: TolokaClient,
) -> Pool:
    """Create a Pool object from given config.

    Args:
        obj: Either a `Pool` object itself or a config to make a `Pool`.
        project: Project to assign a pool to. May pass either an object, config or project_id.
        exam_pool: Related training pool. May pass either an object, config or pool_id.
        expiration: Expiration setting. May pass any of:
            * `None` if this setting if already present;
            * `datetime` object to set exact datetime;
            * `timedelta` to set expiration related to the current time.

    Returns:
        Pool object.
    """
    obj = _structure_from_conf(obj, Pool)
    if project is not None:
        obj.project_id = _extract_id(project, Project)
    if exam_pool:
        if obj.quality_control.training_requirement is None:
            raise ValueError('pool.quality_control.training_requirement should be set before exam_pool assignment')
        obj.quality_control.training_requirement.training_pool_id = _extract_id(exam_pool, Training)
    if expiration:
        obj.will_expire = datetime.now() + expiration if isinstance(expiration, timedelta) else expiration
    return toloka_client.create_pool(obj)


def create_tasks(
    tasks: List[Union[Task, Dict]],
    *,
    toloka_client: TolokaClient,
    pool: Union[Pool, Training, Dict, str, None] = None,
    **kwargs
) -> TaskBatchCreateResult:
    """Create a list of tasks for a given pool.

    Args:
        tasks: List of either a `Task` objects or a task conofigurations.
        pool: Allow to set tasks pool if it's not present in the tasks themselves.
            May be either a `Pool` or `Training` object or config or a pool_id value.
        **kwargs: Any other args presented in `toloka.client.task.CreateTasksParameters`.

    Returns:
        toloka.client.batch_create_results.TaskBatchCreateResult object.
    """
    tasks = [_structure_from_conf(task, Task) for task in tasks]
    if pool is not None:
        try:
            pool_id = _extract_id(pool, Pool)
        except Exception:
            pool_id = _extract_id(pool, Training)
        for task in tasks:
            task.pool_id = pool_id
    return toloka_client.create_tasks(tasks, **kwargs)


def open_pool(
    obj: Union[Pool, str],
    *,
    toloka_client: TolokaClient,
) -> Pool:
    """Open given pool.

    Args:
        obj: Pool id or `Pool` object of it's config.

    Returns:
        Pool object.
    """
    obj = _structure_from_conf(obj, Pool)
    return toloka_client.open_pool(obj)


def open_exam_pool(
    obj: Union[Training, str],
    *,
    toloka_client: TolokaClient,
) -> Pool:
    """Open given training pool.

    Args:
        obj: Training pool_id or `Training` object of it's config.

    Returns:
        Training object.
    """
    obj = _structure_from_conf(obj, Training)
    return toloka_client.open_training(obj)


@with_logger
def wait_pool(
    pool: Union[Pool, Dict, str],
    period: timedelta = timedelta(seconds=60),
    *,
    open_pool: bool = False,
    toloka_client: TolokaClient,
    logger: logging.Logger,
) -> Pool:
    """Wait given pool until close.

    Args:
        pool: Either a `Pool` object or it's config or a pool_id value.
        period: `timedelta` interval between checks. One minute by default.
        open_pool: Allow to open pool at start if it's closed. False by default.

    Returns:
        Pool object.
    """
    pool_id = _extract_id(pool, Pool)
    pool = toloka_client.get_pool(pool_id)
    if pool.is_closed() and open_pool:
        pool = toloka_client.open_pool(pool_id)

    while pool.is_open():
        op = toloka_client.get_analytics([CompletionPercentagePoolAnalytics(subject_id=pool_id)])
        percentage = toloka_client.wait_operation(op).details['value'][0]['result']['value']
        logger.info(f'Pool {pool_id} - {percentage}%')

        time.sleep(period.total_seconds())
        pool = toloka_client.get_pool(pool_id)

    return pool


def get_assignments(
    pool: Union[Pool, Dict, str],
    status: Union[str, List[str], Assignment.Status, List[Assignment.Status], None] = None,
    *,
    toloka_client: TolokaClient,
    **kwargs
) -> List[Assignment]:
    """Get all assignments of selected status from pool.

    Args:
        pool: Either a `Pool` object or it's config or a pool_id.
        status: A status or a list of statuses to get. All statuses (None) by default.
        **kwargs: Any other args presented in `toloka.client.search_requests.AssignmentSearchRequest`.

    Returns:
        List of `Assignment` objects.
    """
    pool_id = _extract_id(pool, Pool)
    return list(toloka_client.get_assignments(pool_id=pool_id, status=status, **kwargs))


def get_assignments_df(
    pool: Union[Pool, Dict, str],
    status: Union[str, List[str], Assignment.Status, List[Assignment.Status], None] = None,
    *,
    toloka_client: TolokaClient,
    **kwargs
) -> pd.DataFrame:
    """Get pool assignments of selected statuses in Pandas DataFrame format useful for aggregation.

    Args:
        pool: Either a `Pool` object or it's config or a pool_id.
        status: A status or a list of statuses to get. All statuses (None) by default.
        **kwargs: Any other args presented in `toloka.client.assignment.GetAssignmentsTsvParameters`.

    Returns:
        DataFrame with selected assignments. Note that nested paths are presented with a ":" sign.
    """
    pool_id = _extract_id(pool, Pool)
    if not status:
        status = []
    elif isinstance(status, (str, Assignment.Status)):
        status = [status]
    return toloka_client.get_assignments_df(pool_id=pool_id, status=status, **kwargs)


@attr.s
class TolokaAuth:
    """Contains Toloka tasks for Prefect that need authentication.
    Redirect passed `token` and `env` into all it's methods.

    Attributes:
        token: Output connection of a task with Toloka OAuth token output. Doesn't store any data.
        env: Output connection of a task with Toloka environment name.

    Example:
        >>> token = PrefectSecret('TOLOKA_TOKEN')
        >>> env = Parameter('env', default='PRODUCTION')
        >>> with TolokaAuth(token, env) as tlk:  # Note that we don't mention these credentials below.
        ...     project = tlk.create_project(some_project_conf)
        ...     pool = tlk.create_pool(some_pool_conf, project=project, expiration=timedelta(days=1))
        ...     _tasks_creation = tlk.create_tasks(some_tasks_data, pool=pool, open_pool=True)
        ...     _waiting = tlk.wait_pool(pool, upstream_tasks=[_tasks_creation])  # Setting upstream means we wait for it.
        ...     assignments = tlk.get_assignments(pool, upstream_tasks=[_waiting])
        ...
        >>> my_task_to_save_results_to_s3(assignments)
        ...
    """

    token: prefect.Task = attr.ib()
    env: prefect.Task = attr.ib()

    def __enter__(self) -> 'TolokaAuth':
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        pass

    create_project = _task_with_self_credentials(create_project)
    create_exam_pool = _task_with_self_credentials(create_exam_pool)
    create_pool = _task_with_self_credentials(create_pool)
    create_tasks = _task_with_self_credentials(create_tasks)
    open_pool = _task_with_self_credentials(open_pool)
    open_exam_pool = _task_with_self_credentials(open_exam_pool)
    wait_pool = _task_with_self_credentials(wait_pool)
    get_assignments = _task_with_self_credentials(get_assignments)
    get_assignments_df = _task_with_self_credentials(get_assignments_df)


@task
def download_string(url: str) -> str:
    """Download text content stored at given url."""
    response = requests.get(url)
    response.raise_for_status()
    return response.text


@task
def download_data(url: str) -> bytes:
    """Download bytes data stored at given url."""
    response = requests.get(url)
    response.raise_for_status()
    return response.content


@task
def download_json(url: str) -> Any:
    """Download and parse json config stored at given url."""
    response = requests.get(url)
    response.raise_for_status()
    return response.json()
