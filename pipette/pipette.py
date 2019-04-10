import atexit
import base64
import importlib
import io
import itertools
import logging
import mmh3
import random
import re
import string
import tempfile
import zlib
from typing import *
from typing import BinaryIO     # Not sure why this is necessary.
import os

import dill
import typeguard

_logger = logging.getLogger(__name__)

T = TypeVar('T')
class Format(Generic[T]):
    """Base class for file formats.

    To implement, override SUFFIX, read(), and write(). Formats are usually
    singleton classes, and are instantiated right here in the module."""

    SUFFIX = NotImplemented

    def read(self, input: BinaryIO) -> T:
        """Reads input, parses it, and returns it."""
        raise NotImplementedError()

    def write(self, input: T, output: BinaryIO) -> None:
        """Writes the given input out to disk."""
        raise NotImplementedError()

class DillFormat(Format[Any]):
    """A format that uses dill to serialize arbitrary Python objects.

    This format has special handling for iterable types. It takes care not to
    read the entire iterable into memory during either reading or writing."""

    SUFFIX = ".dill"

    def read(self, input: BinaryIO) -> Any:
        first = dill.load(input)
        try:
            second = dill.load(input)
        except EOFError:
            return first

        yield first
        yield second
        while True:
            try:
                yield dill.load(input)
            except EOFError:
                break

    def write(self, input: Any, output: BinaryIO) -> None:
        if hasattr(input, "__next__"):
            for item in input:
                dill.dump(item, output)
        else:
            dill.dump(input, output)

dillFormat = DillFormat()

import json
class JsonFormat(Format[Any]):
    """A format that serializes Python object with JSON.

    If you are looking to serialize lists of things, you probably want
    JsonlFormat or JsonlGzFormat."""

    SUFFIX = ".json"

    def read(self, input: BinaryIO) -> Any:
        input = io.TextIOWrapper(input, encoding="UTF-8")
        return json.load(input)

    def write(self, input: Any, output: BinaryIO) -> None:
        output = io.TextIOWrapper(output, encoding="UTF-8")
        json.dump(input, output)

jsonFormat = JsonFormat()

class JsonlFormat(Format[Iterable[Any]]):
    """A format that serializes lists of Python objects to JSON, one line per item."""
    SUFFIX = ".jsonl"

    def read(self, input: BinaryIO) -> Iterable[Any]:
        for line in io.TextIOWrapper(input, encoding="UTF-8"):
            yield json.loads(line)

    def write(self, input: Iterable[Any], output: BinaryIO) -> None:
        output = io.TextIOWrapper(output, encoding="UTF-8")
        for item in input:
            output.write(json.dumps(item))
            output.write("\n")
            output.flush()

jsonlFormat = JsonlFormat()

import gzip
class JsonlGzFormat(Format[Iterable[Any]]):
    """A format that serializes lists of Python objects to JSON, one line per item, and compresses the file."""
    SUFFIX = ".jsonl.gz"

    def read(self, input: BinaryIO) -> Iterable[Any]:
        uncompressed = gzip.GzipFile(mode="rb", fileobj=input)
        for line in io.TextIOWrapper(uncompressed, encoding="UTF-8"):
            yield json.loads(line)

    def write(self, input: Iterable[Any], output: BinaryIO) -> None:
        output = gzip.GzipFile(mode="wb", fileobj=output)
        output = io.TextIOWrapper(output, encoding="UTF-8")
        for item in input:
            output.write(json.dumps(item))
            output.write("\n")
            output.flush()

jsonlGzFormat = JsonlGzFormat()


def random_string(length: int = 16) -> str:
    """Returns a random string of readable characters."""
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))


class Store(object):
    """A key/value store that Pipette uses to store the results from Tasks."""

    def exists(self, name: str) -> bool:
        """Returns True if the given result already exists in the store."""
        raise NotImplementedError()

    def locked(self, name: str) -> bool:
        """Returns True if the given result is locked in the store.

        Results get locked when a task starts working on them, but is not yet
        complete. This prevents multiple processes from working on the same task
        at the same time, and overwriting each other's results."""
        raise NotImplementedError()

    def read(self, name: str, format: Format = dillFormat) -> Any:
        """Reads a result from the store."""
        raise NotImplementedError()

    def write(self, name: str, content: Any, format: Format = dillFormat) -> None:
        """Writes a result to the store.

        While the writing is going on, this locks the results it is writing to,
        so that no other task writes to the same result at the same time."""
        raise NotImplementedError()

    def url_for_name(self, name: str) -> str:
        """Returns a copy-and-paste worthy URL for the result with the given name."""
        raise NotImplementedError()

    def id(self) -> str:
        """Every store has an id. It is unique string that helps recognize when the store changes."""
        raise NotImplementedError()

    _weird_patterns = {"//", "./", "/.", "\\", ".."}
    def _name_check(self, name: str) -> None:
        if name.startswith("/"):
            raise ValueError(f"Name '{name}' can't start with a slash.")
        if name.endswith("/"):
            raise ValueError(f"Name '{name}' can't end with a slash.")
        for weird_pattern in self._weird_patterns:
            if weird_pattern in name:
                raise ValueError(f"Name '{name}' looks weird.")


from pathlib import Path
class LocalStore(Store):
    def __init__(self, base_path: Union[str, Path]):
        if isinstance(base_path, str):
            base_path = Path(base_path)
        self.base_path = base_path
        self.base_path.mkdir(parents=True, exist_ok=True)
        try:
            with (self.base_path / "id.txt").open("xt", encoding="UTF-8") as f:
                f.write(random_string())
        except FileExistsError:
            pass

    def exists(self, name: str) -> bool:
        self._name_check(name)
        return (self.base_path / name).exists()

    def locked(self, name: str) -> bool:
        self._name_check(name)
        return (self.base_path / (name + ".lock")).exists()

    def read(self, name: str, format: Format = dillFormat) -> Any:
        self._name_check(name)
        # This leaks the file until the file object gets garbage collected. A worthwhile tradeoff
        # to get streaming reads to work.
        path = self.base_path / name
        _logger.info("Reading input from %s", path)
        return format.read(path.open("br"))

    def write(self, name: str, content: Any, format: Format = dillFormat) -> None:
        self._name_check(name)
        file_path = self.base_path / name
        lockfile_path = self.base_path / (name + ".lock")

        if file_path.exists():
            raise FileExistsError(file_path)
        if lockfile_path.exists():
            raise FileExistsError(lockfile_path)
        try:
            with lockfile_path.open("wb") as output:
                format.write(content, output)
            lockfile_path.rename(file_path)
        except:
            lockfile_path.unlink()
            raise

    def url_for_name(self, name: str) -> str:
        self._name_check(name)
        return str(self.base_path / name)

    def id(self) -> str:
        with (self.base_path / "id.txt").open("rt", encoding="UTF-8") as f:
            id = f.read()
            return id.strip()

import subprocess
class BeakerStore(Store):
    def __init__(self, local_cache_path: Union[None, str, Path] = None):
        if isinstance(local_cache_path, str):
            local_cache_path = Path(local_cache_path)
        self.local_cache_path = local_cache_path
        if self.local_cache_path is not None:
            self.local_cache_path.mkdir(parents=True, exist_ok=True)

    def id(self) -> str:
        return "beaker"

    class BeakerDataset(NamedTuple):
        name: str
        committed: bool
        task: Optional[str]

    @classmethod
    def _get_dataset(cls, name: str) -> BeakerDataset:
        inspect_ds_process = subprocess.run(
            ["beaker", "dataset", "inspect", name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        if inspect_ds_process.returncode != 0:
            error = inspect_ds_process.stderr.decode("UTF-8")
            if "not a valid name" in error or "does not exist" in error:
                raise FileNotFoundError(name)
            else:
                raise IOError(error)

        inspect_result = json.load(io.BytesIO(inspect_ds_process.stdout))
        assert len(inspect_result) == 1
        inspect_result = inspect_result[0]

        return cls.BeakerDataset(
            name,
            not inspect_result["committed"].startswith("0001-"),
            inspect_result.get("source_task"))

    @classmethod
    def _task_succeeded(cls, task_name: str) -> bool:
        inspect_tk_process = subprocess.run(
            ["beaker", "task", "inspect", task_name],
            check=True,
            stdout=subprocess.PIPE)
        inspect_tk_result = json.load(io.BytesIO(inspect_tk_process.stdout))
        task_status = inspect_tk_result[0]["status"]
        return task_status == "succeeded"

    @classmethod
    def _delete_dataset(cls, name: str):
        """You can't actually delete datasets in Beaker, but you can rename them out of the way."""
        subprocess.run(
            ["beaker", "dataset", "rename", name, f"deleted-{name}-{random_string()}"],
            stdout=subprocess.DEVNULL,
            check=True)

    @classmethod
    def _download_dataset_to(cls, name: str, output: Union[str, Path]):
        fetch_process = subprocess.run(
            ["beaker", "dataset", "fetch", f"--output={output}", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE)
        if fetch_process.returncode != 0:
            error = fetch_process.stderr.decode("UTF-8")
            if "not a valid name" in error:
                raise FileNotFoundError(name)
            else:
                raise IOError(error)

    @classmethod
    def _upload_dataset_from(cls, name, source: Union[str, Path]):
        upload_process = subprocess.run(
            ["beaker", "dataset", "create", f"--name={name}", str(source)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE
        )
        if upload_process.returncode != 0:
            error = upload_process.stderr.decode("UTF-8")
            if "is already in use" in error:
                raise FileExistsError(name)
            else:
                raise IOError(error)

    def exists(self, name: str) -> bool:
        self._name_check(name)

        # check the local cache
        if self.local_cache_path is not None and (self.local_cache_path / name).exists():
            return True

        try:
            ds = self._get_dataset(name)
        except FileNotFoundError:
            return False

        # check if the dataset is committed
        if not ds.committed:
            return False

        # check if the task that produced the dataset succeeded
        if ds.task is None or self._task_succeeded(ds.task):
            return True

        # if it did not succeed, rename the dataset out of the way, and report that the data does
        # not exist
        self._delete_dataset(name)
        return False

    def locked(self, name: str) -> bool:
        self._name_check(name)

        # check the local cache
        if self.local_cache_path is not None and (self.local_cache_path / (name + ".lock")).exists():
            return True

        try:
            ds = self._get_dataset(name)
        except FileNotFoundError:
            return False
        return not ds.committed

    def read(self, name: str, format: Format = dillFormat) -> Any:
        self._name_check(name)

        # local cache case
        if self.local_cache_path is not None:
            local_path = self.local_cache_path / name
            if not local_path.exists():
                down_path = self.local_cache_path / (name + ".down")
                self._download_dataset_to(name, down_path)
                down_path.rename(local_path)
            return format.read(local_path.open("br"))
        else:   # no local cache
            tfile = tempfile.NamedTemporaryFile(prefix=f"{name}-down-", delete=False)
            tfile = Path(tfile.name)
            self._download_dataset_to(name, tfile)
            atexit.register(lambda: tfile.unlink())
            return format.read(tfile.open("br"))

    def write(self, name: str, content: Any, format: Format = dillFormat):
        self._name_check(name)

        if self.locked(name) or self.exists(name):
            raise FileExistsError(name)

        # we write to a local file first, and then upload to beaker
        if self.local_cache_path is None:
            tfile = tempfile.NamedTemporaryFile(prefix=f"{name}-up-", delete=False)
            try:
                format.write(content, tfile)
                tfile.close()
                self._upload_dataset_from(name, Path(tfile.name))
            finally:
                Path(tfile.name).unlink()
        else:
            local_path = (self.local_cache_path / (name + ".lock"))
            try:
                with local_path.open("wb") as output:
                    format.write(content, output)

                self._upload_dataset_from(name, local_path)
                local_path.rename(self.local_cache_path / name)
            finally:
                local_path.unlink()


class TaskStub(NamedTuple):
    """We use this to cut off the dependency chain for tasks that are already done."""
    file_name: str
    format: Format

class SerializedTaskTuple(NamedTuple):
    module_name: str
    class_name: str
    version_tag: str
    inputs: dict

class NamedTuplePickler(dill.Pickler):
    """Using dill, the named tuples we use become huge, so we use this custom pickler to make them
    smaller."""
    def persistent_id(self, obj):
        if obj == TaskStub:
            return 1
        elif obj == SerializedTaskTuple:
            return 2
        else:
            return super(NamedTuplePickler, self).persistent_id(obj)

class NamedTupleUnpickler(dill.Unpickler):
    """Using dill, the named tuples we use become huge, so we use this custom unpickler to make them
    smaller."""
    def persistent_load(self, pid):
        if pid == 1:
            return TaskStub
        elif pid == 2:
            return SerializedTaskTuple
        else:
            return super(NamedTupleUnpickler, self).persistent_load(pid)

def _is_named_tuple_fn(cls) -> Callable[[Any], bool]:
    """For some reason, isinstance(o, *TaskStub) does not work with deserialized task stubs, so
    we have to roll out own hacky way around that."""
    def inner_is_task_stub(o: Any) -> bool:
        if o.__class__.__name__ != cls.__name__:
            return False
        return all((hasattr(o, field) for field in cls._fields))   # If it quacks like a duck ...
    return inner_is_task_stub
_is_task_stub = _is_named_tuple_fn(TaskStub)
_is_serialized_task_tuple = _is_named_tuple_fn(SerializedTaskTuple)

_version_tag_re = re.compile("""^[a-zA-Z0-9]+$""")
O = TypeVar('O')
class Task(Generic[O]):
    VERSION_TAG: str = NotImplemented
    INPUTS: Dict[str, Any] = {}
    DEFAULTS: Dict[str, Any] = {}
    OUTPUT_FORMAT: Format[O] = dillFormat   # Not the most efficient, but it can serialize almost anything. It's a good default.

    def __init__(self, **kwargs):
        assert _version_tag_re.match(self.VERSION_TAG), f"Invalid version tag '{self.VERSION_TAG}'"

        if "store" in kwargs:
            self.store = kwargs["store"]
            del kwargs["store"]
        else:
            store_env_var = os.environ.get("PIPETTE_STORE")
            if store_env_var is None:
                self.store = LocalStore(Path.home() / ".pipette" / "store")
            elif store_env_var == "beaker":
                self.store = BeakerStore()
            elif store_env_var.startswith("beaker:"):
                self.store = BeakerStore(store_env_var[len("beaker:"):])
            else:
                self.store = LocalStore(store_env_var)

        assert "store" not in kwargs
        self.inputs = {**self.DEFAULTS, **kwargs}

        # check the arguments
        extra_arguments = self.inputs.keys() - self.INPUTS.keys()
        if len(extra_arguments) > 0:
            raise ValueError(f"Too many arguments for {self.__class__.__name__}: {' '.join(extra_arguments)}")

        missing_arguments = self.INPUTS.keys() - self.inputs.keys()
        if len(missing_arguments) > 0:
            raise ValueError(f"Missing arguments for {self.__class__.__name__}: {' '.join(missing_arguments)}")

        for name, t in self.INPUTS.items():
            fancy_name = f"{self.__class__.__name__}.{name}"
            try:
                typeguard.check_type(fancy_name, self.inputs[name], t)
            except TypeError as e:
                # Tasks can be replaced by task stubs. That's their whole point.
                if "must be pipette.Task; got __main__.TaskStub instead" in str(e):
                    pass    # It's too hard to check this case properly.
                elif "must be pipette.Task; got pipette.TaskStub instead" in str(e):
                    pass    # It's too hard to check this case properly.
                else:
                    raise

    def do(self, **inputs):
        raise NotImplementedError()

    def results(self):
        printable_inputs = []
        for key, o in self.inputs.items():
            if hasattr(o, "__len__") and len(o) > 1000:
                printable_inputs.append(f"\t{key}: Too big to print")
            else:
                r = repr(o)
                if len(r) > 150:
                    r = r[:150-4] + " ..."
                printable_inputs.append(f"\t{key}: {r}")
        printable_inputs = "\n".join(printable_inputs)

        output_name = self.output_name()
        if self.store.exists(output_name):
            _logger.info(f"Reading {self.output_name()} with the following inputs:\n{printable_inputs}")
            return self.store.read(output_name, self.OUTPUT_FORMAT)

        def replace_tasks_with_results(o: Any):
            if isinstance(o, Task):
                return o.results()
            elif _is_task_stub(o):
                return self.store.read(o.file_name, o.format)
            elif isinstance(o, List):
                return [replace_tasks_with_results(i) for i in o]
            elif isinstance(o, Set):
                return {replace_tasks_with_results(i) for i in o}
            elif isinstance(o, Dict):
                return {key : replace_tasks_with_results(value) for key, value in o.items()}
            else:
                return o
        inputs = replace_tasks_with_results(self.inputs)

        _logger.info(f"Computing {self.output_name()} with the following inputs:\n{printable_inputs}")
        result = self.do(**inputs)
        _logger.info(f"Writing {self.output_name()}")
        self.store.write(output_name, result, self.OUTPUT_FORMAT)

        if hasattr(result, "__next__"):
            # If we just wrote a generator-like function to disk, we need to re-read it.
            return self.store.read(output_name, self.OUTPUT_FORMAT)
        else:
            return result

    def output_exists(self) -> bool:
        return self.store.exists(self.output_name())

    def output_locked(self) -> bool:
        return self.store.locked(self.output_name())

    @staticmethod
    def hash_object(o: Any) -> str:
        with io.BytesIO() as buffer:
            dill.dump(o, buffer)
            hash = mmh3.hash_bytes(buffer.getvalue(), x64arch=True)
        hash = base64.b32encode(hash).decode("UTF-8")
        return hash[:16].lower()

    _cached_output_name = None
    def output_name(self):
        if self._cached_output_name is None:
            def replace_tasks_with_hashes(o: Any):
                if isinstance(o, Task):
                    return o.output_name()
                elif _is_task_stub(o):
                    return o.file_name
                elif isinstance(o, List):
                    return [replace_tasks_with_hashes(i) for i in o]
                elif isinstance(o, Set):
                    return {replace_tasks_with_hashes(i) for i in o}
                elif isinstance(o, Dict):
                    return {key : replace_tasks_with_hashes(value) for key, value in o.items()}
                else:
                    return o
            hash = self.hash_object(replace_tasks_with_hashes(self.inputs))
            self._cached_output_name = f"{self.__class__.__name__}-{self.VERSION_TAG}-{hash}{self.OUTPUT_FORMAT.SUFFIX}"
        return self._cached_output_name

    def output_url(self) -> str:
        return self.store.url_for_name(self.output_name())

    def dependencies(self) -> Dict[str, List['Task']]:
        def dependencies_internal(o: Any) -> Iterable['Task']:
            if isinstance(o, Task):
                yield o
            elif isinstance(o, str):
                return  # Confusingly, str is an Iterable of itself, resulting in infinite recursion.
            elif isinstance(o, Iterable):
                yield from itertools.chain(*(dependencies_internal(i) for i in o))
            elif isinstance(o, Dict):
                yield from dependencies_internal(o.values())
            else:
                return
        return {
            key: list(dependencies_internal(value))
            for key, value in self.inputs.items()
        }

    def flat_unique_dependencies(self) -> Iterable['Task']:
        seen = set()
        for d in itertools.chain(*self.dependencies().values()):
            output_name = d.output_name()
            if output_name in seen:
                continue
            seen.add(output_name)
            yield d

    def serialized_task_config(self) -> str:
        # The important thing about this caching scheme for stubs and tuples is that if a task
        # is reachable through multiple paths in the dependency graph, we still want to serialize
        # it only once. So we make sure that the object graph we pass to the serializer contains
        # every stub and tuple only once, even if it is referenced multiple times.

        # some machinery to replace tasks with task stubs
        output_name_to_task_stub = {}
        def stub_for_task(t: Task) -> TaskStub:
            nonlocal output_name_to_task_stub
            try:
                return output_name_to_task_stub[t.output_name()]
            except KeyError:
                task_stub = TaskStub(t.output_name(), t.OUTPUT_FORMAT)
                output_name_to_task_stub[t.output_name()] = task_stub
                return task_stub

        # some machinery to replace tasks with a serializable tuple form of the task
        output_name_to_tuple = {}
        def tuple_for_task(t: Task) -> SerializedTaskTuple:
            nonlocal output_name_to_tuple
            try:
                return output_name_to_tuple[t.output_name()]
            except KeyError:
                tuple = SerializedTaskTuple(
                    t.__class__.__module__,
                    t.__class__.__name__,
                    t.VERSION_TAG,
                    replace_tasks_with_stubs_and_tuples(t.inputs)
                )
                output_name_to_tuple[t.output_name()] = tuple
                return tuple

        def replace_tasks_with_stubs_and_tuples(o: Any):
            if isinstance(o, Task):
                if o.output_exists():
                    return stub_for_task(o)
                else:
                    return tuple_for_task(o)
            elif isinstance(o, List):
                return [replace_tasks_with_stubs_and_tuples(i) for i in o]
            elif isinstance(o, Set):
                return {replace_tasks_with_stubs_and_tuples(i) for i in o}
            elif isinstance(o, Dict):
                return {key : replace_tasks_with_stubs_and_tuples(value) for key, value in o.items()}
            else:
                return o

        # serialize the task itself
        inputs = replace_tasks_with_stubs_and_tuples(self.inputs)
        with io.BytesIO() as buffer:
            pickler = NamedTuplePickler(buffer)
            pickler.dump((self.store.id(), inputs))
            result = buffer.getvalue()
        result = zlib.compress(result, 9)
        result = base64.urlsafe_b64encode(result)
        result = result.decode("UTF-8")
        return f"{self.__class__.__module__}.{self.__class__.__name__}-{self.VERSION_TAG}({result})"

_task_config_re = re.compile("""^([.a-zA-Z0-9_]+)-([a-zA-Z0-9]+)\(([-a-zA-Z0-9_=]+)\)$""")
def create_from_serialized_task_config(task_config: str) -> Task:
    parsed_tc = _task_config_re.match(task_config)
    if parsed_tc is None:
        raise ValueError(f"Not a valid task config: {task_config}")
    class_name, version_tag, config = parsed_tc.groups()

    # find the class
    module_name, class_name = class_name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    clazz = getattr(module, class_name)

    # find the parameters
    config = config.encode("UTF-8")
    config = base64.urlsafe_b64decode(config)
    config = zlib.decompress(config)
    with io.BytesIO(config) as buffer:
        unpickler = NamedTupleUnpickler(buffer)
        store_id, inputs = unpickler.load()
    assert version_tag == clazz.VERSION_TAG

    # replace all instances of SerializedTaskTuple with Task
    # Again, using this cache not for performance, but so that we get only one task for one
    # task tuple, even if the task tuple is referenced multiple times.
    tuple_id_to_task = {}
    def task_for_tuple(t: SerializedTaskTuple) -> Task:
        nonlocal tuple_id_to_task
        try:
            return tuple_id_to_task[id(t)]
        except KeyError:
            module = importlib.import_module(t.module_name)
            clazz = getattr(module, t.class_name)
            task = clazz(**replace_tuples_with_tasks(t.inputs))
            assert task.VERSION_TAG == t.version_tag
            if store_id != task.store.id():
                _logger.warning("Attempting deserialize a Task that was created on a different store.")
            tuple_id_to_task[id(t)] = task
            return task
    def replace_tuples_with_tasks(o: Any):
        if _is_serialized_task_tuple(o):
            return task_for_tuple(o)
        elif isinstance(o, List):
            return [replace_tuples_with_tasks(i) for i in o]
        elif isinstance(o, Set):
            return {replace_tuples_with_tasks(i) for i in o}
        elif isinstance(o, Dict):
            return {key : replace_tuples_with_tasks(value) for key, value in o.items()}
        else:
            return o
    inputs = replace_tuples_with_tasks(inputs)

    instance = clazz(**inputs)
    if store_id != instance.store.id():
        _logger.warning("Attempting deserialize a Task that was created on a different store.")

    return instance

def to_graphviz(task_or_tasks: Union[Task, Iterable[Task]]) -> str:
    if isinstance(task_or_tasks, Task):
        task_or_tasks = [task_or_tasks]

    result = ["digraph G {"]

    maybe_not_graphed_yet = task_or_tasks
    already_graphed = set()
    while len(maybe_not_graphed_yet) > 0:
        t = maybe_not_graphed_yet.pop()
        t_name = t.output_name()
        if t_name in already_graphed:
            continue

        # put this node into the output
        title = t.__class__.__name__
        subtitle = t_name[len(title)+1:]
        style = [f'label="{title}\\n{subtitle}"']
        if t.output_exists():
            style.append("style=filled")
        style = " ".join(style)
        result.append(f'  "{t_name}" [{style}];')
        already_graphed.add(t_name)

        # put connections into the output
        for key, dependencies in t.dependencies().items():
            for dependency in dependencies:
                maybe_not_graphed_yet.append(dependency)
                result.append(f'  "{dependency.output_name()}" -> "{t_name}" [label="{key}"];')

    result.append("}")
    return "\n".join(result)

from .asciidag import graph as adgraph
from .asciidag import node as adnode
def to_asciidag(
    task_or_tasks: Union[Task, List[Task]],
    *,
    only_incomplete: bool = False,
    print_commands: bool = False
) -> List[adnode.Node]:
    if isinstance(task_or_tasks, Task):
        task_or_tasks = [task_or_tasks]
    tasks = task_or_tasks
    if only_incomplete:
        tasks = [t for t in tasks if not t.output_exists()]

    output_name_to_node = {}
    def node_for_task(t: Task) -> adnode.Node:
        nonlocal output_name_to_node
        try:
            return output_name_to_node[t.output_name()]
        except KeyError:
            tags = set()
            if t.output_exists():
                tags.add("complete")
            if t.output_locked():
                tags.add("locked")
            if print_commands:
                text = f'python -m pipette run "{t.serialized_task_config()}"'
            else:
                text = t.output_name()
            if len(tags) > 0:
                text += f" ({', '.join(tags)})"
            node = adnode.Node(
                text,
                parents=[
                    node_for_task(dep)
                    for dep in t.flat_unique_dependencies()
                    if (not only_incomplete) or not dep.output_exists()
                ]
            )
            output_name_to_node[t.output_name()] = node
            return node
    return [node_for_task(t) for t in tasks]

def to_commands(
    task_or_tasks: Union[Task, List[Task]],
    *,
    only_runnable_now: bool = False
) -> Iterable[str]:
    if isinstance(task_or_tasks, Task):
        task_or_tasks = [task_or_tasks]
    tasks = task_or_tasks

    name_to_task = {}
    tasks_done = set()
    tasks_waiting = set()

    # fill up tasks_done and tasks_waiting
    while len(tasks) > 0:
        t = tasks.pop()
        t_name = t.output_name()
        if t_name in name_to_task:
            continue
        name_to_task[t_name] = t

        if t.output_exists():
            tasks_done.add(t_name)
        else:
            tasks_waiting.add(t_name)
            tasks.extend(t.flat_unique_dependencies())

    while len(tasks_waiting) > 0:
        # find all the tasks that are ready
        tasks_ready = set()
        for t in tasks_waiting:
            dependencies = {d.output_name() for d in name_to_task[t].flat_unique_dependencies()}
            if len(dependencies) == len(dependencies & tasks_done):
                tasks_ready.add(t)

        # print all the tasks that are ready
        assert len(tasks_ready & tasks_done) <= 0
        for t in tasks_ready:
            suffix = ""
            if name_to_task[t].output_locked():
                suffix = " (locked)"
            yield f'python -m pipette run "{name_to_task[t].serialized_task_config()}"{suffix}'

        assert len(tasks_ready) > 0
        if only_runnable_now:
            return
        else:
            yield "# --"

        tasks_done |= tasks_ready
        tasks_waiting -= tasks_ready

def runnable_commands(task_or_tasks: Union[Task, Iterable[Task]]) -> Iterable[str]:
    yield from to_commands(task_or_tasks, only_runnable_now=True)

def main(args: List[str], tasks: Optional[Union[Task, List[Task]]] = None) -> int:
    if tasks is None:
        tasks = []
    elif isinstance(tasks, Task):
        tasks = [tasks]
    logging.basicConfig(level=logging.INFO)

    import argparse
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(title="commands", dest="command")
    run_parser = subparsers.add_parser("run", description="Run a task, or multiple tasks")
    graphviz_parser = subparsers.add_parser("graphviz", description="Print a graph description in dot format")

    runnable_parser = subparsers.add_parser("runnable", description="Print descriptions of all currently runnable tasks")
    runnable_parser.add_argument("--all", "-a", default=False, action="store_true")

    graph_parser = subparsers.add_parser("graph", description="Print a graph description in git format")
    graph_parser.add_argument("--only-incomplete", default=False, action="store_true", help="Only print incomplete tasks")
    graph_parser.add_argument("--commands", default=False, action="store_true", help="Print commands, not task names")
    graph_parser.add_argument("--runnable", "-r", default=False, action="store_true", help="Combination of --commands and --only-incomplete")

    for subparser in [run_parser, graphviz_parser, graph_parser, runnable_parser]:
        subparser.add_argument(
            "serialized_task_configs",
            type=str,
            nargs="*",
            default=[],
            help=f"The serialized task configs you got from '{args[0]} runnable'")

    args = parser.parse_args(args[1:])
    if args.command is None:
        parser.print_usage()
        return 1
    if len(args.serialized_task_configs) > 0:
        tasks = map(create_from_serialized_task_config, args.serialized_task_configs)

    if args.command == "graphviz":
        print(to_graphviz(tasks))
    elif args.command == "graph":
        if args.runnable:
            args.commands = True
            args.only_incomplete = True
        g = adgraph.Graph()
        g.show_nodes(
            to_asciidag(
                tasks,
                only_incomplete=args.only_incomplete,
                print_commands = args.commands))
    elif args.command == "runnable":
        for serialized_command in to_commands(tasks, only_runnable_now=not args.all):
            print(serialized_command)
    elif args.command == "run":
        for task in tasks:
            task.results()
            print(task.output_url())
    else:
        raise ValueError("No command specified")

    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main(sys.argv))
