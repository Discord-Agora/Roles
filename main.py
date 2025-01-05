import asyncio
import itertools
import logging
import math
import os
import re
import time
import traceback
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from functools import lru_cache, partial, wraps
from itertools import islice
from logging.handlers import RotatingFileHandler
from multiprocessing import cpu_count
from operator import attrgetter
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Concatenate,
    Coroutine,
    DefaultDict,
    Dict,
    FrozenSet,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    ParamSpec,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import aiofiles
import aiofiles.os
import aiofiles.ospath
import aiohttp
import aioshutil
import cysimdjson
import interactions
import numpy as np
import orjson
from cachetools import TTLCache
from interactions.api.events import (
    ExtensionLoad,
    ExtensionUnload,
    MemberAdd,
    MemberRemove,
    MemberUpdate,
    MessageCreate,
    MessageReactionAdd,
    MessageReactionRemove,
    NewThreadCreate,
)
from interactions.client.errors import HTTPException, NotFound
from interactions.ext.paginators import Paginator
from pydantic import BaseModel
from yarl import URL

BASE_DIR: str = os.path.dirname(os.path.abspath(__file__))
LOG_FILE: str = os.path.join(BASE_DIR, "roles.log")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s | %(process)d:%(thread)d | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s",
    "%Y-%m-%d %H:%M:%S.%f %z",
)
file_handler = RotatingFileHandler(
    LOG_FILE, maxBytes=1024 * 1024, backupCount=1, encoding="utf-8"
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# Model


T = TypeVar("T", bound=Union[BaseModel, Counter, Dict[str, Any]])

P = ParamSpec("P")


class Status(Enum):
    APPROVED = auto()
    REJECTED = auto()


class EmbedColor(Enum):
    OFF = 0x5D5A58
    FATAL = 0xFF4343
    ERROR = 0xE81123
    WARN = 0xFFB900
    INFO = 0x0078D7
    DEBUG = 0x00B7C3
    TRACE = 0x8E8CD8
    ALL = 0x0063B1


class Action(Enum):
    ADD = "add"
    REMOVE = "remove"
    INCARCERATE = "incarcerate"
    RELEASE = "release"


class Data(BaseModel):
    assigned_roles: Dict[str, Dict[str, int]] = field(default_factory=dict)
    authorized_roles: Dict[str, int] = field(default_factory=dict)
    assignable_roles: Dict[str, List[str]] = field(default_factory=dict)
    incarcerated_members: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    class Config:
        json_encoders = {
            set: list,
        }


@dataclass
class Config:
    ELECT_VETTING_FORUM_ID: int = 1164834982737489930
    APPR_VETTING_FORUM_ID: int = 1307001955230552075
    VETTING_ROLE_IDS: List[int] = field(default_factory=lambda: [1200066469300551782])
    ELECTORAL_ROLE_ID: int = 1200043628899356702
    APPROVED_ROLE_ID: int = 1282944839679344721
    TEMPORARY_ROLE_ID: int = 1164761892015833129
    MINISTER_ROLE_ID: int = 1297556675473182720
    MISSING_ROLE_ID: int = 1289949397362409472
    INCARCERATED_ROLE_ID: int = 1247284720044085370
    AUTHORIZED_CUSTOM_ROLE_IDS: List[int] = field(
        default_factory=lambda: [1213490790341279754]
    )
    AUTHORIZED_PENITENTIARY_ROLE_IDS: List[int] = field(
        default_factory=lambda: [1200097748259717193, 1247144717083476051]
    )
    REQUIRED_APPROVALS: int = 3
    REQUIRED_REJECTIONS: int = 3
    REJECTION_WINDOW_DAYS: int = 7
    LOG_CHANNEL_ID: int = 1166627731916734504
    LOG_FORUM_ID: int = 1159097493875871784
    LOG_POST_ID: int = 1325394043177275445
    GUILD_ID: int = 1150630510696075404


@dataclass
class Servant:
    role_name: str
    members: List[str]
    member_count: int


@dataclass
class Approval:
    approval_count: int = 0
    rejection_count: int = 0
    reviewers: Set[int] = field(default_factory=set)
    last_approval_time: Optional[datetime] = None


class StickyRoles:

    def __init__(self) -> None:
        self.base_path: Path = Path(__file__).parent
        self.db_path = self.base_path / "sticky_roles.json"
        self._data_cache: Optional[dict] = None
        self._last_read: float = 0
        self._cache_ttl: float = 1.0
        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None

    async def read_data(self) -> Dict[str, Any]:
        async with self._lock:
            if (
                not self._data_cache
                or (time.monotonic() - self._last_read) > self._cache_ttl
            ):
                try:
                    self._data_cache = orjson.loads(
                        await (await aiofiles.open(self.db_path, mode="rb")).read()
                    )
                    self._last_read = time.monotonic()
                except (IOError, orjson.JSONDecodeError):
                    self._data_cache = {"members": {}}
            return self._data_cache or {"members": {}}

    async def write_data(self, data: Dict[str, Any]) -> None:
        async with self._lock:
            serialized = orjson.dumps(
                data, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS
            )
            async with aiofiles.open(self.db_path, mode="wb") as f:
                await f.write(serialized)
            self._data_cache, self._last_read = data, time.monotonic()

    async def get_sticky_roles(self, member_id: int) -> list[int]:
        try:
            data = await self.read_data()
            return data.get("members", {}).get(str(member_id), {}).get("role_ids", [])
        except Exception as e:
            logger.error(
                f"Error getting sticky roles for member {member_id}: {e}",
                exc_info=True,
            )
            return []

    async def update_sticky_roles(self, member_id: int, role_ids: list[int]) -> None:
        try:
            role_ids = [*{int(rid) for rid in role_ids}]
            data = await self.read_data()

            ts = datetime.now(timezone.utc).isoformat()

            data["members"][str(member_id)] = {"role_ids": role_ids, "updated_at": ts}

            await self.write_data(data)

        except ValueError as e:
            logger.error(
                f"Invalid role ID format for member {member_id}: {e}",
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                f"Error updating sticky roles for member {member_id}: {e}",
                exc_info=True,
            )
            raise

    async def cleanup_inactive_roles(self, days: int = 30) -> None:
        try:
            data = await self.read_data()
            cutoff_ts = (datetime.now(timezone.utc) - timedelta(days=days)).timestamp()

            data["members"] = {
                mid: mdata
                for mid, mdata in data["members"].items()
                if datetime.fromisoformat(mdata["updated_at"]).timestamp() >= cutoff_ts
            }

            await self.write_data(data)

        except Exception as e:
            logger.error(f"Error during sticky roles cleanup: {e}", exc_info=True)
            raise


class Model(Generic[T]):
    def __init__(self) -> None:
        self.base_path: URL = URL(str(Path(__file__).parent))
        self._data_cache: Dict[str, Any] = {}
        self.parser: cysimdjson.JSONParser = cysimdjson.JSONParser()
        self._file_locks: Dict[str, asyncio.Lock] = {}
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=min(cpu_count(), 4)
        )

    @staticmethod
    def async_retry(max_retries: int = 3, delay: float = 1.0) -> Callable:
        def decorator(func: Callable[..., Coroutine]) -> Callable[..., Coroutine]:
            @wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                for attempt in range(max_retries):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        if attempt == max_retries - 1:
                            raise
                        logger.warning(
                            f"Attempt {attempt + 1} failed: {e}. Retrying..."
                        )
                        await asyncio.sleep(delay * (2**attempt))
                return None

            return wrapper

        return decorator

    async def get_file_lock(self, file_name: str) -> asyncio.Lock:
        return self._file_locks.setdefault(file_name, asyncio.Lock())

    @asynccontextmanager
    async def file_operation(
        self, file_path: Path, mode: str
    ) -> AsyncGenerator[Any, None]:
        lock = await self.get_file_lock(str(file_path))
        async with lock:
            try:
                async with aiofiles.open(str(file_path), mode=mode) as file:
                    yield file
            except IOError as e:
                logger.error(f"IO operation failed for {file_path}: {e}", exc_info=True)
                raise

    @async_retry()
    async def load_data(self, file_name: str, model: Type[T]) -> T:
        file_path = self.base_path / file_name
        try:
            async with self.file_operation(file_path, "rb") as file:
                content = await file.read()
                json_parsed = orjson.loads(content)

                if issubclass(model, BaseModel):
                    instance = (
                        model.model_validate_json(content)
                        if content
                        else model.model_validate({})
                    )
                else:
                    instance = cast(T, json_parsed if json_parsed else {})
                    if file_name == "custom.json":
                        instance = {
                            role: set(members) for role, members in instance.items()
                        }

                self._data_cache[file_name] = instance
                return instance

        except FileNotFoundError:
            instance = model.model_validate({}) if issubclass(model, BaseModel) else {}
            await self.save_data(file_name, instance)
            return cast(T, instance)

        except Exception as e:
            logger.error(f"Error loading {file_name}: {e}", exc_info=True)
            raise ValueError(f"Failed to load {file_name}") from e

    @async_retry()
    async def save_data(self, file_name: str, data: T) -> None:
        file_path = self.base_path / file_name
        try:
            json_data = orjson.dumps(
                (data.model_dump(mode="json") if isinstance(data, BaseModel) else data),
                option=orjson.OPT_INDENT_2
                | orjson.OPT_SERIALIZE_NUMPY
                | orjson.OPT_NON_STR_KEYS,
            )

            async with self.file_operation(file_path, "wb") as file:
                await file.write(json_data)

            self._data_cache[file_name] = data
            logger.info(f"Successfully saved data to {file_name}")

        except Exception as e:
            logger.error(f"Error saving {file_name}: {e}", exc_info=True)
            raise

    def __del__(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)


@dataclass
class Message:
    message: str
    user_stats: Dict[str, Any]
    config: Dict[str, float]
    validation_flags: Dict[str, bool]
    _message_length: int = field(init=False, repr=False)
    _char_frequencies: Counter = field(init=False, repr=False)
    _is_chinese: bool = field(init=False, repr=False)

    def __post_init__(self) -> None:
        msg = self.message or ""
        object.__setattr__(self, "_message_length", len(msg))
        object.__setattr__(self, "_char_frequencies", Counter(msg))
        object.__setattr__(
            self,
            "_is_chinese",
            any(
                ord(c) in range(0x4E00, 0x9FFF + 1)
                or ord(c) in range(0x3400, 0x4DBF + 1)
                for c in msg
            ),
        )

    def analyze(self) -> frozenset[str]:
        violations: set[str] = set()

        if self.validation_flags["repetition"]:
            if self._check_repetition():
                violations.add("message_repetition")

        if self.validation_flags["digit_ratio"]:
            if self._check_digit_ratio():
                violations.add("excessive_digits")

        if self.validation_flags["entropy"]:
            if self._check_entropy():
                violations.add("low_entropy")

        if self.validation_flags["feedback"]:
            self.user_stats["feedback_score"] = max(
                -5, min(5, self.user_stats.get("feedback_score", 0) - len(violations))
            )

        return frozenset(violations)

    def _check_repetition(self) -> bool:
        last_msg = self.user_stats.get("last_message", "")
        if self.message == last_msg:
            rep_count = self.user_stats.get("repetition_count", 0) + 1
            self.user_stats["repetition_count"] = rep_count
            return rep_count >= self.config["MAX_REPEATED_MESSAGES"]
        self.user_stats.update({"repetition_count": 0, "last_message": self.message})
        return False

    def _check_digit_ratio(self) -> bool:
        if not self._message_length:
            return False
        threshold = self.config["DIGIT_RATIO_THRESHOLD"] * (
            1.5 if self._is_chinese else 1.0
        )
        digit_count = sum(1 for c in self.message if c.isdigit())
        return (digit_count / self._message_length) > threshold

    def _check_entropy(self) -> bool:
        if not self._message_length:
            return False

        freqs = np.array(list(self._char_frequencies.values()), dtype=np.float64)
        probs = freqs / self._message_length
        entropy = -np.sum(probs * np.log2(probs))

        base_threshold = self.config["MIN_MESSAGE_ENTROPY"] * (
            0.7 if self._is_chinese else 1.0
        )
        length_adjustment = (np.log2(max(self._message_length, 2)) / 10) * (
            0.8 if self._is_chinese else 1.0
        )

        return entropy < max(base_threshold, 2.0 - length_adjustment)


# Controller


class ChannelHistoryIteractor:
    def __init__(self, history: interactions.ChannelHistory) -> None:
        self.history: interactions.ChannelHistory = history
        self._retries = 0
        self.MAX_RETRIES = 3
        self.RETRY_DELAY = 1.0

    def __aiter__(self):
        return self

    async def __anext__(self):
        while self._retries < self.MAX_RETRIES:
            try:
                return await self.history.__anext__()
            except StopAsyncIteration:
                raise
            except HTTPException as e:
                try:
                    match e.code:
                        case 50083 | 10003 | 50001 | 50013:
                            logger.error(
                                f"Channel {self.history.channel.name} ({self.history.channel.id}): "
                                f"{'archived thread' if e.code == 50083 else 'unknown channel' if e.code == 10003 else 'no access' if e.code == 50001 else 'lacks permission'}"
                            )
                            raise StopAsyncIteration
                        case 10008:
                            logger.warning(
                                f"Unknown message in Channel {self.history.channel.name} ({self.history.channel.id})"
                            )
                        case 50021:
                            logger.warning(
                                f"System message in Channel {self.history.channel.name} ({self.history.channel.id})"
                            )
                        case 160005:
                            logger.warning(
                                f"Channel {self.history.channel.name} ({self.history.channel.id}) is a locked thread"
                            )
                        case _:
                            logger.warning(
                                f"Channel {self.history.channel.name} ({self.history.channel.id}) has unknown code {e.code}"
                            )
                except ValueError:
                    logger.warning(
                        f"Unknown HTTP exception {e.code} {e.errors} {e.route} {e.response} {e.text}",
                        stack_info=True,
                    )
            except aiohttp.ClientPayloadError as e:
                self._retries += 1
                if self._retries >= self.MAX_RETRIES:
                    logger.error(
                        f"Failed to fetch message history after {self.MAX_RETRIES} retries: {str(e)}"
                    )
                    raise StopAsyncIteration
                logger.warning(
                    f"ClientPayloadError occurred (attempt {self._retries}/{self.MAX_RETRIES}): {str(e)}"
                )
                await asyncio.sleep(self.RETRY_DELAY * self._retries)
                continue
            except Exception as e:
                logger.warning(
                    f"Unknown exception {e.__class__.__name__}: {str(e)}", exc_info=True
                )
                self._retries += 1
                if self._retries >= self.MAX_RETRIES:
                    raise StopAsyncIteration
                await asyncio.sleep(self.RETRY_DELAY * self._retries)
                continue


class Roles(interactions.Extension):
    def __init__(self, bot: interactions.Client):
        self.bot: interactions.Client = bot
        self.config: Config = Config()
        self.sticky_roles: StickyRoles = StickyRoles()
        self.vetting_roles: Data = Data()
        self.custom_roles: Dict[str, Set[int]] = {}
        self.incarcerated_members: Dict[str, Dict[str, Any]] = {}
        self.stats: Dict[str, Dict[str, Any]] = {}
        self.processed_thread_ids: Set[int] = set()
        self.approval_counts: Dict[int, Approval] = {}
        self.member_role_locks: Dict[int, Dict[str, Union[asyncio.Lock, datetime]]] = {}
        self.stats_lock: asyncio.Lock = asyncio.Lock()
        self.stats_save_task: asyncio.Task | None = None
        self.reaction_roles: Dict[str, Dict[str, Any]] = {}
        self.message_monitoring_enabled: bool = False
        self.validation_flags: Dict[str, bool] = {
            "repetition": True,
            "digit_ratio": True,
            "entropy": True,
            "feedback": True,
        }
        self.limit_config: Dict[str, Union[float, int]] = {
            "MESSAGE_WINDOW_SECONDS": 60.0,
            "MAX_REPEATED_MESSAGES": 3,
            "DIGIT_RATIO_THRESHOLD": 0.5,
            "MIN_MESSAGE_ENTROPY": 1.5,
        }
        self.excluded_role_ids = {
            self.config.ELECTORAL_ROLE_ID,
            self.config.APPROVED_ROLE_ID,
            self.config.TEMPORARY_ROLE_ID,
        }

        self.cache = TTLCache(maxsize=100, ttl=300)

        self.base_path: Path = Path(__file__).parent
        self.model: Model[Any] = Model()
        self.load_tasks: List[Coroutine] = [
            self.model.load_data("vetting.json", Data),
            self.model.load_data("custom.json", dict),
            self.model.load_data("incarcerated_members.json", dict),
            self.model.load_data("stats.json", dict),
            self.model.load_data("reaction_roles.json", dict),
        ]

        asyncio.create_task(self.load_initial_data())

    async def load_initial_data(self) -> None:
        try:
            results = await asyncio.gather(*self.load_tasks)
            (
                self.vetting_roles,
                self.custom_roles,
                self.incarcerated_members,
                self.stats,
                self.reaction_roles,
            ) = results
            logger.info("Initial data loaded successfully")

            data = await self.sticky_roles.read_data()
            sticky_role_updates: List[Tuple[int, List[int]]] = []
            after = None

            while True:
                members = await self.bot.http.list_members(
                    self.config.GUILD_ID, limit=1000, after=after
                )
                if not members:
                    break

                for member in members:
                    member_id = int(member["user"]["id"])
                    role_ids = [int(role_id) for role_id in member["roles"]]
                    if role_ids:
                        sticky_role_updates.append((member_id, role_ids))

                after = members[-1]["user"]["id"]

            for member_id, roles in sticky_role_updates:
                ts = datetime.now(timezone.utc).isoformat()
                data["members"][str(member_id)] = {"role_ids": roles, "updated_at": ts}

            await self.sticky_roles.write_data(data)
            logger.info(
                f"Sticky roles database initialized with {len(sticky_role_updates)} members"
            )

        except Exception as e:
            logger.critical(f"Failed to load critical data: {e}", exc_info=True)
            raise

    # Decorator

    ContextType = TypeVar("ContextType", bound=interactions.BaseContext)

    @staticmethod
    def error_handler(
        func: Callable[Concatenate[Any, ContextType, P], Coroutine[Any, Any, T]]
    ) -> Callable[Concatenate[Any, ContextType, P], Coroutine[Any, Any, T]]:
        @wraps(func)
        async def wrapper(
            self, ctx: interactions.BaseContext, *args: P.args, **kwargs: P.kwargs
        ) -> T:
            try:
                result = await asyncio.shield(func(self, ctx, *args, **kwargs))
                logger.info(f"`{func.__name__}` completed successfully: {result}")
                return result
            except asyncio.CancelledError as ce:
                logger.warning(
                    f"{func.__name__} was cancelled",
                    extra={"exc_info": True, "stack_info": True},
                )
                raise ce from None
            except Exception as e:
                error_msg = f"Error in {func.__name__}: {e!r}\n{traceback.format_exc()}"
                logger.exception(error_msg)
                raise e from None

        return wrapper

    # Validators

    def get_assignable_role_ids(self) -> frozenset[int]:
        return frozenset(
            role_id
            for roles in self.vetting_roles.assigned_roles.values()
            for name, role_id in roles.items()
            if any(
                name in a_roles
                for a_roles in self.vetting_roles.assignable_roles.values()
            )
        )

    def has_required_roles(
        self,
        ctx: interactions.BaseContext,
        required_role_ids: frozenset[int],
        role_ids_to_check: frozenset[int] | None = None,
        check_assignable: bool = False,
    ) -> bool:
        has_permission = bool(
            frozenset(map(attrgetter("id"), getattr(ctx, "author").roles))
            & required_role_ids
        )
        return has_permission and (
            not check_assignable
            or (
                role_ids_to_check is not None
                and role_ids_to_check <= self.get_assignable_role_ids()
            )
        )

    def validate_vetting_permissions(self, ctx: interactions.BaseContext) -> bool:
        return self.has_required_roles(
            ctx, frozenset(self.vetting_roles.authorized_roles.values())
        )

    def validate_vetting_permissions_with_roles(
        self, ctx: interactions.BaseContext, role_ids_to_add: Iterable[int]
    ) -> bool:
        return self.has_required_roles(
            ctx,
            frozenset(self.vetting_roles.authorized_roles.values()),
            frozenset(role_ids_to_add),
            check_assignable=True,
        )

    def validate_custom_permissions(self, ctx: interactions.BaseContext) -> bool:
        return self.has_required_roles(
            ctx, frozenset(self.config.AUTHORIZED_CUSTOM_ROLE_IDS)
        )

    def validate_penitentiary_permissions(self, ctx: interactions.BaseContext) -> bool:
        return self.has_required_roles(
            ctx, frozenset(self.config.AUTHORIZED_PENITENTIARY_ROLE_IDS)
        )

    @lru_cache()
    def _get_category_role_ids(
        self, category: str, *, _cache: dict[str, frozenset[int]] | None = None
    ) -> frozenset[int]:
        key = f"category_role_ids_{category}"
        if _cache is None:
            return frozenset(
                self.vetting_roles.assigned_roles.get(category, {}).values()
            )
        if key not in _cache:
            _cache[key] = frozenset(
                self.vetting_roles.assigned_roles.get(category, {}).values()
            )
        return _cache[key]

    async def check_role_assignment_conflicts(
        self,
        ctx: interactions.SlashContext,
        member: interactions.Member,
        role_ids_to_add: Iterable[int],
    ) -> bool:
        member_roles = frozenset(map(attrgetter("id"), member.roles))
        roles_to_add = frozenset(role_ids_to_add)

        others_role_ids = self._get_category_role_ids("others")

        roles_to_check = roles_to_add - others_role_ids

        if not roles_to_check:
            return False

        conflicts = [
            (
                member_roles & category_roles,
                roles_to_add & category_roles,
            )
            for category, category_roles in (
                (cat, self._get_category_role_ids(cat))
                for cat in self.vetting_roles.assigned_roles
                if cat != "others"
            )
            if bool(member_roles & category_roles)
            and bool(roles_to_add & category_roles)
            and len((member_roles & category_roles) | (roles_to_add & category_roles))
            > 1
        ]

        if conflicts:
            existing, adding = conflicts[0]
            await self.send_error(
                ctx,
                f"Conflicting roles detected in the category. "
                f"Member already has {len(existing)} role(s) "
                f"and is attempting to add {len(adding)} role(s).",
            )
            return True

        return False

    # View methods

    async def create_embed(
        self,
        title: str,
        description: str = "",
        color: Union[EmbedColor, int] = EmbedColor.INFO,
        fields: Optional[List[Dict[str, str]]] = None,
    ) -> interactions.Embed:
        color_value: int = color.value if isinstance(color, EmbedColor) else color

        embed: interactions.Embed = interactions.Embed(
            title=title, description=description, color=color_value
        )

        if fields:
            for field in fields:
                embed.add_field(
                    name=field.get("name", ""),
                    value=field.get("value", ""),
                    inline=field.get("inline", True),
                )

        guild: Optional[interactions.Guild] = await self.bot.fetch_guild(
            self.config.GUILD_ID
        )
        if guild and guild.icon:
            embed.set_footer(text=guild.name, icon_url=guild.icon.url)

        embed.timestamp = datetime.now(timezone.utc)
        embed.set_footer(text="鍵政大舞台")
        return embed

    async def notify_vetting_reviewers(
        self,
        reviewer_role_ids: List[int],
        thread: interactions.GuildPublicThread,
        timestamp: str,
    ) -> None:
        if not (guild := await self.bot.fetch_guild(thread.guild.id)):
            error_msg = f"Could not fetch the guild with ID {thread.guild.id}."
            logger.error(error_msg)
            raise ValueError(error_msg)

        is_appr_forum = thread.parent_id == self.config.APPR_VETTING_FORUM_ID
        title = (
            f"Quick Identity Verification #{timestamp}"
            if is_appr_forum
            else f"Voter Identity Verification #{timestamp}"
        )

        embed = await self.create_embed(
            title=title,
            description=f"[Click here to jump: {thread.name}](https://discord.com/channels/{thread.guild.id}/{thread.id})",
        )

        async def process_role(role_id: int) -> None:
            try:
                if not (role := await guild.fetch_role(role_id)):
                    logger.error(f"Reviewer role with ID {role_id} not found.")
                    return

                for member in role.members:
                    await self.send_direct_message(member, embed)

                logger.info(
                    f"Notifications sent to role ID {role_id} in thread {thread.id}"
                )
            except Exception as e:
                logger.error(f"Error processing role {role_id}: {e}", exc_info=True)

        for role_id in reviewer_role_ids:
            await process_role(role_id)

        logger.info(f"All reviewer notifications sent for thread {thread.id}")

    @staticmethod
    async def send_direct_message(
        member: interactions.Member, embed: interactions.Embed
    ) -> None:
        try:
            await member.send(embed=embed)
            logger.debug(f"Sent notification to member {member.id}")
        except Exception as e:
            logger.error(f"Failed to send embed to {member.id}: {e}", exc_info=True)

    @lru_cache(maxsize=1)
    def get_log_channels(self) -> tuple[int, int, int]:
        return (
            self.config.LOG_CHANNEL_ID,
            self.config.LOG_POST_ID,
            self.config.LOG_FORUM_ID,
        )

    async def send_response(
        self,
        ctx: Optional[
            Union[
                interactions.SlashContext,
                interactions.InteractionContext,
                interactions.ComponentContext,
            ]
        ],
        title: str,
        message: str,
        color: EmbedColor,
        log_to_channel: bool = True,
        ephemeral: bool = True,
    ) -> None:
        embed: interactions.Embed = await self.create_embed(title, message, color)

        if ctx:
            await ctx.send(embed=embed, ephemeral=ephemeral)

        if log_to_channel:
            LOG_CHANNEL_ID, LOG_POST_ID, LOG_FORUM_ID = self.get_log_channels()
            await self.send_to_channel(LOG_CHANNEL_ID, embed)
            await self.send_to_forum_post(LOG_FORUM_ID, LOG_POST_ID, embed)

    async def send_to_channel(self, channel_id: int, embed: interactions.Embed) -> None:
        try:
            channel = await self.bot.fetch_channel(channel_id)

            if not isinstance(
                channel := (
                    channel if isinstance(channel, interactions.GuildText) else None
                ),
                interactions.GuildText,
            ):
                logger.error(f"Channel ID {channel_id} is not a valid text channel.")
                return

            await channel.send(embed=embed)

        except NotFound as nf:
            logger.error(f"Channel with ID {channel_id} not found: {nf!r}")
        except Exception as e:
            logger.error(f"Error sending message to channel {channel_id}: {e!r}")

    async def send_to_forum_post(
        self, forum_id: int, post_id: int, embed: interactions.Embed
    ) -> None:
        try:
            if not isinstance(
                forum := await self.bot.fetch_channel(forum_id), interactions.GuildForum
            ):
                logger.error(f"Channel ID {forum_id} is not a valid forum channel.")
                return

            if not isinstance(
                thread := await forum.fetch_post(post_id),
                interactions.GuildPublicThread,
            ):
                logger.error(f"Post with ID {post_id} is not a valid thread.")
                return

            await thread.send(embed=embed)

        except NotFound:
            logger.error(f"{forum_id=}, {post_id=} - Forum or post not found")
        except Exception as e:
            logger.error(f"Forum post error [{forum_id=}, {post_id=}]: {e!r}")

    async def send_error(
        self,
        ctx: Optional[
            Union[
                interactions.SlashContext,
                interactions.InteractionContext,
                interactions.ComponentContext,
            ]
        ],
        message: str,
        log_to_channel: bool = False,
        ephemeral: bool = True,
    ) -> None:
        await self.send_response(
            ctx, "Error", message, EmbedColor.ERROR, log_to_channel, ephemeral
        )

    async def send_success(
        self,
        ctx: Optional[
            Union[
                interactions.SlashContext,
                interactions.InteractionContext,
                interactions.ComponentContext,
            ]
        ],
        message: str,
        log_to_channel: bool = True,
        ephemeral: bool = True,
    ) -> None:
        await self.send_response(
            ctx, "Success", message, EmbedColor.INFO, log_to_channel, ephemeral
        )

    async def create_review_components(
        self,
        thread: interactions.GuildPublicThread,
    ) -> Tuple[interactions.Embed, List[interactions.Button]]:
        approval_info: Approval = self.approval_counts.get(thread.id, Approval())
        approval_count: int = approval_info.approval_count

        is_appr_forum = thread.parent_id == self.config.APPR_VETTING_FORUM_ID
        required_approvals = 1 if is_appr_forum else self.config.REQUIRED_APPROVALS
        title = (
            "Quick Identity Verification"
            if is_appr_forum
            else "Voter Identity Verification"
        )

        reviewers_text: str = (
            ",".join(f"<@{rid}>" for rid in sorted(approval_info.reviewers, key=int))
            if approval_info.reviewers
            else "No review records available"
        )

        embed: interactions.Embed = await self.create_embed(
            title=title,
            description=(
                f"- Current status: {approval_count}/{required_approvals} votes\n"
                f"- Review records: {reviewers_text}"
            ),
        )

        return embed, [
            interactions.Button(style=s, label=l, custom_id=c)
            for s, l, c in (
                (interactions.ButtonStyle.SUCCESS, "Approve", "approve"),
                (interactions.ButtonStyle.DANGER, "Reject", "reject"),
            )
        ]

    # Sticky roles

    @interactions.listen("MemberRemove")
    async def on_member_remove(self, event: MemberRemove) -> None:
        try:
            member_roles = [role.id for role in event.member.roles]
            if member_roles:
                await self.sticky_roles.update_sticky_roles(
                    event.member.id, member_roles
                )
                logger.info(
                    f"Saved {len(member_roles)} sticky roles for leaving member {event.member.id}"
                )

                if (
                    not self.sticky_roles._cleanup_task
                    or self.sticky_roles._cleanup_task.done()
                ):
                    self.sticky_roles._cleanup_task = asyncio.create_task(
                        self.sticky_roles.cleanup_inactive_roles()
                    )

        except Exception as e:
            logger.error(
                f"Error saving sticky roles for member {event.member.id}: {e!r}",
                exc_info=True,
            )

    @interactions.listen("MemberAdd")
    async def on_member_add(self, event: MemberAdd) -> None:
        try:
            sticky_role_ids = await self.sticky_roles.get_sticky_roles(event.member.id)
            if not sticky_role_ids:
                return

            guild = await self.bot.fetch_guild(event.member.guild.id)
            roles_to_add = []

            for role_id in sticky_role_ids:
                try:
                    role = await guild.fetch_role(role_id)
                    if role and not (
                        role.permissions
                        & (
                            interactions.Permissions.ADMINISTRATOR
                            | interactions.Permissions.MANAGE_GUILD
                            | interactions.Permissions.MANAGE_ROLES
                            | interactions.Permissions.MANAGE_CHANNELS
                        )
                        or role.id in self.excluded_role_ids
                        or role.bot_managed
                    ):
                        roles_to_add.append(role)
                except Exception as e:
                    logger.warning(f"Could not fetch role {role_id}: {e!r}")
                    continue

            if roles_to_add:
                try:
                    await event.member.add_roles(roles_to_add)
                    logger.info(
                        f"Restored {len(roles_to_add)} sticky roles for member {event.member.id}: "
                        f"{','.join(str(role.name) for role in roles_to_add)}"
                    )
                    await self.sticky_roles.update_sticky_roles(
                        event.member.id, [role.id for role in roles_to_add]
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to add roles to member {event.member.id}: {e!r}",
                        exc_info=True,
                    )

        except Exception as e:
            logger.error(
                f"Error restoring sticky roles for member {event.member.id}: {e!r}",
                exc_info=True,
            )

    @interactions.listen("MemberUpdate")
    async def on_member_update(self, event: MemberUpdate) -> None:
        try:
            before_roles: set[int] = set(role.id for role in event.before.roles)
            after_roles: set[int] = set(role.id for role in event.after.roles)

            if before_roles == after_roles:
                return

            added = after_roles - before_roles
            removed = before_roles - after_roles

            await self.sticky_roles.update_sticky_roles(event.after.id, [*after_roles])

            logger.info(
                "Updated sticky roles for member {} - Added: {}, Removed: {}".format(
                    event.after.id, added, removed
                )
            )

        except Exception as e:
            logger.error(
                "Error updating sticky roles for member %s: %r",
                event.after.id,
                e,
                exc_info=True,
            )

    # Command groups

    module_base = interactions.SlashCommand(
        name="roles", description="Role management commands"
    )
    module_group_custom: interactions.SlashCommand = module_base.group(
        name="custom", description="Custom roles management"
    )
    module_group_vetting: interactions.SlashCommand = module_base.group(
        name="vetting", description="Vetting management"
    )
    module_group_servant: interactions.SlashCommand = module_base.group(
        name="servant", description="Servants management"
    )
    module_group_penitentiary: interactions.SlashCommand = module_base.group(
        name="penitentiary", description="Penitentiary management"
    )
    module_group_debug: interactions.SlashCommand = module_base.group(
        name="debug", description="Debug commands"
    )
    module_group_reaction: interactions.SlashCommand = module_base.group(
        name="reaction", description="Reaction commands"
    )

    # Reaction commands

    @module_group_reaction.subcommand(
        "start", sub_cmd_description="Configure reaction role"
    )
    @interactions.slash_option(
        name="message",
        description="Message ID or URL",
        opt_type=interactions.OptionType.STRING,
        required=True,
    )
    @interactions.slash_option(
        name="emoji",
        description="Emoji to react with",
        opt_type=interactions.OptionType.STRING,
        required=True,
        autocomplete=True,
    )
    @interactions.slash_option(
        name="role",
        description="Role to assign/remove",
        opt_type=interactions.OptionType.ROLE,
        required=True,
    )
    @interactions.slash_option(
        name="action",
        description="Whether to add or remove the role when reacted",
        opt_type=interactions.OptionType.STRING,
        required=True,
        choices=[
            interactions.SlashCommandChoice(name="Add", value="add"),
            interactions.SlashCommandChoice(name="Remove", value="remove"),
        ],
    )
    @error_handler
    async def configure_reaction_role(
        self,
        ctx: interactions.SlashContext,
        message: str,
        emoji: str,
        role: interactions.Role,
        action: str,
    ) -> None:
        if not next(
            (r for r in ctx.author.roles if r.id == self.config.MINISTER_ROLE_ID), None
        ):
            await self.send_error(
                ctx,
                f"Only the <@&{self.config.MINISTER_ROLE_ID}> can configure reaction roles.",
            )
            return

        try:
            message_id = self.extract_message_id(message)
            if not message_id:
                await self.send_error(ctx, "Invalid message ID/URL.")
                return

            if not emoji:
                await self.send_error(ctx, "Invalid emoji provided.")
                return

            self.reaction_roles.setdefault(message_id, {})
            emoji_key = f":{emoji}:" if emoji.isdigit() else emoji
            self.reaction_roles[message_id][emoji_key] = {
                "role_id": role.id,
                "action": action,
            }

            try:
                channel = await ctx.guild.fetch_channel(ctx.channel_id)
                msg = await channel.fetch_message(int(message_id))

                await msg.add_reaction(emoji)

                for reaction in msg.reactions:
                    if str(reaction.emoji) == emoji:
                        async for user in reaction.users():
                            if not user.bot:
                                try:
                                    member = await ctx.guild.fetch_member(user.id)
                                    if action == "add" and role.id not in [
                                        r.id for r in member.roles
                                    ]:
                                        await member.add_role(role.id)
                                    elif action == "remove" and role.id in [
                                        r.id for r in member.roles
                                    ]:
                                        await member.remove_role(role.id)
                                except Exception as e:
                                    logger.error(
                                        f"Failed to modify role for user {user.id}: {e}",
                                        exc_info=True,
                                    )
                                    continue
                        break

            except Exception as e:
                logger.error(f"Failed to add reaction: {e}", exc_info=True)
                await self.send_error(ctx, "Failed to add reaction to message.")
                return

            await self.model.save_data("reaction_roles.json", dict(self.reaction_roles))

            action_text = "added to" if action == "add" else "removed from"
            await self.send_success(
                ctx,
                f"Successfully configured reaction role:\n- Emoji: {emoji_key}\n- Role: {role.mention} will be {action_text} users when they react",
            )

        except Exception as e:
            logger.error(f"Error configuring reaction role: {e}", exc_info=True)
            await self.send_error(ctx, f"Failed to configure reaction role: {str(e)}")

    @configure_reaction_role.autocomplete("emoji")
    async def autocomplete_emoji(self, ctx: interactions.AutocompleteContext) -> None:
        user_input: str = ctx.input_text.casefold()
        choices: list[interactions.SlashCommandChoice] = []

        emojis = await self.bot.http.get_all_guild_emoji(ctx.guild_id)
        for emoji in emojis:
            if user_input in emoji["name"].casefold():
                choices.append(
                    interactions.SlashCommandChoice(
                        name=f":{emoji['name']}:", value=str(emoji["id"])
                    )
                )

        await ctx.send(choices[:25])

    @interactions.listen("MessageReactionAdd")
    async def on_reaction_add(self, event: MessageReactionAdd) -> None:
        if event.author.id == self.bot.user.id:
            return

        message_id = str(event.message.id)
        emoji = event.emoji

        try:
            reaction_config = self.reaction_roles.get(message_id, {}).get(
                emoji.name
            ) or self.reaction_roles.get(message_id, {}).get(str(emoji))

            if not reaction_config:
                logger.info(
                    f"No reaction config found for message {message_id} and emoji {emoji}"
                )
                return

            if not isinstance(reaction_config, dict):
                logger.error(f"Invalid reaction config format: {reaction_config}")
                return

            role_id = reaction_config.get("role_id")
            action = reaction_config.get("action")
            if not role_id or not action:
                logger.error(f"Missing role_id or action in config: {reaction_config}")
                return

            guild = await self.bot.fetch_guild(event.message.guild.id)
            member = await guild.fetch_member(event.author.id)
            role = await guild.fetch_role(role_id)

            if role is None:
                logger.error(f"Could not find role with ID {role_id}")
                return

            try:
                if action == "add":
                    await member.add_roles([role])
                elif action == "remove":
                    await member.remove_roles([role])
                else:
                    logger.error(f"Invalid action `{action}` in config")
                    return

                logger.info(
                    f"Successfully {'added' if action == 'add' else 'removed'} role {role.id} "
                    f"{'to' if action == 'add' else 'from'} member {member.id} via reaction"
                )
            except Exception as e:
                logger.error(f"Failed to modify roles: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"Failed to handle reaction: {e}", exc_info=True)
            logger.error(f"Full reaction config: {self.reaction_roles}")

    @module_group_reaction.subcommand(
        "stop", sub_cmd_description="Stop monitoring a reaction"
    )
    @interactions.slash_option(
        name="config",
        description="Select reaction configuration to stop",
        opt_type=interactions.OptionType.STRING,
        required=True,
        autocomplete=True,
        argument_name="reaction_config",
    )
    @error_handler
    async def stop_monitoring_reaction(
        self,
        ctx: interactions.SlashContext,
        reaction_config: str,
    ) -> None:
        if not any(r.id == self.config.MINISTER_ROLE_ID for r in ctx.author.roles):
            return await self.send_error(
                ctx,
                f"Only <@&{self.config.MINISTER_ROLE_ID}> can stop reaction monitoring.",
            )

        try:
            message_id, emoji = reaction_config.split(":", 1)
        except ValueError:
            return await self.send_error(ctx, "Invalid reaction config format.")

        if (
            message_id not in self.reaction_roles
            or emoji not in self.reaction_roles[message_id]
        ):
            return await self.send_error(
                ctx, "No reaction monitoring found for this message and emoji."
            )

        del self.reaction_roles[message_id][emoji]
        if not self.reaction_roles[message_id]:
            del self.reaction_roles[message_id]

        await self.model.save_data("reaction_roles.json", self.reaction_roles)
        await self.send_success(
            ctx, f"Successfully stopped monitoring reaction {emoji}"
        )

    @stop_monitoring_reaction.autocomplete("config")
    async def autocomplete_reaction_config(
        self, ctx: interactions.AutocompleteContext
    ) -> None:
        choices: list[interactions.SlashCommandChoice] = []
        focused_value = ctx.input_text.lower()

        for message_id, reactions in self.reaction_roles.items():
            for emoji, config in reactions.items():
                if not isinstance(config, dict):
                    continue

                role_id = config.get("role_id")
                action = config.get("action", "add")

                guild = await self.bot.fetch_guild(ctx.guild_id)
                role = await guild.fetch_role(role_id)
                if role is None:
                    continue

                choice_name = f"{emoji} | {action} {role.name}"
                if focused_value in choice_name.lower():
                    choices.append(
                        interactions.SlashCommandChoice(
                            name=choice_name, value=f"{message_id}:{emoji}"
                        )
                    )

        await ctx.send(choices[:25])

    @staticmethod
    def extract_message_id(message_input: str) -> Optional[str]:
        if message_input.isdigit():
            return message_input

        if "discord.com/channels" in message_input:
            url_parts = message_input.split("/")
            if len(url_parts) > 0:
                return url_parts[-1]

        return None

    @interactions.listen("MessageReactionRemove")
    async def on_reaction_remove(self, event: MessageReactionRemove) -> None:
        if event.author.id == self.bot.user.id:
            return

        message_id = str(event.message.id)
        emoji = str(event.emoji)

        try:
            reaction_config = self.reaction_roles.get(message_id, {}).get(emoji)
            if not reaction_config:
                return

            if not isinstance(reaction_config, dict):
                logger.error(f"Invalid reaction config format: {reaction_config}")
                return

            role_id = reaction_config.get("role_id")
            action = reaction_config.get("action")
            if not role_id or not action:
                return

            guild = await self.bot.fetch_guild(event.message.guild.id)
            member = await guild.fetch_member(event.author.id)
            role = await guild.fetch_role(role_id)

            if role is not None:
                if action == "add":
                    await member.remove_roles([role])
                else:
                    await member.add_roles([role])
            logger.info(
                f"{'Removed' if action == 'add' else 'Added'} role {role.id} "
                f"{'from' if action == 'add' else 'to'} member {member.id} via reaction removal"
            )

        except Exception as e:
            logger.error(f"Failed to handle reaction removal: {e}", exc_info=True)

    # Debug commands

    @module_group_debug.subcommand(
        "delete", sub_cmd_description="Delete files from the extension directory"
    )
    @interactions.slash_option(
        name="type",
        description="Type of files to delete",
        required=True,
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
        argument_name="file_type",
    )
    @interactions.check(interactions.has_id(1268909926458064991))
    async def command_delete(
        self, ctx: interactions.SlashContext, file_type: str
    ) -> None:
        await ctx.defer(ephemeral=True)

        if not os.path.exists(BASE_DIR):
            return await self.send_error(ctx, "Extension directory does not exist.")

        if file_type == "all":
            return await self.send_error(
                ctx, "Cannot delete all files at once for safety reasons."
            )

        file_path = os.path.join(BASE_DIR, file_type)
        if not os.path.isfile(file_path):
            return await self.send_error(
                ctx, f"File `{file_type}` does not exist in the extension directory."
            )

        try:
            os.remove(file_path)
            await ctx.send(f"Successfully deleted file `{file_type}`.")
            logger.info(f"Deleted file {file_type} from extension directory")

        except PermissionError:
            logger.error(f"Permission denied while deleting {file_type}")
            await self.send_error(ctx, "Permission denied while deleting file.")
        except Exception as e:
            logger.error(f"Error deleting {file_type}: {e}", exc_info=True)
            await self.send_error(
                ctx, f"An error occurred while deleting {file_type}: {str(e)}"
            )

    @command_delete.autocomplete("type")
    async def delete_type_autocomplete(
        self, ctx: interactions.AutocompleteContext
    ) -> None:
        choices: list[dict[str, str]] = []

        try:
            if os.path.exists(BASE_DIR):
                files = [
                    f
                    for f in os.listdir(BASE_DIR)
                    if os.path.isfile(os.path.join(BASE_DIR, f))
                    and not f.startswith(".")
                ]

                choices.extend({"name": file, "value": file} for file in sorted(files))
        except PermissionError:
            logger.error("Permission denied while listing files")
            choices = [{"name": "Error: Permission denied", "value": "error"}]
        except Exception as e:
            logger.error(f"Error listing files: {e}", exc_info=True)
            choices = [{"name": f"Error: {str(e)}", "value": "error"}]

        await ctx.send(choices[:25])

    @module_group_debug.subcommand(
        "export", sub_cmd_description="Export files from the extension directory"
    )
    @interactions.slash_option(
        name="type",
        description="Type of files to export",
        required=True,
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
        argument_name="file_type",
    )
    @interactions.slash_default_member_permission(
        interactions.Permissions.ADMINISTRATOR
    )
    async def command_export(
        self, ctx: interactions.SlashContext, file_type: str
    ) -> None:
        await ctx.defer(ephemeral=True)
        filename: str = ""

        if not os.path.exists(BASE_DIR):
            return await self.send_error(ctx, "Extension directory does not exist.")

        if file_type != "all" and not os.path.isfile(os.path.join(BASE_DIR, file_type)):
            return await self.send_error(
                ctx, f"File `{file_type}` does not exist in the extension directory."
            )

        try:
            async with aiofiles.tempfile.NamedTemporaryFile(
                prefix="export_", suffix=".tar.gz", delete=False
            ) as afp:
                filename = afp.name
                base_name = filename[:-7]

                await aioshutil.make_archive(
                    base_name,
                    "gztar",
                    BASE_DIR,
                    "." if file_type == "all" else file_type,
                )

            if not os.path.exists(filename):
                return await self.send_error(ctx, "Failed to create archive file.")

            file_size = os.path.getsize(filename)
            if file_size > 8_388_608:
                return await self.send_error(
                    ctx, "Archive file is too large to send (>8MB)."
                )

            message = (
                "All extension files attached."
                if file_type == "all"
                else f"File `{file_type}` attached."
            )
            await ctx.send(
                message,
                files=[interactions.File(filename)],
            )

        except PermissionError:
            logger.error(f"Permission denied while exporting {file_type}")
            await self.send_error(ctx, "Permission denied while accessing files.")
        except Exception as e:
            logger.error(f"Error exporting {file_type}: {e}", exc_info=True)
            await self.send_error(
                ctx, f"An error occurred while exporting {file_type}: {str(e)}"
            )
        finally:
            if filename and os.path.exists(filename):
                try:
                    os.unlink(filename)
                except Exception as e:
                    logger.error(f"Error cleaning up temp file: {e}", exc_info=True)

    @command_export.autocomplete("type")
    async def export_type_autocomplete(
        self, ctx: interactions.AutocompleteContext
    ) -> None:
        choices: list[dict[str, str]] = [{"name": "All Files", "value": "all"}]

        try:
            if os.path.exists(BASE_DIR):
                files = [
                    f
                    for f in os.listdir(BASE_DIR)
                    if os.path.isfile(os.path.join(BASE_DIR, f))
                    and not f.startswith(".")
                ]

                choices.extend({"name": file, "value": file} for file in sorted(files))
        except PermissionError:
            logger.error("Permission denied while listing files")
            choices = [{"name": "Error: Permission denied", "value": "error"}]
        except Exception as e:
            logger.error(f"Error listing files: {e}", exc_info=True)
            choices = [{"name": f"Error: {str(e)}", "value": "error"}]

        await ctx.send(choices[:25])

    @module_group_debug.subcommand(
        "inactive",
        sub_cmd_description="Convert inactive members to missing members",
    )
    @error_handler
    @interactions.slash_default_member_permission(
        interactions.Permissions.ADMINISTRATOR
    )
    @interactions.max_concurrency(interactions.Buckets.GUILD, 1)
    async def process_inactive_members(self, ctx: interactions.SlashContext) -> None:
        if not ctx.author.guild_permissions & interactions.Permissions.ADMINISTRATOR:
            await self.send_error(
                ctx, "You need Administrator permission to use this command."
            )
            return

        await ctx.defer(ephemeral=True)

        try:
            guild = await self.bot.fetch_guild(self.config.GUILD_ID)
            logger.info(f"Processing inactive members for guild {guild.id}")

            roles = await asyncio.gather(
                guild.fetch_role(self.config.TEMPORARY_ROLE_ID),
                guild.fetch_role(self.config.ELECTORAL_ROLE_ID),
                guild.fetch_role(self.config.MISSING_ROLE_ID),
            )
            temp_role, electoral_role, missing_role = roles
            logger.info(
                f"Fetched roles - temp: {temp_role.id}, electoral: {electoral_role.id}, missing: {missing_role.id}"
            )

            if not all(roles):
                logger.error("One or more required roles not found")
                return await self.send_error(
                    ctx,
                    f"Required roles could not be found. Please verify that <@&{self.config.TEMPORARY_ROLE_ID}>, <@&{self.config.ELECTORAL_ROLE_ID}>, and <@&{self.config.MISSING_ROLE_ID}> exist in the server.",
                )

            temp_cutoff = datetime.now(timezone.utc) - timedelta(days=15)
            electoral_cutoff = datetime.now(timezone.utc) - timedelta(days=30)
            converted_members = []

            text_channels = {
                channel
                for channel in guild.channels
                if isinstance(
                    channel, (interactions.GuildText, interactions.ThreadChannel)
                )
                and channel.type
                in (
                    interactions.ChannelType.GUILD_TEXT,
                    interactions.ChannelType.GUILD_PUBLIC_THREAD,
                    interactions.ChannelType.GUILD_PRIVATE_THREAD,
                )
            }
            logger.info(f"Found {len(text_channels)} text channels to scan")

            members_to_check = temp_role.members + electoral_role.members
            total_members = len(members_to_check)
            logger.info(f"Total members to check: {total_members}")
            processed = skipped = 0

            MEMBER_EDIT_RATE = 9
            MESSAGE_RATE = 4
            BATCH_SIZE = min(MEMBER_EDIT_RATE, total_members)
            REPORT_THRESHOLD = 5

            member_edit_sem = asyncio.Semaphore(MEMBER_EDIT_RATE)
            message_sem = asyncio.Semaphore(MESSAGE_RATE)

            async def process_member(member: interactions.Member):
                nonlocal processed, skipped
                cutoff = temp_cutoff if temp_role in member.roles else electoral_cutoff
                logger.debug(f"Processing member {member.id} with cutoff {cutoff}")

                if member.joined_at and member.joined_at > cutoff:
                    logger.debug(f"Member {member.id} skipped - within grace period")
                    skipped += 1
                    processed += 1
                    return

                try:
                    cutoff_ts = cutoff.timestamp()
                    cutoff_ms = int(cutoff_ts * 1000)
                    member_id = member.id
                    channels = tuple(text_channels)
                    logger.debug(
                        f"Checking activity for member {member_id} across {len(channels)} channels"
                    )

                    is_active = False
                    for chunk_start in range(0, len(channels), 3):
                        chunk = channels[chunk_start : chunk_start + 3]
                        logger.debug(f"Processing channel chunk {chunk_start//3 + 1}")

                        for channel in chunk:
                            try:
                                limit, max_limit = 50, 500
                                last_msg_id = None
                                logger.debug(
                                    f"Scanning channel {channel.id} for member {member_id}"
                                )

                                while limit <= max_limit:
                                    history = channel.history(
                                        limit=limit, before=last_msg_id, after=cutoff_ms
                                    )

                                    async for message in ChannelHistoryIteractor(
                                        history
                                    ):
                                        msg_author = await message.author
                                        msg_timestamp = message.timestamp
                                        msg_edited = message.edited_timestamp
                                        msg_id = message.id

                                        if (
                                            msg_author.id == member_id
                                            and (msg_edited or msg_timestamp) > cutoff
                                        ):
                                            logger.info(
                                                f"Found activity for member {member_id} in channel {channel.id}"
                                            )
                                            is_active = True
                                            break
                                        last_msg_id = msg_id

                                    if not last_msg_id or is_active:
                                        break

                                    limit <<= 1

                                await asyncio.sleep(1.0)

                            except Exception as e:
                                logger.debug(
                                    f"Error scanning channel {channel.id}: {e}"
                                )
                                continue

                        if is_active:
                            break
                        await asyncio.sleep(2.0)

                    if not is_active:
                        new_roles = {
                            role.id
                            for role in member.roles
                            if role not in (temp_role, electoral_role)
                        } | {missing_role.id}

                        async with member_edit_sem:
                            logger.info(
                                f"Updating roles for inactive member {member.id}"
                            )
                            await member.edit(
                                roles=list(new_roles),
                                reason="Converting inactive member",
                            )
                            await asyncio.sleep(1.0)

                        converted_members.append(member.mention)
                        logger.info(
                            f"Successfully updated roles for member {member.id}"
                        )

                        if len(converted_members) >= REPORT_THRESHOLD:
                            async with message_sem:
                                logger.info(
                                    f"Sending progress report - processed: {processed}/{total_members}"
                                )
                                await self.send_success(
                                    ctx,
                                    f"- Processed: {processed}/{total_members} members\n"
                                    f"- Skipped (grace period): {skipped}\n"
                                    f"- Recently converted members: {', '.join(converted_members)}",
                                )
                                await asyncio.sleep(1.0)
                            converted_members.clear()

                    processed += 1

                except Exception as e:
                    logger.error(
                        f"Error processing member {member.id}: {e}", exc_info=True
                    )

            for i in range(0, total_members, BATCH_SIZE):
                batch = members_to_check[i : i + BATCH_SIZE]
                logger.info(
                    f"Processing batch {i//BATCH_SIZE + 1} with {len(batch)} members"
                )
                await asyncio.gather(*(process_member(m) for m in batch))
                await asyncio.sleep(10.0)

                if converted_members:
                    async with message_sem:
                        logger.info(
                            f"Sending batch progress report - processed: {processed}/{total_members}"
                        )
                        await self.send_success(
                            ctx,
                            f"Batch progress:\n"
                            f"- Processed: {processed}/{total_members}\n"
                            f"- Skipped: {skipped}\n"
                            f"- Recent conversions: {', '.join(converted_members)}",
                        )
                        await asyncio.sleep(1.0)
                    converted_members.clear()

            logger.info(
                f"Process completed - Total: {total_members}, Skipped: {skipped}, Converted: {processed - skipped}"
            )
            await self.send_success(
                ctx,
                f"Process completed:\n"
                f"- Total processed: {total_members}\n"
                f"- Skipped: {skipped}\n"
                f"- Converted: {processed - skipped}",
            )

        except Exception as e:
            logger.error(f"Error in process_inactive_members: {e}", exc_info=True)
            await self.send_error(
                ctx, f"An error occurred while converting members:\n```py\n{str(e)}```"
            )

    @module_group_debug.subcommand(
        "conflicts", sub_cmd_description="Check and resolve role conflicts"
    )
    @error_handler
    @interactions.slash_default_member_permission(
        interactions.Permissions.ADMINISTRATOR
    )
    @interactions.max_concurrency(interactions.Buckets.GUILD, 1)
    async def check_role_conflicts(self, ctx: interactions.SlashContext) -> None:
        ROLE_PRIORITIES = {
            self.config.MISSING_ROLE_ID: 1,
            self.config.INCARCERATED_ROLE_ID: 2,
            self.config.ELECTORAL_ROLE_ID: 3,
            self.config.APPROVED_ROLE_ID: 4,
            self.config.TEMPORARY_ROLE_ID: 5,
        }
        BATCH_SIZE = 8
        ROLE_CHANGE_INTERVAL = 1.0
        REPORT_INTERVAL = 5.0

        if not ctx.author.guild_permissions & interactions.Permissions.ADMINISTRATOR:
            await self.send_error(
                ctx,
                "You need Administrator permission to check role conflicts.",
            )
            return

        await ctx.defer(ephemeral=True)

        try:
            processed, conflicts = 0, 0
            log_buffer = []
            last_report_time = time.monotonic()

            member_chunks = [
                list(ctx.guild.members)[i : i + BATCH_SIZE]
                for i in range(0, len(ctx.guild.members), BATCH_SIZE)
            ]

            for chunk in member_chunks:
                chunk_start = time.monotonic()

                for member in chunk:
                    try:
                        member_role_ids = {role.id for role in member.roles}
                        priority_roles = {
                            role_id: prio
                            for role_id, prio in ROLE_PRIORITIES.items()
                            if role_id in member_role_ids
                        }

                        if len(priority_roles) > 1:
                            highest_prio = min(priority_roles.values())
                            roles_to_remove = {
                                role_id
                                for role_id, prio in priority_roles.items()
                                if prio > highest_prio
                            }

                            if roles_to_remove:
                                roles_to_keep = [
                                    role.id
                                    for role in member.roles
                                    if role.id not in roles_to_remove
                                ]
                                await member.edit(
                                    roles=roles_to_keep,
                                    reason="Resolving role priority conflicts",
                                )
                                await asyncio.sleep(ROLE_CHANGE_INTERVAL)

                                conflicts += 1
                                log_buffer.append(
                                    f"- Member: {member.mention}\n- Removed roles: {len(roles_to_remove)}"
                                )

                                current_time = time.monotonic()
                                if current_time - last_report_time >= REPORT_INTERVAL:
                                    if log_buffer:
                                        await self.send_success(
                                            None, "\n".join(log_buffer)
                                        )
                                        log_buffer = []
                                        last_report_time = current_time
                                        await asyncio.sleep(ROLE_CHANGE_INTERVAL)

                    except Exception as e:
                        logger.error(
                            f"Error processing member {member.id}: {str(e)}\n"
                            f"{traceback.format_exc()}"
                        )

                    processed += 1

                elapsed = time.monotonic() - chunk_start
                if elapsed < REPORT_INTERVAL:
                    await asyncio.sleep(REPORT_INTERVAL - elapsed)

            if log_buffer:
                await asyncio.sleep(ROLE_CHANGE_INTERVAL)
                await self.send_success(None, "\n".join(log_buffer))

            if conflicts:
                await asyncio.sleep(ROLE_CHANGE_INTERVAL)
                await self.send_success(
                    None,
                    f"- Total members processed: {processed}\n- Conflicts resolved: {conflicts}",
                )

            logger.info(
                f"- Members processed: {processed}\n- Conflicts resolved: {conflicts}"
            )

        except Exception as e:
            error_msg = f"Critical error in role conflict check:\n{str(e)}"
            logger.error(f"{error_msg}\n{traceback.format_exc()}")
            await self.send_error(None, error_msg, log_to_channel=True)

    @module_group_debug.subcommand(
        "view", sub_cmd_description="View configuration files"
    )
    @interactions.slash_option(
        name="config",
        description="Configuration type to view",
        opt_type=interactions.OptionType.STRING,
        required=True,
        choices=[
            *(
                interactions.SlashCommandChoice(name=n, value=v)
                for n, v in {
                    "Vetting Roles": "vetting",
                    "Custom Roles": "custom",
                    "Incarcerated Members": "incarcerated",
                    "Stats": "stats",
                    "Config": "dynamic",
                }.items()
            )
        ],
    )
    @error_handler
    async def view_config(self, ctx: interactions.SlashContext, config: str) -> None:
        await ctx.defer(ephemeral=True)
        try:
            if not (config_data := await self._get_config_data(config)):
                return await self.send_error(
                    ctx,
                    f"Unable to find data for the `{config}` configuration. Please verify that the configuration exists and try again. Try using a different configuration type from the dropdown menu.",
                )

            if not (embeds := await self._generate_embeds(config, config_data)):
                return await self.send_error(
                    ctx,
                    f"The `{config}` configuration exists but contains no displayable data. This may indicate an empty or corrupted configuration file.",
                )

            paginator = Paginator(
                client=self.bot,
                pages=embeds,
                timeout_interval=120,
                show_callback_button=True,
                show_select_menu=True,
                show_back_button=True,
                show_next_button=True,
                show_first_button=True,
                show_last_button=True,
                wrong_user_message="This leaderboard can only be controlled by the user who requested it.",
                hide_buttons_on_stop=True,
            )

            await paginator.send(ctx)

        except Exception as e:
            logger.error(f"Error in view_config: {e}\n{traceback.format_exc()}")
            await self.send_error(
                ctx,
                f"An unexpected error occurred while viewing the configuration: {str(e)}",
            )

    async def _get_config_data(self, config: str) -> Optional[Any]:
        return (
            self.limit_config
            if config == "dynamic"
            else (
                await self.model.load_data(
                    *next(
                        (
                            (f, t)
                            for c, (f, t) in {
                                "vetting": ("vetting.json", Data),
                                "custom": ("custom.json", dict),
                                "incarcerated": ("incarcerated_members.json", dict),
                                "stats": ("stats.json", dict),
                                "reaction_roles": ("reaction_roles.json", dict),
                            }.items()
                            if c == config
                        ),
                        (None, None),
                    )
                )
                if config != "dynamic"
                else None
            )
        )

    async def _generate_embeds(
        self, config: str, config_data: Any
    ) -> List[interactions.Embed]:
        embeds: List[interactions.Embed] = []
        current_embed = await self.create_embed(
            title=f"{config.title()} Configuration Details",
            description="Below are the detailed settings and configurations.",
        )
        field_count = 0
        max_fields = 25
        total_chars = len(current_embed.title or "") + len(
            current_embed.description or ""
        )

        async def add_field(name: str, value: str, inline: bool = True) -> None:
            nonlocal current_embed, field_count, total_chars
            name = name[:256]

            if len(value) > 1024:
                chunks = [value[i : i + 1024] for i in range(0, len(value), 1024)]
                for i, chunk in enumerate(chunks):
                    field_name = f"{name} (Part {i+1})"[:256]
                    field_value = (
                        chunk or "*No data is currently available for this field*"
                    )

                    field_chars = len(field_name) + len(field_value)
                    if total_chars + field_chars > 6000 or field_count >= max_fields:
                        embeds.append(current_embed)
                        current_embed = await self.create_embed(
                            title=f"{config.title()} Configuration Details (Page {len(embeds) + 2})",
                            description="Continued configuration details.",
                        )
                        field_count = 0
                        total_chars = len(current_embed.title or "") + len(
                            current_embed.description or ""
                        )

                    current_embed.add_field(
                        name=field_name,
                        value=field_value,
                        inline=inline,
                    )
                    field_count += 1
                    total_chars += field_chars
            else:
                field_name = name[:256]
                field_value = (
                    value[:1024] or "*No data is currently available for this field*"
                )
                field_chars = len(field_name) + len(field_value)

                if total_chars + field_chars > 6000 or field_count >= max_fields:
                    embeds.append(current_embed)
                    current_embed = await self.create_embed(
                        title=f"{config.title()} Configuration Details (Page {len(embeds) + 2})",
                        description="Continued configuration details.",
                    )
                    field_count = 0
                    total_chars = len(current_embed.title or "") + len(
                        current_embed.description or ""
                    )

                current_embed.add_field(
                    name=field_name,
                    value=field_value,
                    inline=inline,
                )
                field_count += 1
                total_chars += field_chars

        match config:
            case "vetting":
                overview = [
                    f"**{category.title()}**: {len(roles)} configured roles"
                    for category, roles in config_data.assigned_roles.items()
                    if category in ("ideology", "domicile", "status")
                ]

                await add_field(
                    "Configuration Overview",
                    "\n".join(overview)
                    or "*No roles have been configured yet. Use the configuration commands to set up roles.*",
                )

                for category, roles in config_data.assigned_roles.items():
                    await add_field(
                        f"{category.title()} Configured Roles",
                        "\n".join(
                            f"- <@&{role_id}> (`{role}`)"
                            for role, role_id in sorted(roles.items())
                        )
                        or "*No roles have been configured for this category yet*",
                    )

                for category, assignable_roles in config_data.assignable_roles.items():
                    await add_field(
                        f"Available {category.title()} Roles",
                        "\n".join(
                            f"- `{role}` (Available for assignment)"
                            for role in sorted(assignable_roles)
                        )
                        or "*No assignable roles configured for this category*",
                    )

            case "custom":
                for role_name, members in config_data.items():
                    await add_field(
                        f"Custom Role: {role_name}",
                        "\n".join(f"- <@{member_id}>" for member_id in members)
                        or "*No members currently have this role*",
                    )

            case "incarcerated":
                for member_id, info in config_data.items():
                    try:
                        release_time = int(float(info["release_time"]))
                        roles_str = (
                            ", ".join(
                                f"<@&{role_id}>"
                                for role_id in info.get("original_roles", [])
                            )
                            or "*No previous roles recorded*"
                        )
                        await add_field(
                            f"Restricted Member: <@{member_id}>",
                            f"- Release Scheduled: <t:{release_time}:F>\n- Previous Roles: {roles_str}",
                        )
                    except (ValueError, KeyError) as e:
                        logger.error(f"Error processing member {member_id}: {str(e)}")
                        continue

            case "stats":
                for member_id, stats in config_data.items():
                    formatted_stats = []

                    for key, value in stats.items():
                        match key:
                            case "message_timestamps":
                                formatted_stats.append(
                                    f"- **Total Messages**: {len(value)}"
                                )

                            case "last_message":
                                formatted_stats.append(
                                    f"- **Last Message Content**: {str(value)[:50]}"
                                )

                            case "last_threshold_adjustment":
                                formatted_stats.append(
                                    f"- **Last Threshold Update**: <t:{int(float(value))}:R>"
                                )

                            case "repetition" | "digit_ratio" | "entropy" | "feedback":
                                formatted_stats.append(
                                    f"- **{key.replace('_', ' ').title()}**: {'True' if value else 'False'}"
                                )

                            case (
                                "MESSAGE_WINDOW_SECONDS"
                                | "MAX_REPEATED_MESSAGES"
                                | "DIGIT_RATIO_THRESHOLD"
                                | "MIN_MESSAGE_ENTROPY"
                            ):
                                if isinstance(value, (int, float)):
                                    formatted_stats.append(
                                        f"- **{key.replace('_', ' ').title()}**: {value:.2f}"
                                    )

                            case _:
                                if isinstance(value, (int, float)):
                                    formatted_stats.append(
                                        f"- **{key.replace('_', ' ').title()}**: {value:.2f}"
                                    )
                                else:
                                    formatted_stats.append(
                                        f"- **{key.replace('_', ' ').title()}**: {value}"
                                    )

                    await add_field(
                        f"Member Activity: <@{member_id}>",
                        "\n".join(formatted_stats)
                        or "*No activity statistics available for this member*",
                    )

            case "dynamic":
                for config_name, value in config_data.items():
                    await add_field(f"{config_name}", f"```py\n{value}```")

        if field_count:
            embeds.append(current_embed)

        return embeds

    # Custom roles commands

    @module_group_custom.subcommand(
        "configure", sub_cmd_description="Configure custom roles"
    )
    @interactions.slash_option(
        name="roles",
        description="Enter role names separated by commas",
        opt_type=interactions.OptionType.STRING,
        required=True,
    )
    @interactions.slash_option(
        name="action",
        description="Choose whether to add or remove the specified roles",
        opt_type=interactions.OptionType.STRING,
        required=True,
        choices=[
            interactions.SlashCommandChoice(name="Add", value="add"),
            interactions.SlashCommandChoice(name="Remove", value="remove"),
        ],
    )
    @error_handler
    async def configure_custom_roles(
        self, ctx: interactions.SlashContext, roles: str, action: str
    ) -> None:
        if not self.validate_custom_permissions(ctx):
            return await self.send_error(
                ctx,
                "You don't have sufficient permissions to configure custom roles.",
            )
        role_set = {role.strip() for role in roles.split(",")}
        updated_roles = (
            {role for role in role_set if role not in self.custom_roles}
            if action == "add"
            else {role for role in role_set if role in self.custom_roles}
        )

        if action == "add":
            self.custom_roles.update({role: set() for role in updated_roles})
        else:
            for role in updated_roles:
                self.custom_roles.pop(role, None)

        if updated_roles:
            await self.save_custom_roles()
            await self.send_success(
                ctx,
                f"{'Added to' if action == 'add' else 'Removed from'} custom roles: `{', '.join(updated_roles)}`.",
            )
        else:
            await self.send_error(
                ctx,
                f"No changes were made because the specified roles {'already exist' if action == 'add' else '''don't exist'''}. Please check the role names and try again.",
            )

    @interactions.user_context_menu(name="Custom Roles")
    @error_handler
    async def custom_roles_context_menu(
        self, ctx: interactions.ContextMenuContext
    ) -> None:
        try:
            logger.info(f"Context menu triggered for user: {ctx.target.id}")

            if not self.validate_custom_permissions(ctx):
                logger.warning(
                    f"User {ctx.author.id} lacks permission for custom roles menu"
                )
                return await self.send_error(
                    ctx,
                    "You don't have sufficient permissions to manage custom roles.",
                )

            components = interactions.StringSelectMenu(
                custom_id=f"manage_roles_menu_{(member := ctx.target).id}",
                placeholder="Select action (Add/Remove roles)",
                *(
                    interactions.StringSelectOption(label=label, value=value)
                    for label, value in (("Add", "add"), ("Remove", "remove"))
                ),
            )

            await ctx.send(
                f"Please select whether you want to add or remove custom roles for {member.mention}. After selecting an action, you'll be able to choose specific roles.",
                components=components,
            )
            logger.info(f"Context menu response sent for user: {ctx.target.id}")

        except Exception as e:
            logger.error(f"Error in custom_roles_context_menu: {str(e)}")
            logger.error(traceback.format_exc())
            await self.send_error(
                ctx,
                f"An unexpected error occurred while managing custom roles. Our team has been notified: {str(e)}",
            )

    @module_group_custom.subcommand(
        "mention", sub_cmd_description="Mention custom role members"
    )
    @interactions.slash_option(
        name="roles",
        description="Roles",
        opt_type=interactions.OptionType.STRING,
        required=True,
        autocomplete=True,
    )
    @error_handler
    async def mention_custom_roles(
        self, ctx: interactions.SlashContext, roles: str
    ) -> None:
        if not self.validate_custom_permissions(ctx):
            return await self.send_error(
                ctx,
                "You don't have permission to use this command.",
            )

        role_set: set[str] = set(map(str.strip, roles.split(",")))
        custom_roles_set: set[str] = set(self.custom_roles)

        found_roles: set[str] = role_set & custom_roles_set
        not_found_roles: set[str] = role_set - custom_roles_set

        mentioned_users: set[str] = set(
            f"<@{uid}>" for role in found_roles for uid in self.custom_roles[role]
        )

        if mentioned_users:
            await ctx.send(
                f"Found users with the following roles: `{', '.join(found_roles)}`. Here are the users: {' '.join(mentioned_users)}"
            )
            return

        if not_found_roles:
            await self.send_error(
                ctx,
                f"Unable to find the following roles: {', '.join(not_found_roles)}. Please check the role names and try again. You can use the autocomplete feature to see available roles.",
            )
            return

        await self.send_error(
            ctx,
            f"No users currently have the roles: `{roles}`. The roles exist, but no users are assigned to them at the moment.",
        )

    @mention_custom_roles.autocomplete("roles")
    async def autocomplete_custom_roles(
        self, ctx: interactions.AutocompleteContext
    ) -> None:
        user_input: str = ctx.input_text.lower()
        choices = (
            interactions.SlashCommandChoice(name=role, value=role)
            for role in self.custom_roles
            if user_input in role.lower()
        )
        await ctx.send(tuple(itertools.islice(choices, 25)))

    # Vetting commands

    @module_group_vetting.subcommand(
        "toggle",
        sub_cmd_description="Configure message monitoring and validation settings",
    )
    @interactions.slash_option(
        name="type",
        argument_name="setting_type",
        description="Type of setting to configure",
        opt_type=interactions.OptionType.STRING,
        required=True,
        choices=[
            *(
                interactions.SlashCommandChoice(
                    name=n.replace("_", " ").title(), value=v
                )
                for n, v in (
                    ("Message Monitoring", "monitoring"),
                    ("Message Repetition", "repetition"),
                    ("Digit Ratio", "digit_ratio"),
                    ("Message Entropy", "entropy"),
                    ("Feedback System", "feedback"),
                )
            )
        ],
    )
    @interactions.slash_option(
        name="state",
        description="Enable or disable this setting",
        opt_type=interactions.OptionType.STRING,
        required=True,
        choices=[
            *(
                interactions.SlashCommandChoice(name=n, value=v)
                for n, v in (("Enable", "enable"), ("Disable", "disable"))
            )
        ],
    )
    @error_handler
    async def toggle_settings(
        self, ctx: interactions.SlashContext, setting_type: str, state: str
    ) -> None:
        if not self.validate_vetting_permissions(ctx):
            return await self.send_error(
                ctx, "You do not have the required permissions to use this command."
            )

        enabled = state == "enable"

        if setting_type == "monitoring":
            self.message_monitoring_enabled = enabled
            setting_name = "Message monitoring"
        else:
            self.validation_flags[setting_type] = enabled
            setting_name = setting_type.replace("_", " ").title()

        await self.send_success(
            ctx,
            f"`{setting_name}` has been {'enabled' if enabled else 'disabled'}.",
        )

    @module_group_vetting.subcommand(
        "assign", sub_cmd_description="Add roles to a member"
    )
    @interactions.slash_option(
        name="member",
        description="Member",
        required=True,
        opt_type=interactions.OptionType.USER,
    )
    @interactions.slash_option(
        name="ideology",
        description="倾向",
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
    )
    @interactions.slash_option(
        name="domicile",
        description="区位",
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
    )
    @interactions.slash_option(
        name="status",
        description="民权",
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
    )
    @interactions.slash_option(
        name="others",
        description="其他",
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
    )
    @error_handler
    async def assign_vetting_roles(
        self,
        ctx: interactions.SlashContext,
        member: interactions.Member,
        ideology: Optional[str] = None,
        domicile: Optional[str] = None,
        status: Optional[str] = None,
        others: Optional[str] = None,
    ):
        await self.update_vetting_roles(
            ctx, member, Action.ADD, ideology, domicile, status, others
        )

    @module_group_vetting.subcommand(
        "remove", sub_cmd_description="Remove roles from a member"
    )
    @interactions.slash_option(
        name="member",
        description="Member",
        required=True,
        opt_type=interactions.OptionType.USER,
    )
    @interactions.slash_option(
        name="ideology",
        description="倾向",
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
    )
    @interactions.slash_option(
        name="domicile",
        description="区位",
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
    )
    @interactions.slash_option(
        name="status",
        description="民权",
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
    )
    @interactions.slash_option(
        name="others",
        description="其他",
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
    )
    @error_handler
    async def remove_vetting_roles(
        self,
        ctx: interactions.SlashContext,
        member: interactions.Member,
        ideology: Optional[str] = None,
        domicile: Optional[str] = None,
        status: Optional[str] = None,
        others: Optional[str] = None,
    ):
        await self.update_vetting_roles(
            ctx, member, Action.REMOVE, ideology, domicile, status, others
        )

    async def update_vetting_roles(
        self,
        ctx: interactions.SlashContext,
        member: interactions.Member,
        action: Action,
        ideology: Optional[str] = None,
        domicile: Optional[str] = None,
        status: Optional[str] = None,
        others: Optional[str] = None,
    ) -> None:
        kwargs = dict(
            zip(
                ("ideology", "domicile", "status", "others"),
                (ideology, domicile, status, others),
            )
        )
        role_ids = self.get_role_ids_to_assign(
            dict(
                filter(
                    lambda x: isinstance(x, tuple) and x[1] is not None, kwargs.items()
                )
            )
        )

        if not self.validate_vetting_permissions(ctx):
            await self.send_error(
                ctx, "You do not have the required permissions to use this command."
            )
            return

        if not self.validate_vetting_permissions_with_roles(ctx, role_ids):
            await self.send_error(
                ctx,
                "Some of the roles you're trying to manage are restricted. You can only manage roles that are within your permission level.",
            )
            return

        if not role_ids:
            role_types = tuple(k for k, v in kwargs.items() if v is not None)
            await self.send_error(
                ctx,
                f"No valid roles found to {action.value.lower()}. Please specify at least one valid role type ({', '.join(role_types) if role_types else 'ideology, domicile, status, or others'}).",
            )
            return

        if action == Action.ADD and await self.check_role_assignment_conflicts(
            ctx, member, role_ids
        ):
            return
        if action == Action.ADD:
            await self.assign_roles_to_member(ctx, member, list(role_ids))
        else:
            await self.remove_roles_from_member(ctx, member, role_ids)

    @assign_vetting_roles.autocomplete("ideology")
    async def autocomplete_ideology_assign(self, ctx: interactions.AutocompleteContext):
        await self.autocomplete_vetting_role(ctx, "ideology")

    @assign_vetting_roles.autocomplete("domicile")
    async def autocomplete_domicile_assign(self, ctx: interactions.AutocompleteContext):
        await self.autocomplete_vetting_role(ctx, "domicile")

    @assign_vetting_roles.autocomplete("status")
    async def autocomplete_status_assign(self, ctx: interactions.AutocompleteContext):
        await self.autocomplete_vetting_role(ctx, "status")

    @assign_vetting_roles.autocomplete("others")
    async def autocomplete_others_assign(self, ctx: interactions.AutocompleteContext):
        await self.autocomplete_vetting_role(ctx, "others")

    @remove_vetting_roles.autocomplete("ideology")
    async def autocomplete_ideology_remove(self, ctx: interactions.AutocompleteContext):
        await self.autocomplete_vetting_role(ctx, "ideology")

    @remove_vetting_roles.autocomplete("domicile")
    async def autocomplete_domicile_remove(self, ctx: interactions.AutocompleteContext):
        await self.autocomplete_vetting_role(ctx, "domicile")

    @remove_vetting_roles.autocomplete("status")
    async def autocomplete_status_remove(self, ctx: interactions.AutocompleteContext):
        await self.autocomplete_vetting_role(ctx, "status")

    @remove_vetting_roles.autocomplete("others")
    async def autocomplete_others_remove(self, ctx: interactions.AutocompleteContext):
        await self.autocomplete_vetting_role(ctx, "others")

    async def autocomplete_vetting_role(
        self, ctx: interactions.AutocompleteContext, role_category: str
    ) -> None:
        if not (roles := getattr(self, "vetting_roles", None)) or role_category not in (
            assigned := roles.assigned_roles
        ):
            await ctx.send([])
            return

        user_input = ctx.input_text.casefold()
        await ctx.send(
            tuple(
                interactions.SlashCommandChoice(name=name, value=name)
                for name in itertools.islice(
                    filter(
                        lambda x: user_input in x.casefold(), assigned[role_category]
                    ),
                    25,
                )
            )
        )

    def get_role_ids_to_assign(self, kwargs: Dict[str, str]) -> Set[int]:
        filtered_items: List[Tuple[str, str]] = [
            (k, v) for k, v in kwargs.items() if isinstance(v, str) and v is not None
        ]
        return {
            role_id
            for param, value in filtered_items
            if value
            and (role_id := self.vetting_roles.assigned_roles.get(param, {}).get(value))
        }

    async def assign_roles_to_member(
        self,
        ctx: interactions.SlashContext,
        member: interactions.Member,
        role_ids_to_add: List[int],
    ) -> None:
        roles_to_add: List[interactions.Role] = [
            *filter(None, (ctx.guild.get_role(rid) for rid in role_ids_to_add))
        ]

        if not roles_to_add:
            return await self.send_error(
                ctx,
                "No valid roles were found to add. Please check that the role IDs are correct and that the bot has permission to manage these roles.",
            )

        try:
            await member.add_roles(roles_to_add)
            await self.send_success(
                ctx,
                f"Moderator {ctx.author.mention} added roles to {member.mention}: {', '.join(r.name for r in roles_to_add)}.",
            )
        except Exception as e:
            await self.send_error(
                ctx,
                f"Failed to add roles due to the following error: {e!s}. This may be due to missing permissions or role hierarchy issues. Please ensure the bot's role is higher than the roles being assigned.",
            )

    async def remove_roles_from_member(
        self,
        ctx: interactions.SlashContext,
        member: interactions.Member,
        role_ids_to_remove: Set[int],
    ) -> None:
        roles_to_remove: frozenset = frozenset(
            role for role in ctx.guild.roles if role.id in role_ids_to_remove
        )

        if not roles_to_remove:
            await self.send_error(ctx, "No valid roles were found to remove.")
            return

        try:
            role_names: str = ", ".join(map(lambda r: f"`{r.name}`", roles_to_remove))

            await member.remove_roles([*roles_to_remove])
            await self.send_success(
                ctx,
                f"Moderator {ctx.author.mention} removed roles from {member.mention}: {role_names}.",
            )

        except Exception as e:
            error_message: str = str(e)
            await self.send_error(
                ctx,
                f"Failed to remove roles due to the following error: {error_message}.",
            )
            logger.exception(
                "Failed to remove roles from member %d (%s): %s",
                member.id,
                member.user.username,
                error_message,
            )

    @interactions.component_callback("approve")
    @error_handler
    async def on_approve_member(
        self, ctx: interactions.ComponentContext
    ) -> Optional[str]:
        return await self.process_approval_status_change(ctx, Status.APPROVED)

    @interactions.component_callback("reject")
    @error_handler
    async def on_reject_member(
        self, ctx: interactions.ComponentContext
    ) -> Optional[str]:
        return await self.process_approval_status_change(ctx, Status.REJECTED)

    @asynccontextmanager
    async def member_lock(self, member_id: int) -> AsyncGenerator[None, None]:
        now = datetime.now(timezone.utc)
        lock_info = self.member_role_locks.setdefault(
            member_id, {"lock": asyncio.Lock(), "last_used": now}
        )
        lock_info["last_used"] = now
        try:
            if isinstance(lock_info["lock"], asyncio.Lock):
                async with lock_info["lock"]:
                    yield
            else:
                yield
        finally:
            current_time: datetime = datetime.now(timezone.utc)
            self.member_role_locks = {
                mid: info
                for mid, info in self.member_role_locks.items()
                if isinstance(info["lock"], asyncio.Lock)
                and not info["lock"].locked()
                and isinstance(info["last_used"], datetime)
                and (current_time - cast(datetime, info["last_used"])).total_seconds()
                <= 3600
            }

    async def process_approval_status_change(
        self, ctx: interactions.ComponentContext, status: Status
    ) -> Optional[str]:
        if not await self.validate_context(ctx):
            return None

        thread = await self.bot.fetch_channel(ctx.channel_id)
        if not isinstance(thread, interactions.GuildPublicThread):
            raise ValueError("Invalid context: Must be used in threads")

        guild = await self.bot.fetch_guild(thread.guild.id)
        member = await guild.fetch_member(thread.owner_id)
        if not member:
            await self.send_error(ctx, "Member not found in server")
            return None

        async with self.member_lock(member.id):
            try:
                roles = await self.fetch_required_roles(guild)
                if not self.validate_roles(roles):
                    await self.send_error(ctx, "Required roles configuration invalid.")
                    return None

                thread_approvals = self.get_thread_approvals(thread.id)
                if not await self.validate_reviewer(ctx, thread_approvals):
                    return None

                handler = {
                    Status.APPROVED: self.process_approval,
                    Status.REJECTED: self.process_rejection,
                }[status]

                return await handler(
                    ctx=ctx,
                    member=member,
                    roles=roles,
                    current_roles=frozenset(r.id for r in member.roles),
                    thread_approvals=thread_approvals,
                    thread=thread,
                )

            except KeyError:
                await self.send_error(ctx, "Invalid status.")
                return None
            except Exception as e:
                logger.exception(f"Status change failed: {e}", exc_info=True)
                await self.send_error(ctx, "Processing error occurred.")
                return None
            finally:
                await self.update_review_components(ctx, thread)

    async def validate_context(self, ctx: interactions.ComponentContext) -> bool:
        if not self.validate_vetting_permissions(ctx):
            await self.send_error(ctx, "Insufficient permissions")
            return False
        return True

    async def validate_reviewer(
        self, ctx: interactions.ComponentContext, thread_approvals: Approval
    ) -> bool:
        if ctx.author.id in thread_approvals.reviewers:
            await self.send_error(ctx, "Duplicate vote detected")
            return False
        return True

    @staticmethod
    def validate_roles(roles: Dict[str, Optional[interactions.Role]]) -> bool:
        return all(roles.values())

    async def update_review_components(
        self, ctx: interactions.ComponentContext, thread: interactions.GuildPublicThread
    ) -> None:
        try:
            embed, buttons = await self.create_review_components(thread)
            await ctx.message.edit(embed=embed, components=buttons)
        except Exception as e:
            logger.error(f"Failed to update message: {repr(e)}")

    async def process_approval(
        self,
        ctx: interactions.ComponentContext,
        member: interactions.Member,
        roles: Dict[str, interactions.Role],
        current_roles: FrozenSet[int],
        thread_approvals: Approval,
        thread: interactions.GuildPublicThread,
    ) -> str:
        logger.info(
            f"Starting approval process for member {member.id} | Roles: {current_roles}"
        )

        is_appr_forum: bool = thread.parent_id == self.config.APPR_VETTING_FORUM_ID
        role_mapping = {
            True: ("temporary", "approved"),
            False: ("approved", "electoral"),
        }
        required_role_name, target_role_name = role_mapping[is_appr_forum]
        required_role = roles[required_role_name]
        target_role = roles[target_role_name]

        if target_role.id in current_roles:
            await self.send_error(
                ctx,
                f"{member.mention} already has the {target_role_name} role and cannot be approved again.",
            )
            return f"Approval aborted: Already has {target_role_name} role"

        if required_role.id not in current_roles:
            await self.send_error(
                ctx,
                f"{member.mention} must have the {required_role_name} role before being approved.",
            )
            return f"Approval aborted: Missing {required_role_name} role"

        required_approvals: int = 1 if is_appr_forum else self.config.REQUIRED_APPROVALS
        thread_approvals = Approval(
            approval_count=min(thread_approvals.approval_count + 1, required_approvals),
            reviewers=thread_approvals.reviewers | {ctx.author.id},
            last_approval_time=thread_approvals.last_approval_time,
        )
        self.approval_counts[thread.id] = thread_approvals

        if thread_approvals.approval_count == required_approvals:
            thread_approvals.last_approval_time = datetime.now(timezone.utc)
            await self.update_member_roles(
                member, target_role, required_role, current_roles
            )
            await self.send_approval_notification(ctx, member, thread_approvals)
            self.cleanup_approval_data(thread.id)
            return f"Approved {member.mention} with role updates"

        remaining = required_approvals - thread_approvals.approval_count
        await self.send_success(
            ctx,
            f"Your approval for {member.mention} has been registered. Current approval status: {thread_approvals.approval_count}/{required_approvals} approvals needed. Waiting for {remaining} more approval(s).",
            log_to_channel=False,
        )
        return "Approval registered"

    async def process_rejection(
        self,
        ctx: interactions.ComponentContext,
        member: interactions.Member,
        roles: Dict[str, interactions.Role],
        current_roles: FrozenSet[int],
        thread_approvals: Approval,
        thread: interactions.GuildPublicThread,
    ) -> str:
        is_appr_forum: bool = thread.parent_id == self.config.APPR_VETTING_FORUM_ID
        required_role = roles[("approved" if is_appr_forum else "electoral")]
        fallback_role = roles[("temporary" if is_appr_forum else "approved")]

        if required_role.id not in current_roles:
            role_name = "approved" if is_appr_forum else "electoral"
            await self.send_error(
                ctx,
                f"Unable to reject {member.mention} as they have not yet been granted the {role_name} role.",
            )
            return f"Rejection aborted: No {role_name} role"

        if (
            not is_appr_forum
            and thread_approvals.last_approval_time
            and (
                datetime.now(timezone.utc) - thread_approvals.last_approval_time
            ).total_seconds()
            > self.config.REJECTION_WINDOW_DAYS * 86400
        ):
            await self.send_error(
                ctx,
                f"The rejection window for {member.mention} has expired. Rejections must be submitted within {self.config.REJECTION_WINDOW_DAYS} days of approval. Please contact an administrator if you believe this is in error.",
            )
            return "Rejection aborted: Window closed"

        required_rejections: int = (
            1 if is_appr_forum else self.config.REQUIRED_REJECTIONS
        )
        rejection_count = min(thread_approvals.rejection_count + 1, required_rejections)

        thread_approvals = Approval(
            approval_count=thread_approvals.approval_count,
            rejection_count=rejection_count,
            reviewers=thread_approvals.reviewers | {ctx.author.id},
            last_approval_time=thread_approvals.last_approval_time,
        )
        self.approval_counts[thread.id] = thread_approvals

        if rejection_count == required_rejections:
            await self.update_member_roles(
                member, fallback_role, required_role, current_roles
            )
            await self.send_rejection_notification(ctx, member, thread_approvals)
            self.cleanup_approval_data(thread.id)
            return f"Rejected {member.mention} with role updates"

        remaining: int = required_rejections - rejection_count
        await self.send_success(
            ctx,
            f"Your rejection vote for {member.mention} has been registered. Current status: {rejection_count}/{required_rejections} rejections needed. Waiting for {remaining} more rejection(s) to complete the process.",
            log_to_channel=False,
        )
        return "Rejection registered"

    def cleanup_approval_data(self, thread_id: int) -> None:
        self.approval_counts.pop(thread_id, None)
        self.processed_thread_ids.discard(thread_id)

    async def fetch_required_roles(
        self, guild: interactions.Guild
    ) -> Dict[str, Optional[interactions.Role]]:
        return {
            "electoral": await guild.fetch_role(self.config.ELECTORAL_ROLE_ID),
            "approved": await guild.fetch_role(self.config.APPROVED_ROLE_ID),
            "temporary": await guild.fetch_role(self.config.TEMPORARY_ROLE_ID),
        }

    def get_thread_approvals(self, thread_id: int) -> Approval:
        return self.approval_counts.get(thread_id, Approval())

    @staticmethod
    async def update_member_roles(
        member: interactions.Member,
        role_to_add: interactions.Role,
        role_to_remove: interactions.Role,
        current_roles: FrozenSet[int],
    ) -> bool:
        logger.info(
            f"Updating roles for {member.id}: +{role_to_add.id}, -{role_to_remove.id}"
        )

        try:
            if role_to_remove.id in current_roles:
                await member.remove_roles([role_to_remove])
            if role_to_add.id not in current_roles:
                await member.add_roles([role_to_add])

            for attempt in range(3):
                updated_member = await member.guild.fetch_member(member.id)
                updated_roles = {role.id for role in updated_member.roles}

                if (
                    role_to_add.id in updated_roles
                    and role_to_remove.id not in updated_roles
                ):
                    logger.info(f"Role update successful for {member.id}")
                    return True

                if attempt < 2:
                    logger.warning(
                        f"Retrying role update for {member.id}, attempt {attempt + 2}"
                    )
                    if role_to_add.id not in updated_roles:
                        await member.add_roles([role_to_add])
                    if role_to_remove.id in updated_roles:
                        await member.remove_roles([role_to_remove])
                    await asyncio.sleep(0.5 * (2**attempt))

            logger.error(f"Role update failed for {member.id} after 3 attempts")
            return False

        except Exception as e:
            logger.error(f"Error updating roles for {member.id}: {e}", exc_info=True)
            return False

    async def send_approval_notification(
        self,
        ctx: (
            interactions.SlashContext
            | interactions.InteractionContext
            | interactions.ComponentContext
            | None
        ),
        member: interactions.Member,
        thread_approvals: Approval,
    ) -> None:
        if not ctx:
            return

        reviewers_list = sorted(thread_approvals.reviewers)
        reviewers_text = (
            f"<@{reviewers_list[0]}>"
            if len(reviewers_list) == 1
            else ", ".join(f"<@{rid}>" for rid in reviewers_list[:-1])
            + f" and <@{reviewers_list[-1]}>"
        )

        await self.send_success(
            ctx,
            f"{member.mention} has been approved by {reviewers_text}.",
            ephemeral=False,
        )

    async def send_rejection_notification(
        self,
        ctx: (
            interactions.SlashContext
            | interactions.InteractionContext
            | interactions.ComponentContext
            | None
        ),
        member: interactions.Member,
        thread_approvals: Approval,
    ) -> None:
        if not ctx:
            return

        reviewers_list = sorted(thread_approvals.reviewers)
        reviewers_text = (
            f"<@{reviewers_list[0]}>"
            if len(reviewers_list) == 1
            else ", ".join(f"<@{rid}>" for rid in reviewers_list[:-1])
            + f" and <@{reviewers_list[-1]}>"
        )

        await self.send_success(
            ctx,
            f"{member.mention} has been rejected by {reviewers_text}.",
            ephemeral=False,
        )

    # Servant commands

    @module_group_servant.subcommand("view", sub_cmd_description="Servant Directory")
    async def view_servant_roles(self, ctx: interactions.SlashContext) -> None:
        await ctx.defer(ephemeral=True)

        filtered_roles = self.filter_roles(tuple(ctx.guild.roles))
        role_members_list = self.extract_role_members_list(filtered_roles)

        if not role_members_list:
            await self.send_error(ctx, "No matching roles found.")
            return

        total_members = sum(rm.member_count for rm in role_members_list)

        title = f"Servant Directory ({total_members} members)"

        embeds = []
        current_embed = await self.create_embed(title=title)
        field_count = 0

        for role_member in role_members_list:
            members_str = "\n".join([f"- {m}" for m in role_member.members])

            if not members_str:
                continue

            if field_count >= 25:
                embeds.append(current_embed)
                current_embed = await self.create_embed(title=title)
                field_count = 0

            current_embed.add_field(
                name=f"{role_member.role_name} ({role_member.member_count} members)",
                value=members_str,
                inline=True,
            )
            field_count += 1

        if field_count:
            embeds.append(current_embed)

        paginator = Paginator(
            client=self.bot,
            pages=embeds,
            timeout_interval=120,
            show_callback_button=True,
            show_select_menu=True,
            show_back_button=True,
            show_next_button=True,
            show_first_button=True,
            show_last_button=True,
            wrong_user_message="This leaderboard can only be controlled by the user who requested it.",
            hide_buttons_on_stop=True,
        )

        await paginator.send(ctx)

    @staticmethod
    @lru_cache(maxsize=256)
    def filter_roles(
        roles: Tuple[interactions.Role, ...]
    ) -> Tuple[interactions.Role, ...]:
        return (
            ()
            if not roles
            else tuple(
                itertools.islice(
                    filter(
                        lambda r: not r.name.startswith(("——", "══"))
                        and not r.bot_managed,
                        sorted(roles, key=attrgetter("position"), reverse=True),
                    ),
                    next(
                        (
                            i
                            for i, r in enumerate(
                                sorted(roles, key=attrgetter("position"), reverse=True)
                            )
                            if r.name == "═════･[Bot身份组]･═════"
                        ),
                        len(roles),
                    ),
                )
            )
        )

    @staticmethod
    @lru_cache()
    def extract_role_members_list(
        roles: Tuple[interactions.Role, ...]
    ) -> List[Servant]:
        return [
            Servant(
                role_name=role.name,
                members=[member.mention for member in role.members],
                member_count=len(role.members),
            )
            for role in roles
            if role.members
        ]

    # Penitentiary commands

    @module_group_penitentiary.subcommand(
        "incarcerate", sub_cmd_description="Incarcerate"
    )
    @interactions.slash_option(
        name="member",
        description="Member",
        required=True,
        opt_type=interactions.OptionType.USER,
    )
    @interactions.slash_option(
        name="duration",
        description="Incarceration duration (e.g.: 1d 2h 30m)",
        required=True,
        opt_type=interactions.OptionType.STRING,
    )
    @error_handler
    async def incarcerate_member(
        self,
        ctx: interactions.SlashContext,
        member: interactions.Member,
        duration: str,
    ) -> None:
        if not self.validate_penitentiary_permissions(ctx):
            return await self.send_error(
                ctx,
                "You don't have permission to use this command.",
            )

        try:
            incarceration_duration = await asyncio.to_thread(
                lambda: self.parse_duration(duration)
            )
        except ValueError as e:
            return await self.send_error(ctx, str(e))

        return await self.manage_penitentiary_status(
            ctx=ctx,
            member=member,
            action=Action.INCARCERATE,
            duration=incarceration_duration,
        )

    @staticmethod
    def parse_duration(duration: str) -> timedelta:
        _UNIT_MAP: dict[str, int] = {"d": 86400, "h": 3600, "m": 60}

        try:
            total: int = sum(
                int(match.group(1)) * _UNIT_MAP[match.group(2).lower()]
                for match in re.finditer(r"(\d+)([dhm])", duration, re.IGNORECASE)
            )
            if total <= 0:
                raise ValueError
            return timedelta(seconds=total)
        except (AttributeError, KeyError):
            raise ValueError(
                "Invalid duration format. Use combinations of `d` (days), `h` (hours), and `m` (minutes)."
            )
        except ValueError:
            raise ValueError("Incarceration time must be greater than zero.")

    @module_group_penitentiary.subcommand("release", sub_cmd_description="Release")
    @interactions.slash_option(
        name="member",
        description="Member",
        required=True,
        opt_type=interactions.OptionType.USER,
        autocomplete=True,
    )
    @error_handler
    async def release_member(
        self, ctx: interactions.SlashContext, member: interactions.Member
    ) -> None:
        if not self.validate_penitentiary_permissions(ctx):
            return await self.send_error(
                ctx,
                "You don't have permission to use this command.",
            )

        await self.manage_penitentiary_status(ctx, member, Action.RELEASE)

    @release_member.autocomplete("member")
    async def autocomplete_incarcerated_member(
        self, ctx: interactions.AutocompleteContext
    ) -> None:
        user_input: str = ctx.input_text.casefold()
        guild: interactions.Guild = ctx.guild

        choices = list(
            islice(
                (
                    interactions.SlashCommandChoice(
                        name=member.user.username, value=str(member.id)
                    )
                    for member in guild.members
                    if user_input in member.user.username.casefold()
                ),
                25,
            )
        )

        await ctx.send(choices)

    async def manage_penitentiary_status(
        self,
        ctx: Optional[interactions.SlashContext],
        member: interactions.Member,
        action: Action,
        **kwargs: Any,
    ) -> None:
        guild = await self.bot.fetch_guild(self.config.GUILD_ID)
        roles = await self.fetch_penitentiary_roles(guild)

        if not all(roles.values()):
            error_msg = "Required roles not found."
            if ctx:
                return await self.send_error(ctx, error_msg)
            else:
                logger.error(
                    f"Required roles not found for penitentiary action: {error_msg}"
                )
                return

        action_map: Dict[Action, Callable] = {
            Action.INCARCERATE: partial(self.perform_member_incarceration, **kwargs),
            Action.RELEASE: self.perform_member_release,
        }
        try:
            await action_map[action](member, roles, ctx)
        except KeyError:
            error_msg = f"Invalid penitentiary action specified: {action}"
            if ctx:
                await self.send_error(ctx, error_msg)
            else:
                logger.error(error_msg)

    @lru_cache(maxsize=1)
    def _get_role_ids(self) -> Dict[str, int]:
        return {
            "incarcerated": self.config.INCARCERATED_ROLE_ID,
            "electoral": self.config.ELECTORAL_ROLE_ID,
            "approved": self.config.APPROVED_ROLE_ID,
            "temporary": self.config.TEMPORARY_ROLE_ID,
        }

    async def perform_member_incarceration(
        self,
        member: interactions.Member,
        roles: Dict[str, Optional[interactions.Role]],
        ctx: Optional[interactions.SlashContext],
        duration: timedelta,
    ) -> None:
        try:
            incarcerated_role = roles["incarcerated"]
            role_ids = self._get_role_ids()
            member_role_ids = {role.id for role in member.roles}

            roles_to_remove = [
                roles.get(role_key)
                for role_key, role_id in role_ids.items()
                if role_key != "incarcerated"
                and roles.get(role_key) is not None
                and role_id in member_role_ids
            ]

            original_roles = tuple(
                role_id
                for role_id in (
                    role_ids["electoral"],
                    role_ids["approved"],
                    role_ids["temporary"],
                )
                if role_id in member_role_ids
            )

            if roles_to_remove:
                await member.remove_roles(roles_to_remove)
                logger.info(
                    f"Removed roles {[r.id for r in roles_to_remove if r is not None]} from {member}"
                )

            if incarcerated_role:
                await member.add_roles((incarcerated_role,))
                logger.info(
                    f"Added incarcerated role {incarcerated_role.id} to {member}"
                )

        except Exception as e:
            logger.error(
                f"Error assigning roles during incarceration: {e}", exc_info=True
            )
            return

        release_time = int(time.time() + duration.total_seconds())
        self.incarcerated_members[str(member.id)] = {
            "release_time": str(release_time),
            "original_roles": original_roles,
        }
        await self.save_incarcerated_members()

        executor = getattr(ctx, "author", None)
        log_message = (
            f"{member.mention} has been incarcerated until <t:{release_time}:F> "
            f"(<t:{release_time}:R>) by {executor.mention if executor else 'the system'}."
        )
        await self.send_success(ctx, log_message, ephemeral=False)

    async def perform_member_release(
        self,
        member: interactions.Member,
        roles: Dict[str, Optional[interactions.Role]],
        ctx: Optional[interactions.SlashContext],
    ) -> None:
        member_id_str = str(member.id)
        member_data = self.incarcerated_members.get(member_id_str, {})
        original_role_ids = frozenset(member_data.get("original_roles", []))
        current_role_ids = {role.id for role in member.roles}

        roles_to_add = tuple(
            role
            for role in member.guild.roles
            if role.id in original_role_ids and role.id not in current_role_ids
        )

        if (incarcerated_role := roles.get("incarcerated")) in member.roles:
            await member.remove_roles((incarcerated_role,))
            logger.info(f"Removed incarcerated role from {member}")

        if roles_to_add:
            await member.add_roles(roles_to_add)
            logger.info(
                f"Restored roles {tuple(r.id for r in roles_to_add)} to {member}"
            )

        del self.incarcerated_members[member_id_str]
        await self.save_incarcerated_members()

        executor = getattr(ctx, "author", None) if ctx else None
        release_time = int(float(member_data.get("release_time", 0)))
        current_time = int(time.time())
        log_message = f"{member.mention} has been released by {executor.mention if executor else 'the system'} at <t:{current_time}:F>. Scheduled: <t:{release_time}:F>."
        await self.send_success(ctx, log_message, ephemeral=False)

    async def schedule_release(
        self, member_id: str, data: Dict[str, Any], delay: float
    ) -> None:
        try:
            await asyncio.wait_for(asyncio.sleep(delay), timeout=delay)
        except asyncio.TimeoutError:
            pass
        finally:
            await self.release_prisoner(member_id, data)

    async def release_prisoner(self, member_id: str, data: Dict[str, Any]) -> None:
        release_time: int = int(float(data.get("release_time", 0)))

        try:
            guild = await self.bot.fetch_guild(self.config.GUILD_ID)
            member = await guild.fetch_member(int(member_id))
        except Exception as e:
            error_msg = f"Error fetching guild/member {member_id}: {e!r}. Release time: <t:{release_time}:F>"
            logger.error(error_msg)
            await self.send_error(None, error_msg)
            return

        try:
            if member:
                await self.manage_penitentiary_status(None, member, Action.RELEASE)
            else:
                log_message = f"Member {member_id} not found. Scheduled release: <t:{release_time}:F>."
                await self.send_error(None, log_message)
        except Exception as e:
            error_msg = f"Release failed for {member_id}: {e!r}. Scheduled: <t:{release_time}:F>."
            logger.error(error_msg)
            await self.send_error(None, error_msg)
        finally:
            self.incarcerated_members.pop(member_id, None)
            await self.save_incarcerated_members()

    async def fetch_penitentiary_roles(
        self, guild: interactions.Guild
    ) -> Dict[str, Optional[interactions.Role]]:
        role_ids = self._get_role_ids()
        roles: Dict[str, Optional[interactions.Role]] = {}

        for key, role_id in role_ids.items():
            try:
                role = await guild.fetch_role(role_id)
                logger.info(f"Successfully fetched {key} role: {role.id}")
                roles[key] = role
            except Exception as e:
                logger.error(
                    f"Failed to fetch {key} role (ID: {role_id}): {e}", exc_info=True
                )
                continue

        return roles

    # Events

    @interactions.listen(MessageCreate)
    async def on_message_create(self, event: MessageCreate) -> None:
        if (
            not self.message_monitoring_enabled
            or event.message.author.bot
            or not event.message.guild
        ):
            return

        guild = event.message.guild
        author_id = event.message.author.id

        try:
            member = await guild.fetch_member(author_id)
        except Exception:
            return

        if not any(role.id == self.config.TEMPORARY_ROLE_ID for role in member.roles):
            return

        current_time = time.monotonic()
        message_content = event.message.content.strip()

        async with self.stats_lock:
            if str(author_id) not in self.stats:
                self.stats[str(author_id)] = Counter()

            stats = self.stats[str(author_id)]

            timestamps = stats.get("message_timestamps", [])
            cutoff = current_time - 7200
            stats["message_timestamps"] = [
                t for t in [*timestamps, current_time] if t > cutoff
            ]

            is_valid = not Message(
                message_content, stats, self.limit_config, self.validation_flags
            ).analyze()

            invalid_count = stats.get("invalid_message_count", 0)
            recovery_streaks = stats.get("recovery_streaks", 0)
            feedback_score = stats.get("feedback_score", 0.0)

            stats.update(
                {
                    "invalid_message_count": (
                        invalid_count - (1 if invalid_count > 0 else 0)
                        if is_valid
                        else invalid_count + 1
                    ),
                    "recovery_streaks": (recovery_streaks + 1 if is_valid else 0),
                    "feedback_score": (
                        min(5.0, feedback_score + 0.5)
                        if is_valid
                        else max(-5.0, feedback_score - 1.0)
                    ),
                }
            )

            if not is_valid:
                logger.warning(f"Invalid message from user {author_id}")

            await self._adjust_thresholds(stats)
            self.stats_save_task and self.stats_save_task.cancel()
            self.stats_save_task = asyncio.create_task(self._save_stats())

    async def _adjust_thresholds(self, user_stats: Dict[str, Any]) -> None:
        if not self.message_monitoring_enabled or not self.validation_flags.get(
            "feedback"
        ):
            return

        feedback = float(user_stats.get("feedback_score", 0.0))
        last_adj = float(user_stats.get("last_threshold_adjustment", 0.0))
        time_delta = (now := time.time()) - last_adj

        adj = 0.01 * feedback * math.tanh(abs(feedback) / 5)
        decay = math.exp(-time_delta / 3600)

        threshold_map: Dict[str, Tuple[float, float, float]] = {}

        if self.validation_flags.get("digit_ratio"):
            threshold_map["DIGIT_RATIO_THRESHOLD"] = (0.1, 1.0, 0.5)

        if self.validation_flags.get("entropy"):
            threshold_map["MIN_MESSAGE_ENTROPY"] = (0.0, 4.0, 1.5)

        if threshold_map:
            self.limit_config.update(
                {
                    name: min(
                        max(
                            float(self.limit_config.get(name, default))
                            + adj
                            + (default - float(self.limit_config.get(name, default)))
                            * (1 - decay),
                            min_v,
                        ),
                        max_v,
                    )
                    for name, (min_v, max_v, default) in threshold_map.items()
                }
            )

            user_stats["last_threshold_adjustment"] = now

            logger.debug(
                f"Thresholds adjusted - Feedback: {feedback:.2f}, Adjustment: {adj:.4f}, Decay: {decay:.4f}"
            )

    async def _save_stats(self) -> None:
        try:
            await asyncio.sleep(5.0)
            async with self.stats_lock:
                await self.save_stats_roles()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Failed to save stats: {e}", exc_info=True)
        finally:
            self.stats_save_task = None

    # @interactions.listen(MessageCreate)
    # async def on_missing_member_message(self, event: MessageCreate) -> None:
    #     if any((event.message.author.bot, not event.message.guild)):
    #         return

    #     guild, author_id = event.message.guild, event.message.author.id

    #     try:
    #         member = await guild.fetch_member(author_id)
    #         if self.config.MISSING_ROLE_ID not in {role.id for role in member.roles}:
    #             return

    #         missing_role = await guild.fetch_role(self.config.MISSING_ROLE_ID)
    #         if isinstance(missing_role, Exception):
    #             return

    #         temp_role = await guild.fetch_role(self.config.TEMPORARY_ROLE_ID)
    #         if isinstance(temp_role, Exception):
    #             return

    #         await member.remove_roles([missing_role])
    #         await member.add_roles([temp_role])

    #         logger.info(
    #             f"Converted member {member.id} from missing to temporary status after message"
    #         )

    #         await self.send_success(
    #             None,
    #             f"Member {member.mention} has been converted from `{missing_role.name}` to `{temp_role.name}` after sending a message.",
    #         )

    #     except Exception as e:
    #         logger.error(
    #             f"Error converting missing member {author_id} to temporary: {e!r}\n{traceback.format_exc()}"
    #         )

    @interactions.listen(ExtensionLoad)
    async def on_extension_load(self, event: ExtensionLoad) -> None:
        self.update_roles_based_on_activity.start()
        self.cleanup_old_locks.start()
        self.check_incarcerated_members.start()
        self.cleanup_stats.start()

    @interactions.listen(ExtensionUnload)
    async def on_extension_unload(self, event: ExtensionUnload) -> None:
        tasks_to_stop: tuple = (
            self.update_roles_based_on_activity,
            self.cleanup_old_locks,
            self.check_incarcerated_members,
            self.cleanup_stats,
        )
        for task in tasks_to_stop:
            task.stop()

        pending_tasks = [
            task for task in asyncio.all_tasks() if task.get_name().startswith("Task-")
        ]
        await asyncio.gather(
            *map(partial(asyncio.wait_for, timeout=10.0), pending_tasks),
            return_exceptions=True,
        )

    @interactions.listen(NewThreadCreate)
    async def on_new_thread_create(self, event: NewThreadCreate) -> None:
        if not isinstance(thread := event.thread, interactions.GuildPublicThread) or (
            (
                thread.parent_id
                not in (
                    self.config.ELECT_VETTING_FORUM_ID,
                    self.config.APPR_VETTING_FORUM_ID,
                )
            )
            | (thread.id in self.processed_thread_ids)
            | (thread.owner_id is None)
        ):
            return

        try:
            await self.handle_new_thread(thread)
        finally:
            self.processed_thread_ids.add(thread.id)

    # Tasks

    @interactions.Task.create(interactions.IntervalTrigger(hours=1))
    async def cleanup_stats(self) -> None:
        try:
            guild: interactions.Guild = await self.bot.fetch_guild(self.config.GUILD_ID)
            processed = removed = 0
            members_to_remove = set()
            temp_role_id = self.config.TEMPORARY_ROLE_ID
            approved_role_id = self.config.APPROVED_ROLE_ID

            for member_batch in (
                tuple(islice(self.stats, i, i + 100))
                for i in range(0, len(self.stats), 100)
            ):
                for member_id in member_batch:
                    processed += 1
                    try:
                        member = await guild.fetch_member(int(member_id))
                        if member:
                            member_role_ids = frozenset(
                                role.id for role in member.roles
                            )
                            if not (
                                temp_role_id in member_role_ids
                                and approved_role_id not in member_role_ids
                            ):
                                members_to_remove.add(member_id)
                                removed += 1
                        else:
                            members_to_remove.add(member_id)
                            removed += 1
                    except Exception as e:
                        logger.error(
                            f"Error processing member {member_id} during stats cleanup: {e!r}"
                        )

            if members_to_remove:
                filtered_stats = {}
                for k, v in self.stats.items():
                    if k not in members_to_remove:
                        filtered_stats[k] = v
                self.stats = filtered_stats
                await self.save_stats_roles()
                logger.info(
                    f"Stats cleanup completed - Processed: {processed}, Removed: {removed} members"
                )
            else:
                logger.info(
                    f"Stats cleanup completed - No members needed removal (Processed: {processed})"
                )

        except Exception as e:
            logger.error(
                f"Critical error in stats cleanup task: {e!r}\n{traceback.format_exc()}"
            )
            raise

    @interactions.Task.create(interactions.IntervalTrigger(days=7))
    async def cleanup_old_locks(self) -> None:
        current_time: datetime = datetime.now(timezone.utc)
        threshold: timedelta = timedelta(days=7)
        self.member_role_locks = {
            k: v
            for k, v in self.member_role_locks.items()
            if not (
                isinstance(v["lock"], asyncio.Lock)
                and not v["lock"].locked()
                and isinstance(v["last_used"], datetime)
                and isinstance(current_time, datetime)
                and (current_time - cast(datetime, v["last_used"])).total_seconds()
                > threshold.total_seconds()
            )
        }
        removed: int = len(self.member_role_locks) - len(self.member_role_locks)
        logger.info(f"Cleaned up {removed} old locks.")

    @interactions.Task.create(interactions.IntervalTrigger(seconds=30))
    async def check_incarcerated_members(self) -> None:
        self.incarcerated_members = await self.model.load_data(
            "incarcerated_members.json", dict
        )
        now: float = time.time()
        release_times: Dict[str, float] = {
            str(member_id): (
                float(data["release_time"]) if isinstance(data, dict) else 0.0
            )
            for member_id, data in self.incarcerated_members.items()
        }

        release_map: Dict[str, Dict[str, Any]] = {
            str(member_id): data if isinstance(data, dict) else {"release_time": 0.0}
            for member_id, data in dict(
                itertools.compress(
                    self.incarcerated_members.items(),
                    (t <= now for t in release_times.values()),
                )
            ).items()
        }

        schedule_map: Dict[str, Dict[str, Any]] = {
            str(member_id): data if isinstance(data, dict) else {"release_time": 0.0}
            for member_id, data in dict(
                itertools.compress(
                    self.incarcerated_members.items(),
                    (0 < t - now <= 60 for t in release_times.values()),
                )
            ).items()
        }

        tasks = (
            asyncio.create_task(
                self.schedule_release(
                    member_id, data, max(0.0, float(data["release_time"]) - now)
                ),
                name=f"release_{member_id}",
            )
            for member_id, data in schedule_map.items()
        )
        tuple(tasks)

        exceptions: List[Exception] = []
        for member_id, data in release_map.items():
            try:
                await self.release_prisoner(member_id, data)
            except Exception as e:
                exceptions.append(e)
                logger.error(f"Error releasing prisoner: {e}", exc_info=True)

        if release_map:
            logger.info(
                f"Released {len(release_map)} prisoners"
                f"{f' with {len(exceptions)} errors' if exceptions else ''}"
            )
        else:
            logger.debug("No prisoners to release at this time")

    @interactions.Task.create(interactions.IntervalTrigger(hours=1))
    async def update_roles_based_on_activity(self) -> None:
        try:
            guild: interactions.Guild = await self.bot.fetch_guild(self.config.GUILD_ID)
            roles: Dict[str, interactions.Role] = {
                name: await guild.fetch_role(id_)
                for name, id_ in (
                    ("approved", self.config.APPROVED_ROLE_ID),
                    ("temporary", self.config.TEMPORARY_ROLE_ID),
                    ("electoral", self.config.ELECTORAL_ROLE_ID),
                )
            }

            role_updates: Dict[str, DefaultDict[int, List[interactions.Role]]] = {
                op: defaultdict(list) for op in ("remove", "add")
            }
            log_messages: List[str] = []
            members_to_update: Set[int] = set()

            filtered_stats = {
                mid: stats
                for mid, stats in self.stats.items()
                if isinstance(stats, dict)
                and (
                    len(stats.get("message_timestamps", []))
                    - stats.get("invalid_message_count", 0)
                )
                >= 50
            }

            for member_id, stats in filtered_stats.items():
                if not (member := await guild.fetch_member(int(member_id))):
                    logger.warning(f"Member {member_id} not found during processing")
                    continue
                member_role_ids: Set[int] = {role.id for role in member.roles}
                stats_dict = cast(Dict[str, Any], stats)
                valid_messages = len(
                    stats_dict.get("message_timestamps", [])
                ) - stats_dict.get("invalid_message_count", 0)

                if all(
                    (
                        roles["temporary"].id in member_role_ids,
                        roles["approved"].id not in member_role_ids,
                        roles["electoral"].id not in member_role_ids,
                        valid_messages >= 5,
                    )
                ):
                    member_id_int = int(member_id)
                    role_updates["remove"][member_id_int].append(roles["temporary"])
                    role_updates["add"][member_id_int].append(roles["approved"])
                    members_to_update.add(member_id_int)
                    log_messages.append(
                        f"Updated roles for `{member_id}`: Sent `{valid_messages}` valid messages, upgraded from `{roles['temporary'].name}` to `{roles['approved'].name}`."
                    )
            if members_to_update:
                for member_id_int in members_to_update:
                    try:
                        if member := await guild.fetch_member(member_id_int):
                            if remove_roles := role_updates["remove"][member_id_int]:
                                await member.remove_roles(remove_roles)
                            if add_roles := role_updates["add"][member_id_int]:
                                await member.add_roles(add_roles)
                        else:
                            logger.error(
                                f"Member {member_id} not found during role update"
                            )
                    except Exception as e:
                        logger.error(
                            f"Error updating roles for member {member_id}: {str(e)}\n{traceback.format_exc()}"
                        )

                if log_messages:
                    await self.send_success(None, "\n".join(log_messages))

            new_stats: Dict[str, Dict[str, Any]] = {
                k: cast(Dict[str, Any], v)
                for k, v in self.stats.items()
                if (
                    int(k) not in members_to_update
                    or len(cast(Dict[str, Any], v).get("message_timestamps", [])) < 5
                )
            }
            self.stats = new_stats
            await self.save_stats_roles()

        except Exception as e:
            logger.error(
                f"Critical error in role update task: {str(e)}\n{traceback.format_exc()}"
            )
            raise

    # Serve

    async def handle_new_thread(self, thread: interactions.GuildPublicThread) -> None:
        try:
            timestamp = f"{int(datetime.now(timezone.utc).strftime('%y%m%d%H%M'))}"
            new_title = f"[{timestamp}] {thread.name}"

            await thread.edit(name=new_title)

            review_task = asyncio.create_task(self.send_review_components(thread))
            notify_task = asyncio.create_task(
                self.notify_vetting_reviewers(
                    self.config.VETTING_ROLE_IDS, thread, timestamp
                )
            )

            await review_task
            await notify_task

        except Exception as e:
            logger.exception(f"Error processing new post: {e!r}")

    async def send_review_components(
        self, thread: interactions.GuildPublicThread
    ) -> None:
        embed, buttons = await self.create_review_components(thread)
        await thread.send(embed=embed, components=buttons)

    custom_roles_menu_pattern = re.compile(r"manage_roles_menu_(\d+)")

    @interactions.component_callback(custom_roles_menu_pattern)
    async def handle_custom_roles_menu(
        self, ctx: interactions.ComponentContext
    ) -> None:
        if (
            not (match := self.custom_roles_menu_pattern.match(ctx.custom_id))
            or not ctx.values
        ):
            await self.send_error(
                ctx,
                (
                    "Please select an action (add/remove) from the dropdown menu to continue."
                    if match
                    else "Invalid menu format"
                ),
            )
            return

        member_id: int = int(match[1])
        action_str = ctx.values[0]
        if action_str not in ("add", "remove"):
            raise ValueError(f"Invalid action: {action_str}")
        action: Literal["add", "remove"] = "add" if action_str == "add" else "remove"

        member: interactions.Member = await ctx.guild.fetch_member(member_id)
        if not member:
            await self.send_error(ctx, f"Unable to find member with ID {member_id}.")
            return

        custom_roles_count: int = len(self.custom_roles)
        if not custom_roles_count or custom_roles_count > 25:
            await self.send_error(
                ctx,
                (
                    lambda x: (
                        "There are currently no custom roles configured that can be managed."
                        if not x
                        else "There are too many custom roles to display in a single menu. Support for multiple pages will be added soon."
                    )
                )(custom_roles_count),
            )
            return

        options: tuple[interactions.StringSelectOption, ...] = tuple(
            map(
                lambda role: interactions.StringSelectOption(label=role, value=role),
                self.custom_roles,
            )
        )

        await ctx.send(
            f"Please select which role you would like to {action} {('to' if action == 'add' else 'from')} {member.mention}",
            components=[
                interactions.StringSelectMenu(
                    *options,
                    custom_id=f"{action}_roles_menu_{member.id}",
                    placeholder="Select role to manage",
                )
            ],
            ephemeral=True,
        )

    role_menu_regex_pattern = re.compile(r"(add|remove)_roles_menu_(\d+)")

    @interactions.component_callback(role_menu_regex_pattern)
    async def on_role_menu_select(self, ctx: interactions.ComponentContext) -> None:
        try:
            logger.info(
                f"on_role_menu_select triggered with custom_id: {ctx.custom_id}"
            )

            if not (match := self.role_menu_regex_pattern.match(ctx.custom_id)):
                logger.error(f"Invalid custom ID format: {ctx.custom_id}")
                return await self.send_error(ctx, "Invalid custom ID format.")

            action, member_id_str = match.groups()
            member_id = int(member_id_str)
            logger.info(f"Parsed action: {action}, member_id: {member_id}")

            if action not in {Action.ADD.value, Action.REMOVE.value}:
                logger.error(f"Invalid action: {action}")
                return await self.send_error(ctx, f"Invalid action: {action}")

            try:
                member = await ctx.guild.fetch_member(member_id)
            except NotFound:
                logger.error(f"Member with ID {member_id} not found.")
                return await self.send_error(
                    ctx, f"Member with ID {member_id} not found."
                )

            if not (selected_role := next(iter(ctx.values), None)):
                logger.warning("No role selected.")
                return await self.send_error(
                    ctx,
                    "Please select a role from the dropdown menu to continue.",
                )

            logger.info(f"Selected role: {selected_role}")

            if await self.update_custom_roles(
                member_id, {selected_role}, Action(action)
            ):
                action_past = "added to" if action == "add" else "removed from"
                success_message = (
                    f"The role {selected_role} has been {action_past} {member.mention}."
                )
                logger.info(success_message)
                await self.send_success(ctx, success_message)
                await self.save_custom_roles()
            else:
                logger.warning("No roles were updated.")
                await self.send_error(ctx, "No roles were updated.")

        except Exception as e:
            logger.error(f"Error in on_role_menu_select: {str(e)}")
            logger.error(traceback.format_exc())
            await self.send_error(ctx, f"An unexpected error occurred: {str(e)}")

    async def update_custom_roles(
        self, user_id: int, roles: Set[str], action: Action
    ) -> Set[str]:
        updated_roles: set[str] = set()
        for role in roles:
            if role not in self.custom_roles:
                self.custom_roles[role] = set()
                updated_roles.add(role)
                continue

            members = self.custom_roles[role]
            if action == Action.ADD and user_id not in members:
                members.add(user_id)
                updated_roles.add(role)
            elif action == Action.REMOVE and user_id in members:
                members.remove(user_id)
                if not members:
                    self.custom_roles.pop(role, None)
                updated_roles.add(role)

        if updated_roles:
            await self.save_custom_roles()
        return updated_roles

    async def save_custom_roles(self) -> None:
        try:
            serializable_custom_roles = dict(
                map(lambda x: (x[0], list(x[1])), self.custom_roles.items())
            )
            await self.model.save_data("custom.json", serializable_custom_roles)
            logger.info("Custom roles saved successfully")
        except Exception as e:
            logger.error(f"Failed to save custom roles: {e}", exc_info=True)
            raise

    async def save_stats_roles(self) -> None:
        try:
            await self.model.save_data("stats.json", dict(self.stats))
            logger.info("Stats saved successfully")
        except Exception as e:
            logger.error(f"Failed to save stats roles: {e!r}")
            raise

    async def save_incarcerated_members(self) -> None:
        try:
            await self.model.save_data(
                "incarcerated_members.json", dict(self.incarcerated_members)
            )
        except Exception as e:
            logger.error(f"Failed to save incarcerated members: {e!r}")
            raise
