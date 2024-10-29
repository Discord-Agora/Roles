import asyncio
import itertools
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
from itertools import filterfalse, islice
from multiprocessing import cpu_count
from operator import attrgetter, sub
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
import cysimdjson
import interactions
import numpy as np
import orjson
from cachetools import TTLCache
from interactions.api.events import (
    ExtensionLoad,
    ExtensionUnload,
    MessageCreate,
    NewThreadCreate,
)
from interactions.client.errors import NotFound
from interactions.ext.paginators import Paginator
from loguru import logger
from pydantic import BaseModel, Field
from yarl import URL

BASE_DIR: str = os.path.dirname(os.path.abspath(__file__))
LOG_FILE: str = os.path.join(BASE_DIR, "roles.log")

logger.remove()
logger.add(
    sink=LOG_FILE,
    level="DEBUG",
    format=(
        "{time:YYYY-MM-DD HH:mm:ss.SSS ZZ} | "
        "{process}:{thread} | "
        "{level: <8} | "
        "{name}:{function}:{line} - "
        "{message}"
    ),
    filter=lambda record: (
        record["name"].startswith("extensions.github_d_com__kazuki388_s_Roles.main")
    ),
    colorize=None,
    serialize=False,
    backtrace=True,
    diagnose=True,
    context=None,
    enqueue=False,
    catch=True,
    rotation="1 MB",
    retention=1,
    compression="gz",
    encoding="utf-8",
    mode="a",
)

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
    assigned_roles: Dict[str, Dict[str, int]] = Field(default_factory=dict)
    authorized_roles: Dict[str, int] = Field(default_factory=dict)
    assignable_roles: Dict[str, List[str]] = Field(default_factory=dict)
    incarcerated_members: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    class Config:
        json_encoders = {
            set: list,
        }


@dataclass
class Config:
    VETTING_FORUM_ID: int = 1164834982737489930
    VETTING_ROLE_IDS: List[int] = field(default_factory=lambda: [1200066469300551782])
    ELECTORAL_ROLE_ID: int = 1200043628899356702
    APPROVED_ROLE_ID: int = 1282944839679344721
    TEMPORARY_ROLE_ID: int = 1164761892015833129
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
    LOG_POST_ID: int = 1279118293936111707
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


class Model(Generic[T]):
    def __init__(self):
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

            return wrapper

        return decorator

    async def _get_file_lock(self, file_name: str) -> asyncio.Lock:
        return self._file_locks.setdefault(file_name, asyncio.Lock())

    @asynccontextmanager
    async def _file_operation(self, file_path: Path, mode: str):
        lock = await self._get_file_lock(str(file_path))
        async with lock:
            try:
                async with aiofiles.open(str(file_path), mode=mode) as file:
                    yield file
            except IOError as e:
                logger.error(f"IO operation failed for {file_path}: {e}")
                raise

    @async_retry(max_retries=3, delay=1.0)
    async def load_data(self, file_name: str, model: Type[T]) -> T:
        file_path = self.base_path / file_name
        try:
            async with self._file_operation(file_path, "rb") as file:
                content = await file.read()

            loop = asyncio.get_running_loop()
            try:
                json_parsed = await loop.run_in_executor(
                    self._executor, lambda: orjson.loads(content)
                )
            except orjson.JSONDecodeError:
                logger.warning(
                    f"`orjson` failed to parse {file_name}, using `cysimdjson`"
                )
                json_parsed = await loop.run_in_executor(
                    self._executor, lambda: self.parser.parse(content)
                )

            if file_name == "custom.json":
                instance = {
                    role: frozenset(members) for role, members in json_parsed.items()
                }
            elif issubclass(model, BaseModel):
                instance = await loop.run_in_executor(
                    self._executor, lambda: model.model_validate(json_parsed)
                )
            elif model == Counter:
                instance = dict(Counter(json_parsed))
            else:
                instance = json_parsed

            self._data_cache[file_name] = instance
            logger.info(f"Successfully loaded data from {file_name}")
            return cast(T, instance if isinstance(instance, model) else model(instance))

        except FileNotFoundError:
            logger.info(f"{file_name} not found. Creating new.")
            if file_name == "custom.json":
                instance = {}
            else:
                instance = (
                    model()
                    if issubclass(model, BaseModel)
                    else Counter() if model == Counter else {}
                )
            await self.save_data(file_name, instance)
            return instance if isinstance(instance, model) else model(instance)

        except Exception as e:
            error_msg = f"Unexpected error loading {file_name}: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e

    @async_retry(max_retries=3, delay=1.0)
    async def save_data(self, file_name: str, data: T) -> None:
        file_path = self.base_path / file_name
        try:
            loop = asyncio.get_running_loop()
            json_data = await loop.run_in_executor(
                self._executor,
                lambda: orjson.dumps(
                    data.dict() if hasattr(data, "dict") else data,
                    option=orjson.OPT_INDENT_2
                    | orjson.OPT_SERIALIZE_NUMPY
                    | orjson.OPT_NON_STR_KEYS,
                ),
            )

            async with self._file_operation(file_path, "wb") as file:
                await file.write(json_data)

            self._data_cache[file_name] = data
            logger.info(f"Successfully saved data to {file_name}")

        except Exception as e:
            logger.error(f"Error saving {file_name}: {e}")
            raise

    def __del__(self):
        self._executor.shutdown(wait=False, cancel_futures=True)


@dataclass
class Message:
    message: str
    user_stats: Dict[str, Any]
    config: Dict[str, float]
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
                map(
                    lambda c: "\u4e00" <= c <= "\u9fff" or "\u3400" <= c <= "\u4dbf",
                    msg,
                )
            ),
        )

    def analyze(self) -> frozenset[str]:
        violations: set[str] = set()
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                "message_repetition": executor.submit(self._check_repetition),
                "excessive_digits": executor.submit(self._check_digit_ratio),
                "low_entropy": executor.submit(self._check_entropy),
            }
            violations.update(k for k, f in futures.items() if f.result())

        self.user_stats["feedback_score"] = max(
            -5, min(5, self.user_stats.get("feedback_score", 0) - len(violations))
        )
        return frozenset(violations)

    def _check_repetition(self) -> bool:
        last_msg = self.user_stats.get("last_message", "")
        if self.message == last_msg:
            rep_count = self.user_stats.get("repetition_count", 0) + 1
            self.user_stats["repetition_count"] = rep_count
            return rep_count >= self.config["MAX_REPETITIONS"]
        self.user_stats.update({"repetition_count": 0, "last_message": self.message})
        return False

    def _check_digit_ratio(self) -> bool:
        if not self._message_length:
            return False
        threshold = self.config["DIGIT_THRESHOLD"] * (1.5 if self._is_chinese else 1.0)
        digit_count = sum(
            freq for char, freq in self._char_frequencies.items() if char.isdigit()
        )
        return (digit_count / self._message_length) > threshold

    def _check_entropy(self) -> bool:
        if not self._message_length:
            return False

        freqs = np.fromiter(self._char_frequencies.values(), dtype=np.float64)
        probs = freqs / self._message_length
        entropy = -np.sum(probs * np.log2(probs))

        base_threshold = self.config["MIN_ENTROPY_THRESHOLD"] * (
            0.7 if self._is_chinese else 1.0
        )
        length_adjustment = (np.log2(max(self._message_length, 2)) / 10) * (
            0.8 if self._is_chinese else 1.0
        )

        return entropy < max(base_threshold, 2.0 - length_adjustment)


# Controller


class Roles(interactions.Extension):
    def __init__(self, bot: interactions.Client):
        self.bot: interactions.Client = bot
        self.config: Config = Config()
        self.vetting_roles: Data = Data()
        self.custom_roles: Dict[str, Set[int]] = {}
        self.incarcerated_members: Dict[str, Dict[str, Any]] = {}
        self.stats: Counter[str] = Counter()
        self.processed_thread_ids: Set[int] = set()
        self.approval_counts: Dict[int, Approval] = {}
        self.member_lock_map: Dict[int, Dict[str, Union[asyncio.Lock, datetime]]] = {}
        self._stats_lock: asyncio.Lock = asyncio.Lock()
        self._save_stats_task: asyncio.Task | None = None

        self.dynamic_config: Dict[str, Union[float, int]] = {
            "MESSAGE_RATE_WINDOW": 60.0,
            "MAX_REPETITIONS": 3,
            "DIGIT_THRESHOLD": 0.5,
            "MIN_ENTROPY_THRESHOLD": 1.5,
        }

        self.cache = TTLCache(maxsize=100, ttl=300)

        self.base_path: Path = Path(__file__).parent
        self.model: Model[Any] = Model()
        self.load_tasks: List[Coroutine] = [
            self.model.load_data("vetting.json", Data),
            self.model.load_data("custom.json", dict),
            self.model.load_data("incarcerated_members.json", dict),
            self.model.load_data("stats.json", Counter),
        ]

        asyncio.create_task(self.load_initial_data())

    async def load_initial_data(self):
        try:
            results = await asyncio.gather(*self.load_tasks)
            (
                self.vetting_roles,
                self.custom_roles,
                self.incarcerated_members,
                self.stats,
            ) = results
            logger.info("Initial data loaded successfully")
        except Exception as e:
            logger.critical(f"Failed to load critical data: {e}")

    # Decorator

    ContextType = TypeVar("ContextType", bound=interactions.ContextType)

    def error_handler(
        func: Callable[Concatenate[Any, ContextType, P], Coroutine[Any, Any, T]]
    ) -> Callable[Concatenate[Any, ContextType, P], Coroutine[Any, Any, T]]:
        @wraps(func)
        async def wrapper(
            self, ctx: interactions.ContextType, *args: P.args, **kwargs: P.kwargs
        ) -> T:
            try:
                result = await func(self, ctx, *args, **kwargs)
                logger.info(f"`{func.__name__}` completed successfully: {result}")
                return result
            except asyncio.CancelledError as ce:
                logger.warning(f"{func.__name__} was cancelled")
                raise ce from None
            except Exception as e:
                logger.exception(
                    f"Error in {func.__name__}: {e!r}\n{traceback.format_exc()}"
                )
                try:
                    await asyncio.shield(self.send_error(ctx, str(e)))
                except Exception:
                    pass
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

    def _validate_roles(
        self,
        ctx: interactions.ContextType,
        required_role_ids: frozenset[int],
        role_ids_to_check: frozenset[int] | None = None,
        check_assignable: bool = False,
    ) -> bool:
        has_permission = bool(
            frozenset(map(attrgetter("id"), ctx.author.roles)) & required_role_ids
        )
        return has_permission and (
            not check_assignable
            or (
                role_ids_to_check is not None
                and role_ids_to_check <= self.get_assignable_role_ids()
            )
        )

    def validate_vetting_permissions(self, ctx: interactions.ContextType) -> bool:
        return self._validate_roles(ctx, frozenset(self.config.VETTING_ROLE_IDS))

    def validate_vetting_permissions_with_roles(
        self, ctx: interactions.ContextType, role_ids_to_add: Iterable[int]
    ) -> bool:
        return self._validate_roles(
            ctx,
            frozenset(self.config.VETTING_ROLE_IDS),
            frozenset(role_ids_to_add),
            check_assignable=True,
        )

    def validate_custom_permissions(self, ctx: interactions.ContextType) -> bool:
        return self._validate_roles(
            ctx, frozenset(self.config.AUTHORIZED_CUSTOM_ROLE_IDS)
        )

    def validate_penitentiary_permissions(self, ctx: interactions.ContextType) -> bool:
        return self._validate_roles(
            ctx, frozenset(self.config.AUTHORIZED_PENITENTIARY_ROLE_IDS)
        )

    @lru_cache(maxsize=128)
    def _get_category_role_ids(
        self, category: str, *, _cache: dict[str, frozenset[int]] = {}
    ) -> frozenset[int]:
        key = f"category_role_ids_{category}"
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

        conflicts = [
            (
                existing := member_roles & category_roles,
                adding := roles_to_add & category_roles,
            )
            for category, category_roles in (
                (cat, self._get_category_role_ids(cat))
                for cat in self.vetting_roles.assigned_roles
            )
            if bool(existing := member_roles & category_roles)
            and bool(adding := roles_to_add & category_roles)
            and len(existing | adding) > 1
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
        self, title: str, description: str = "", color: EmbedColor = EmbedColor.INFO
    ) -> interactions.Embed:
        return interactions.Embed(
            title=title,
            description=description,
            color=color.value,
            timestamp=datetime.now(timezone.utc),
        ).set_footer(text="鍵政大舞台")

    async def notify_vetting_reviewers(
        self,
        reviewer_role_ids: List[int],
        thread: interactions.GuildPublicThread,
        timestamp: str,
    ) -> None:
        if not (guild := await self.bot.fetch_guild(thread.guild.id)):
            error_msg = f"Guild with ID {thread.guild.id} could not be fetched."
            logger.error(error_msg)
            raise ValueError(error_msg)

        embed = await self.create_embed(
            title=f"Voter Identity Approval #{timestamp}",
            description=f"[Click to jump: {thread.name}](https://discord.com/channels/{thread.guild.id}/{thread.id})",
            color=EmbedColor.INFO,
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
                logger.error(f"Error processing role {role_id}: {e}")

        for role_id in reviewer_role_ids:
            await process_role(role_id)

        logger.info(f"All reviewer notifications sent for thread {thread.id}")

    async def send_direct_message(
        self, member: interactions.Member, embed: interactions.Embed
    ) -> None:
        try:
            await member.send(embed=embed)
            logger.debug(f"Sent notification to member {member.id}")
        except Exception as e:
            logger.error(f"Failed to send embed to {member.id}: {e}")

    @lru_cache(maxsize=1)
    def _get_log_channels(self) -> tuple[int, int, int]:
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
    ) -> None:
        embed: interactions.Embed = await self.create_embed(title, message, color)

        if ctx:
            await ctx.send(embed=embed, ephemeral=True)

        if log_to_channel:
            LOG_CHANNEL_ID, LOG_POST_ID, LOG_FORUM_ID = self._get_log_channels()
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
    ) -> None:
        await self.send_response(
            ctx, "Error", message, EmbedColor.ERROR, log_to_channel
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
    ) -> None:
        await self.send_response(
            ctx, "Success", message, EmbedColor.INFO, log_to_channel
        )

    async def create_review_components(
        self,
        thread: interactions.GuildPublicThread,
    ) -> Tuple[interactions.Embed, List[interactions.Button]]:
        approval_info: Approval = self.approval_counts.get(thread.id, Approval())
        approval_count: int = approval_info.approval_count
        required_count: int = (
            self.config.REQUIRED_REJECTIONS
            if approval_count >= self.config.REQUIRED_APPROVALS
            else self.config.REQUIRED_APPROVALS
        )

        reviewers_text: str = (
            ",".join(f"<@{rid}>" for rid in sorted(approval_info.reviewers, key=int))
            if approval_info.reviewers
            else "No approvals yet."
        )

        embed: interactions.Embed = await self.create_embed(
            title="Voter Identity Approval",
            description=f"- Approvals: {approval_count}/{self.config.REQUIRED_APPROVALS}\n- Reviewers: {reviewers_text}",
            color=EmbedColor.INFO,
        )

        return embed, [
            interactions.Button(style=s, label=l, custom_id=c, disabled=False)
            for s, l, c in (
                (interactions.ButtonStyle.SUCCESS, "Approve", "approve"),
                (interactions.ButtonStyle.DANGER, "Reject", "reject"),
            )
        ]

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

    # Debug commands

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
        await ctx.defer()
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

            await Paginator.create_from_embeds(self.bot, *embeds, timeout=300).send(ctx)

        except Exception as e:
            logger.error(f"Error in view_config: {e}\n{traceback.format_exc()}")
            await self.send_error(
                ctx,
                f"An unexpected error occurred while viewing the configuration: {str(e)}",
            )

    async def _get_config_data(self, config: str) -> Optional[Any]:
        return (
            self.dynamic_config
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
                                "stats": ("stats.json", Counter),
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
            color=EmbedColor.INFO,
        )
        field_count = 0
        max_fields = 25

        async def add_field(name: str, value: str, inline: bool = False) -> None:
            nonlocal current_embed, field_count
            current_embed.add_field(
                name=name,
                value=value or "*No data is currently available for this field*",
                inline=inline,
            )
            field_count += 1
            if field_count >= max_fields:
                embeds.append(current_embed)
                current_embed = await self.create_embed(
                    title=f"{config.title()} Configuration Details (Page {len(embeds) + 2})",
                    description="Continued configuration details.",
                    color=EmbedColor.INFO,
                )
                field_count = 0

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
                    True,
                )

                for category, roles in config_data.assigned_roles.items():
                    await add_field(
                        f"{category.title()} Configured Roles",
                        "\n".join(
                            f"- <@&{role_id}> (`{role}`)"
                            for role, role_id in sorted(roles.items())
                        )
                        or "*No roles have been configured for this category yet*",
                        True,
                    )

                for category, assignable_roles in config_data.assignable_roles.items():
                    await add_field(
                        f"Available {category.title()} Roles",
                        "\n".join(
                            f"- `{role}` (Available for assignment)"
                            for role in sorted(assignable_roles)
                        )
                        or "*No assignable roles configured for this category*",
                        True,
                    )

            case "custom":
                for role_name, members in config_data.items():
                    await add_field(
                        f"Custom Role: {role_name}",
                        "\n".join(f"- <@{member_id}>" for member_id in members)
                        or "*No members currently have this role*",
                        True,
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
                            True,
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
                        True,
                    )

            case "dynamic":
                for config_name, value in config_data.items():
                    await add_field(f"{config_name}", f"```py\n{value}```", True)

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
                f"Successfully {'added to' if action == 'add' else 'removed from'} custom roles: {', '.join(updated_roles)}. Use `/custom mention` to mention members with these roles.",
                log_to_channel=False,
            )
        else:
            await self.send_error(
                ctx,
                f"No changes were made because the specified roles {'already exist' if action == 'add' else 'don\'t exist'}. Please check the role names and try again.",
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
                ephemeral=True,
            )
            logger.info(f"Context menu response sent for user: {ctx.target.id}")

        except Exception as e:
            logger.error(f"Error in custom_roles_context_menu: {str(e)}")
            logger.error(traceback.format_exc())
            await self.send_error(
                ctx,
                f"An unexpected error occurred while managing custom roles. Our team has been notified: {str(e)}",
                log_to_channel=False,
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

        role_set: frozenset[str] = frozenset(map(str.strip, roles.split(",")))
        custom_roles_set: frozenset[str] = frozenset(self.custom_roles)

        found_roles: frozenset[str] = role_set & custom_roles_set
        not_found_roles: frozenset[str] = role_set - custom_roles_set

        mentioned_users: frozenset[str] = frozenset(
            f"<@{uid}>" for role in found_roles for uid in self.custom_roles[role]
        )

        if mentioned_users:
            await ctx.send(
                f"Successfully found users with the following roles: {', '.join(found_roles)}. Here are the users: {' '.join(mentioned_users)}"
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
            f"No users currently have the roles: {roles}. The roles exist, but no users are assigned to them at the moment.",
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
        required=False,
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
    )
    @interactions.slash_option(
        name="domicile",
        description="区位",
        required=False,
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
    )
    @interactions.slash_option(
        name="status",
        description="民权",
        required=False,
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
    ):
        await self._manage_vetting_roles(
            ctx, member, Action.ADD, ideology, domicile, status
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
        required=False,
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
    )
    @interactions.slash_option(
        name="domicile",
        description="区位",
        required=False,
        opt_type=interactions.OptionType.STRING,
        autocomplete=True,
    )
    @interactions.slash_option(
        name="status",
        description="民权",
        required=False,
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
    ):
        await self._manage_vetting_roles(
            ctx, member, Action.REMOVE, ideology, domicile, status
        )

    async def _manage_vetting_roles(
        self,
        ctx: interactions.SlashContext,
        member: interactions.Member,
        action: Action,
        ideology: Optional[str] = None,
        domicile: Optional[str] = None,
        status: Optional[str] = None,
    ) -> None:
        kwargs = dict(
            zip(("ideology", "domicile", "status"), (ideology, domicile, status))
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
                f"No valid roles found to {action.value.lower()}. Please specify at least one valid role type ({', '.join(role_types) if role_types else 'ideology, domicile, or status'}).",
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

    @remove_vetting_roles.autocomplete("ideology")
    async def autocomplete_ideology_remove(self, ctx: interactions.AutocompleteContext):
        await self.autocomplete_vetting_role(ctx, "ideology")

    @remove_vetting_roles.autocomplete("domicile")
    async def autocomplete_domicile_remove(self, ctx: interactions.AutocompleteContext):
        await self.autocomplete_vetting_role(ctx, "domicile")

    @remove_vetting_roles.autocomplete("status")
    async def autocomplete_status_remove(self, ctx: interactions.AutocompleteContext):
        await self.autocomplete_vetting_role(ctx, "status")

    async def autocomplete_vetting_role(
        self, ctx: interactions.AutocompleteContext, role_type: str
    ) -> None:
        if not (roles := getattr(self, "vetting_roles", None)) or role_type not in (
            assigned := roles.assigned_roles
        ):
            await ctx.send([])
            return

        user_input = ctx.input_text.casefold()
        await ctx.send(
            tuple(
                interactions.SlashCommandChoice(name=name, value=name)
                for name in itertools.islice(
                    filter(lambda x: user_input in x.casefold(), assigned[role_type]),
                    25,
                )
            )
        )

    def get_role_ids_to_assign(self, kwargs: Dict[str, str]) -> Set[int]:
        return {
            role_id
            for param, value in kwargs.items()
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
        lock_info = self.member_lock_map.setdefault(
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
            now = datetime.now(timezone.utc)
            self.member_lock_map = {
                mid: info
                for mid, info in self.member_lock_map.items()
                if isinstance(info["last_used"], datetime)
                if (now - info["last_used"]).total_seconds() <= 3600
            }

    async def process_approval_status_change(
        self, ctx: interactions.ComponentContext, status: Status
    ) -> Optional[str]:
        if not (await self.validate_context(ctx)):
            return None

        if not isinstance(
            thread := await self.bot.fetch_channel(ctx.channel_id),
            interactions.GuildPublicThread,
        ):
            raise ValueError("Invalid context: Must be used in threads")

        guild = await self.bot.fetch_guild(thread.guild.id)
        if not (member := await guild.fetch_member(thread.owner_id)):
            await self.send_error(ctx, "Member not found in server")
            return None

        async with self.member_lock(member.id):
            try:
                if not self.validate_roles(
                    roles := await self.fetch_required_roles(guild)
                ):
                    await self.send_error(ctx, "Required roles configuration invalid")
                    return None

                current_roles = frozenset(map(lambda r: r.id, member.roles))
                thread_approvals = self.get_thread_approvals(thread.id)

                if not await self.validate_reviewer(ctx, thread_approvals):
                    return None

                handler = {
                    Status.APPROVED: self.process_approval,
                    Status.REJECTED: self.process_rejection,
                }.get(status)

                if not handler:
                    await self.send_error(ctx, "Invalid status")
                    return None

                return await handler(
                    ctx, member, roles, current_roles, thread_approvals, thread
                )

            except Exception as e:
                logger.exception(f"Status change failed: {e}")
                await self.send_error(ctx, "Processing error occurred")
                return None
            finally:
                await self.update_review_components(ctx, thread)

    async def validate_context(self, ctx: interactions.ComponentContext) -> bool:
        if not self.validate_vetting_permissions(ctx):
            await self.send_error(ctx, "Insufficient permissions", log_to_channel=False)
            return False
        return True

    async def validate_reviewer(
        self, ctx: interactions.ComponentContext, thread_approvals: Approval
    ) -> bool:
        if ctx.author.id in thread_approvals.reviewers:
            await self.send_error(ctx, "Duplicate vote detected", log_to_channel=False)
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

        electoral_id = roles["electoral"].id
        if electoral_id in current_roles:
            await self.send_error(
                ctx,
                f"{member.mention} already has the electoral role and cannot be approved again.",
            )
            logger.info(f"Member {member.id} already has electoral role")
            return "Approval aborted: Already approved"

        thread_approvals = Approval(
            approval_count=min(
                thread_approvals.approval_count + 1, self.config.REQUIRED_APPROVALS
            ),
            reviewers=thread_approvals.reviewers | {ctx.author.id},
            last_approval_time=thread_approvals.last_approval_time,
        )
        self.approval_counts[thread.id] = thread_approvals

        if thread_approvals.approval_count == self.config.REQUIRED_APPROVALS:
            thread_approvals.last_approval_time = datetime.now(timezone.utc)
            await self.update_member_roles(
                member, roles["electoral"], roles["approved"], current_roles
            )
            await self.send_approval_notification(thread, member, thread_approvals)
            self.cleanup_approval_data(thread.id)
            return f"Approved {member.mention} with role updates"

        remaining = self.config.REQUIRED_APPROVALS - thread_approvals.approval_count
        await self.send_success(
            ctx,
            f"Your approval for {member.mention} has been registered. Current approval status: {thread_approvals.approval_count}/{self.config.REQUIRED_APPROVALS} approvals needed. Waiting for {remaining} more approval(s).",
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
        electoral_id: int = roles["electoral"].id
        if electoral_id not in current_roles:
            await self.send_error(
                ctx,
                f"Unable to reject {member.mention} as they have not yet been approved. Members must first be approved before they can be rejected.",
            )
            return "Rejection aborted: Not approved"

        if thread_approvals.last_approval_time and self.is_rejection_window_closed(
            thread_approvals
        ):
            await self.send_error(
                ctx,
                f"The rejection window for {member.mention} has expired. Rejections must be submitted within {self.config.REJECTION_WINDOW_DAYS} days of approval. Please contact an administrator if you believe this is in error.",
            )
            return "Rejection aborted: Window closed"

        thread_approvals = Approval(
            approval_count=thread_approvals.approval_count,
            rejection_count=min(
                thread_approvals.rejection_count + 1, self.config.REQUIRED_REJECTIONS
            ),
            reviewers=thread_approvals.reviewers | {ctx.author.id},
            last_approval_time=thread_approvals.last_approval_time,
        )
        self.approval_counts[thread.id] = thread_approvals

        if thread_approvals.rejection_count == self.config.REQUIRED_REJECTIONS:
            await self.update_member_roles(
                member, roles["approved"], roles["electoral"], current_roles
            )
            await self.send_rejection_notification(thread, member, thread_approvals)
            self.cleanup_approval_data(thread.id)
            return f"Rejected {member.mention} with role updates"

        remaining: int = (
            self.config.REQUIRED_REJECTIONS - thread_approvals.rejection_count
        )
        await self.send_success(
            ctx,
            f"Your rejection vote for {member.mention} has been registered. Current status: {thread_approvals.rejection_count}/{self.config.REQUIRED_REJECTIONS} rejections needed. Waiting for {remaining} more rejection(s) to complete the process.",
            log_to_channel=False,
        )
        return "Rejection registered"

    def is_rejection_window_closed(self, thread_approvals: Approval) -> bool:
        if not thread_approvals.last_approval_time:
            return False
        time_diff = datetime.now(timezone.utc) - thread_approvals.last_approval_time
        return time_diff.total_seconds() > self.config.REJECTION_WINDOW_DAYS * 86400

    def cleanup_approval_data(self, thread_id: int) -> None:
        self.approval_counts.pop(thread_id, None)
        self.processed_thread_ids.discard(thread_id)

    async def fetch_required_roles(
        self, guild: interactions.Guild
    ) -> Dict[str, Optional[interactions.Role]]:
        return {
            "electoral": await guild.fetch_role(self.config.ELECTORAL_ROLE_ID),
            "approved": await guild.fetch_role(self.config.APPROVED_ROLE_ID),
        }

    def get_thread_approvals(self, thread_id: int) -> Approval:
        return self.approval_counts.get(thread_id, Approval())

    async def update_member_roles(
        self,
        member: interactions.Member,
        role_to_add: interactions.Role,
        role_to_remove: interactions.Role,
        current_roles: FrozenSet[int],
    ) -> bool:
        async def verify_roles(member_to_check: interactions.Member) -> set[int]:
            return {
                role.id
                for role in (
                    await member_to_check.guild.fetch_member(member_to_check.id)
                ).roles
            }

        async def execute_role_updates(
            target: interactions.Member,
            *,
            to_add: interactions.Role | None = None,
            to_remove: interactions.Role | None = None,
        ) -> None:
            if to_remove and to_remove.id in {role.id for role in target.roles}:
                logger.info(f"Removing role {to_remove.id} from member {target.id}")
                await target.remove_roles([to_remove])
            if to_add and to_add.id not in {role.id for role in target.roles}:
                logger.info(f"Adding role {to_add.id} to member {target.id}")
                await target.add_roles([to_add])

        logger.info(
            f"Initiating role update for member {member.id}. Current roles: {current_roles}. Role to add: {role_to_add.id}, Role to remove: {role_to_remove.id}"
        )

        try:
            updates = (
                (
                    ("remove", role_to_remove)
                    if role_to_remove.id in current_roles
                    else None
                ),
                ("add", role_to_add) if role_to_add.id not in current_roles else None,
            )
            updates_needed = [u for u in updates if u]

            if updates_needed:
                for action, role in updates_needed:
                    await execute_role_updates(
                        member,
                        to_add=role if action == "add" else None,
                        to_remove=role if action == "remove" else None,
                    )

            MAX_RETRIES = 3
            backoff = (0.5, 1.0, 2.0)

            for attempt in range(MAX_RETRIES):
                final_roles = await verify_roles(member)
                logger.info(
                    f"Verification attempt {attempt + 1} - Current roles: {final_roles}"
                )

                if (
                    role_to_add.id in final_roles
                    and role_to_remove.id not in final_roles
                ):
                    logger.info("Role update successful and verified")
                    return True

                if attempt < MAX_RETRIES - 1:
                    logger.warning(
                        f"Role verification failed on attempt {attempt + 1}, retrying. Expected: +{role_to_add.id}, -{role_to_remove.id}. Current: {final_roles}"
                    )
                    await execute_role_updates(
                        member,
                        to_add=(
                            role_to_add if role_to_add.id not in final_roles else None
                        ),
                        to_remove=(
                            role_to_remove if role_to_remove.id in final_roles else None
                        ),
                    )
                    await asyncio.sleep(backoff[attempt])

            logger.error("Role update failed after all retry attempts")
            return False

        except Exception as e:
            logger.error(
                f"Critical error during role update for member {member.id}: {type(e).__name__}: {str(e)}"
            )
            return False

    async def send_approval_notification(
        self,
        thread: interactions.GuildPublicThread,
        member: interactions.Member,
        thread_approvals: Approval,
    ) -> None:
        await self.send_success(
            thread,
            f"{member.mention} approved by {''.join(f'<@{rid}>' + (',' if i < len(thread_approvals.reviewers)-1 else '') for i, rid in enumerate(thread_approvals.reviewers))}. The request has been successfully approved. The member's roles will be updated accordingly.",
            log_to_channel=False,
        )

    async def send_rejection_notification(
        self,
        thread: interactions.GuildPublicThread,
        member: interactions.Member,
        thread_approvals: Approval,
    ) -> None:
        reviewers_list = sorted(thread_approvals.reviewers)
        reviewers_text = (
            f"<@{reviewers_list[0]}>"
            if len(reviewers_list) == 1
            else ", ".join(f"<@{rid}>" for rid in reviewers_list[:-1])
            + f", <@{reviewers_list[-1]}>"
        )

        await self.send_success(
            thread,
            f"{member.mention} rejected by {reviewers_text}",
            log_to_channel=False,
        )

    # Servant commands

    @module_group_servant.subcommand("view", sub_cmd_description="Servant Directory")
    async def view_servant_roles(self, ctx: interactions.SlashContext) -> None:
        await ctx.defer()

        filtered_roles = self.filter_roles(tuple(ctx.guild.roles))
        role_members_list = self.extract_role_members_list(filtered_roles)

        if not role_members_list:
            await self.send_error(ctx, "No matching roles found.")
            return

        total_members = sum(rm.member_count for rm in role_members_list)

        title = f"Servant Directory ({total_members} members)"

        embeds = []
        current_embed = await self.create_embed(
            title=title, description="", color=EmbedColor.INFO
        )
        field_count = 0

        for role_member in role_members_list:
            members_str = "\n".join([f"- {m}" for m in role_member.members])

            if not members_str:
                continue

            if field_count >= 25:
                embeds.append(current_embed)
                current_embed = await self.create_embed(
                    title=title, description="", color=EmbedColor.INFO
                )
                field_count = 0

            current_embed.add_field(
                name=f"{role_member.role_name} ({role_member.member_count} members)",
                value=members_str,
                inline=True,
            )
            field_count += 1

        if field_count:
            embeds.append(current_embed)

        await Paginator.create_from_embeds(self.bot, *embeds, timeout=300).send(ctx)

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
    @lru_cache(maxsize=128)
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
                role
                for role_key, role_id in role_ids.items()
                if role_key != "incarcerated"
                and (role := roles.get(role_key))
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
                    f"Removed roles {[r.id for r in roles_to_remove]} from {member}"
                )

            if incarcerated_role:
                await member.add_roles((incarcerated_role,))
                logger.info(
                    f"Added incarcerated role {incarcerated_role.id} to {member}"
                )

        except Exception as e:
            logger.error(f"Error assigning roles during incarceration: {e}")
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
        await self.send_success(ctx, log_message)

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
        await self.send_success(ctx, log_message)

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
        current_time: int = int(time.time())

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
                logger.error(f"Failed to fetch {key} role (ID: {role_id}): {e}")
                continue

        return roles

    # Events

    @interactions.listen(MessageCreate)
    async def on_message_create(self, event: MessageCreate) -> None:
        if event.message.author.bot:
            return

        member_id = event.message.author.id
        member = await event.message.guild.fetch_member(member_id)

        if not any(role.id == self.config.TEMPORARY_ROLE_ID for role in member.roles):
            return

        author_id: str = str(member_id)
        message_content: str = event.message.content.strip()
        current_time: float = time.monotonic()

        async with self._stats_lock:
            user_stats = await self._process_user_stats(
                author_id, message_content, current_time
            )
            await self._debounce_save_stats()

    async def _process_user_stats(
        self, author_id: str, message_content: str, current_time: float
    ) -> Dict[str, Any]:
        default_stats = {
            "message_timestamps": [],
            "last_message": 0,
            "repetition_count": 0,
            "invalid_message_count": 0,
            "feedback_score": 0,
            "violation_history": [],
            "recovery_streaks": 0,
        }

        if author_id not in self.stats:
            self.stats[author_id] = default_stats.copy()

        user_stats: Dict[str, Any] = self.stats[author_id]
        for key, value in default_stats.items():
            if key not in user_stats:
                user_stats[key] = value

        message_rate_window = self.dynamic_config["MESSAGE_RATE_WINDOW"]
        short_window, long_window = (
            message_rate_window * 0.25,
            message_rate_window * 2.0,
        )
        cutoff_short, cutoff_long = (
            current_time - short_window,
            current_time - long_window,
        )

        message_timestamps = user_stats["message_timestamps"]
        recent_msgs = [ts for ts in message_timestamps if ts > cutoff_short]
        historical_msgs = [
            ts for ts in message_timestamps if cutoff_long < ts <= cutoff_short
        ]

        user_stats["message_timestamps"] = [
            *historical_msgs,
            *recent_msgs,
            current_time,
        ]

        classification = await self._classify_message_advanced(
            message_content.lower(), user_stats
        )
        is_invalid = classification["is_invalid"]
        invalid_message_count = user_stats["invalid_message_count"]
        feedback_score = user_stats["feedback_score"]
        recovery_streaks = user_stats["recovery_streaks"]

        if is_invalid:
            reasons = classification["reasons"]
            user_stats["invalid_message_count"] = invalid_message_count + 1
            user_stats["recovery_streaks"] = 0

            violation_history = user_stats.setdefault("violation_history", [])
            violation_history.append(
                {
                    "timestamp": current_time,
                    "reasons": reasons,
                    "message_rate": (
                        len(recent_msgs) / short_window if short_window else 0
                    ),
                }
            )
            user_stats["violation_history"] = violation_history[-10:]

            logger.warning(
                f"Message violation detected for user {author_id}: {','.join(reasons if isinstance(reasons, list) else [])}",
                extra={
                    "user_id": author_id,
                    "violations": reasons,
                    "message_stats": user_stats,
                },
            )

            repeated_violations = sum(
                bool(
                    set(v["reasons"] if isinstance(v["reasons"], list) else [])
                    & set(reasons if isinstance(reasons, list) else [])
                )
                for v in violation_history[-3:]
            )
            base_penalty = len(reasons) if isinstance(reasons, list) else 0
            pattern_multiplier = 1 + (repeated_violations * 0.5)

            user_stats["feedback_score"] = max(
                -5, feedback_score - (base_penalty * pattern_multiplier)
            )
            await self._adjust_thresholds_dynamic(user_stats)
        else:
            user_stats["invalid_message_count"] = max(0, invalid_message_count - 1)
            user_stats["recovery_streaks"] = recovery_streaks + 1
            user_stats["feedback_score"] = min(
                5, feedback_score + min(1 + (recovery_streaks * 0.2), 2)
            )

        return user_stats

    async def _classify_message_advanced(
        self, message: str, user_stats: Dict[str, Any]
    ) -> Dict[str, Union[bool, List[str]]]:
        result = Message(message, user_stats, self.dynamic_config).analyze()
        return {
            "is_invalid": bool(result),
            "reasons": result if isinstance(result, list) else [],
        }

    async def _adjust_thresholds_dynamic(self, user_stats: Dict[str, Any]) -> None:
        feedback = user_stats.get("feedback_score", 0)
        message_count = len(user_stats.get("message_timestamps", []))
        invalid_count = user_stats.get("invalid_message_count", 0)

        confidence = min(1.0, message_count / 100)
        violation_ratio = invalid_count / max(1, message_count)
        base_factor = 0.01 * (1 + confidence)
        adj_factor = (
            base_factor
            * feedback
            * math.tanh(abs(feedback) / 5)
            * (1.5 if violation_ratio > 0.3 else 1.0)
        )

        time_delta = time.time() - user_stats.get("last_threshold_adjustment", 0)
        decay_factor = math.exp(-time_delta / 3600)

        logger.info(
            f"Adjusting thresholds - feedback: {feedback}, confidence: {confidence:.2f}, "
            f"violation_ratio: {violation_ratio:.2f}, adjustment: {adj_factor:.4f}"
        )

        default_thresholds = {"DIGIT_THRESHOLD": 0.5, "MIN_ENTROPY_THRESHOLD": 1.5}
        thresholds = (
            ("DIGIT_THRESHOLD", 0.1, 1.0),
            ("MIN_ENTROPY_THRESHOLD", 0.0, 4.0),
        )

        alpha = 0.3 * confidence
        for name, min_val, max_val in thresholds:
            old_val = self.dynamic_config[name]
            default = default_thresholds[name]
            decay_adjustment = (default - old_val) * (1 - decay_factor)
            new_val = min(
                max(old_val + adj_factor + decay_adjustment, min_val), max_val
            )
            self.dynamic_config[name] = old_val * (1 - alpha) + new_val * alpha

            logger.info(
                f"Adjusted {name}: {old_val:.4f} -> {new_val:.4f} "
                f"(default: {default}, decay: {decay_adjustment:.4f})"
            )

        user_stats["last_threshold_adjustment"] = time.time()

    async def _debounce_save_stats(self) -> None:
        if self._save_stats_task:
            self._save_stats_task.cancel()
        self._save_stats_task = asyncio.create_task(
            self._delayed_save_stats(),
            name=f"save_stats_{id(self)}_{time.monotonic_ns()}",
        )

    async def _delayed_save_stats(self) -> None:
        try:
            await asyncio.sleep(5.0)
            async with self._stats_lock:
                await self.save_stats_roles()
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error(f"Error in _delayed_save_stats: {e}", exc_info=True)
        finally:
            object.__setattr__(self, "_save_stats_task", None)

    @interactions.listen(ExtensionLoad)
    async def on_extension_load(self, event: ExtensionLoad) -> None:
        self.update_roles_based_on_activity.start()
        self.cleanup_old_locks.start()
        self.check_incarcerated_members.start()

    @interactions.listen(ExtensionUnload)
    async def on_extension_unload(self, event: ExtensionUnload) -> None:
        tasks_to_stop: tuple = (
            self.update_roles_based_on_activity,
            self.cleanup_old_locks,
            self.check_incarcerated_members,
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
        thread = event.thread
        if not isinstance(thread := event.thread, interactions.GuildPublicThread) or (
            (thread.parent_id != self.config.VETTING_FORUM_ID)
            | (thread.id in self.processed_thread_ids)
            | (thread.owner_id is None)
        ):
            return

        try:
            await self.handle_new_thread(thread)
        finally:
            self.processed_thread_ids.add(thread.id)

    # Tasks

    @interactions.Task.create(interactions.IntervalTrigger(days=7))
    async def cleanup_old_locks(self) -> None:
        current_time: datetime = datetime.now(timezone.utc)
        threshold: timedelta = timedelta(days=7)
        self.member_lock_map = {
            k: v
            for k, v in self.member_lock_map.items()
            if not (
                isinstance(v["lock"], asyncio.Lock)
                and not v["lock"].locked()
                and (current_time - v["last_used"]).total_seconds()
                > threshold.total_seconds()
            )
        }
        removed: int = len(self.member_lock_map) - len(self.member_lock_map)
        logger.info(f"Cleaned up {removed} old locks.")

    @interactions.Task.create(interactions.IntervalTrigger(seconds=30))
    async def check_incarcerated_members(self) -> None:
        self.incarcerated_members = await self.model.load_data(
            "incarcerated_members.json", dict
        )
        now: float = time.time()
        release_times: Dict[str, float] = {
            member_id: float(data["release_time"])
            for member_id, data in self.incarcerated_members.items()
        }

        release_map: Dict[str, Dict[str, Any]] = dict(
            itertools.compress(
                self.incarcerated_members.items(),
                (t <= now for t in release_times.values()),
            )
        )

        schedule_map: Dict[str, Dict[str, Any]] = dict(
            itertools.compress(
                self.incarcerated_members.items(),
                (0 < t - now <= 60 for t in release_times.values()),
            )
        )

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
                valid_messages = len(stats.get("message_timestamps", [])) - stats.get(
                    "invalid_message_count", 0
                )

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
                        f"Updated roles for {member_id}: Sent {valid_messages} valid messages, "
                        f"upgraded from {roles['temporary'].name} to {roles['approved'].name}."
                    )
            if members_to_update:
                for member_id in members_to_update:
                    try:
                        if member := await guild.fetch_member(member_id):
                            if remove_roles := role_updates["remove"][int(member_id)]:
                                await member.remove_roles(remove_roles)
                            if add_roles := role_updates["add"][int(member_id)]:
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

            self.stats = {
                k: v
                for k, v in self.stats.items()
                if (
                    int(k) not in members_to_update
                    or len(v.get("message_timestamps", [])) < 5
                )
            }
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

            review_task = asyncio.create_task(self._send_review_components(thread))
            notify_task = asyncio.create_task(
                self.notify_vetting_reviewers(
                    self.config.VETTING_ROLE_IDS, thread, timestamp
                )
            )

            await review_task
            await notify_task

        except Exception as e:
            logger.exception(f"Error processing new post: {e!r}")

    async def _send_review_components(
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
        action: Literal["add", "remove"] = ctx.values[0]

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
                    max_values=1,
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

            if updated_roles := await self.update_custom_roles(
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
            logger.error(f"Failed to save custom roles: {e}")
            raise

    async def save_stats_roles(self) -> None:
        try:
            await self.model.save_data("stats.json", dict(self.stats))
            logger.info(f"Stats saved successfully: {self.stats!r}")
        except Exception as e:
            logger.error(f"Failed to save stats roles: {e!r}", exc_info=True)
            raise

    async def save_incarcerated_members(self) -> None:
        try:
            await self.model.save_data(
                "incarcerated_members.json", dict(self.incarcerated_members)
            )
        except Exception as e:
            logger.error(f"Failed to save incarcerated members: {e!r}")
            raise
