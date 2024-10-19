import asyncio
import logging
import os
import re
import time
import traceback
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from functools import lru_cache, partial, wraps
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import (
    Any,
    Callable,
    Concatenate,
    Coroutine,
    Dict,
    Final,
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
)

import aiofiles
import cysimdjson
import interactions
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
from pydantic import BaseModel, Field, ValidationError
from yarl import URL

BASE_DIR: Final[str] = os.path.dirname(__file__)
LOG_FILE: Final[str] = os.path.join(BASE_DIR, "roles.log")

logger: Final[logging.Logger] = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler: Final[RotatingFileHandler] = RotatingFileHandler(
    LOG_FILE, maxBytes=1 * 1024 * 1024, backupCount=1
)
file_handler.setLevel(logging.DEBUG)

formatter: Final[logging.Formatter] = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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
    assigned_roles: Dict[str, Dict[str, int]] = Field(default_factory=dict)
    authorized_roles: Dict[str, int] = Field(default_factory=dict)
    assignable_roles: Dict[str, List[str]] = Field(default_factory=dict)
    incarcerated_members: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    class Config:
        json_encoders = {
            set: list,
        }


@dataclass(frozen=True)
class Config:
    VETTING_FORUM_ID: Final[int] = 1164834982737489930
    VETTING_ROLE_IDS: Final[List[int]] = field(
        default_factory=lambda: [1200066469300551782]
    )
    ELECTORAL_ROLE_ID: Final[int] = 1200043628899356702
    APPROVED_ROLE_ID: Final[int] = 1282944839679344721
    TEMPORARY_ROLE_ID: Final[int] = 1164761892015833129
    INCARCERATED_ROLE_ID: Final[int] = 1247284720044085370
    AUTHORIZED_CUSTOM_ROLE_IDS: Final[List[int]] = field(
        default_factory=lambda: [1213490790341279754]
    )
    AUTHORIZED_PENITENTIARY_ROLE_IDS: Final[List[int]] = field(
        default_factory=lambda: [1200097748259717193, 1247144717083476051]
    )
    REQUIRED_APPROVALS: Final[int] = 3
    REQUIRED_REJECTIONS: Final[int] = 3
    REJECTION_WINDOW_DAYS: Final[int] = 7
    LOG_CHANNEL_ID: Final[int] = 1166627731916734504
    LOG_FORUM_ID: Final[int] = 1159097493875871784
    LOG_POST_ID: Final[int] = 1279118293936111707
    GUILD_ID: Final[int] = 1150630510696075404


@dataclass
class Servant:
    role_name: str
    members: List[str]
    member_count: int


@dataclass
class ApprovalInfo:
    approval_count: int = 0
    rejection_count: int = 0
    reviewers: Set[int] = field(default_factory=set)
    last_approval_time: Optional[datetime] = None


class Model(Generic[T]):
    def __init__(self):
        self.base_path: Final[URL] = URL(str(Path(__file__).parent))
        self._data_cache: Dict[str, Any] = {}
        self.parser: Final[cysimdjson.JSONParser] = cysimdjson.JSONParser()
        self._file_locks: Dict[str, asyncio.Lock] = {}
        self._executor = ThreadPoolExecutor(max_workers=4)

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
                        logging.warning(
                            f"Attempt {attempt + 1} failed: {e}. Retrying..."
                        )
                        await asyncio.sleep(delay * (2**attempt))

            return wrapper

        return decorator

    async def _get_file_lock(self, file_name: str) -> asyncio.Lock:
        return self._file_locks.setdefault(file_name, asyncio.Lock())

    @asynccontextmanager
    async def _file_operation(self, file_path: Path, mode: str):
        async with await self._get_file_lock(str(file_path)):
            try:
                async with aiofiles.open(str(file_path), mode) as file:
                    yield file
            except IOError as e:
                logging.error(f"IO operation failed for {file_path}: {e}")
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
                    self._executor, orjson.loads, content
                )
            except orjson.JSONDecodeError:
                logging.warning(
                    f"`orjson` failed to parse {file_name}, falling back to `cysimdjson`"
                )
                json_parsed = await loop.run_in_executor(
                    self._executor, self.parser.parse, content
                )

            if file_name == "custom.json":
                instance = {role: set(members) for role, members in json_parsed.items()}
            elif issubclass(model, BaseModel):
                try:
                    instance = await loop.run_in_executor(
                        self._executor, model.model_validate, json_parsed
                    )
                except ValidationError as ve:
                    logging.error(f"Validation error for {file_name}: {ve}")
                    raise
            elif model == Counter:
                instance = Counter(json_parsed)
            else:
                instance = json_parsed

            self._data_cache[file_name] = instance
            logging.info(f"Successfully loaded data from {file_name}")
            return instance
        except FileNotFoundError:
            logging.info(f"{file_name} not found. Creating a new one.")
            if issubclass(model, BaseModel):
                instance = model()
            elif model == Counter:
                instance = Counter()
            else:
                instance = {}

            await self.save_data(file_name, instance)
            return instance
        except Exception as e:
            error_msg = f"Unexpected error loading {file_name}: {e}"
            logging.error(error_msg)
            raise ValueError(error_msg)

    @async_retry(max_retries=3, delay=1.0)
    async def save_data(self, file_name: str, data: T) -> None:
        file_path = self.base_path / file_name
        try:
            loop = asyncio.get_running_loop()
            json_data = await loop.run_in_executor(
                self._executor,
                partial(
                    orjson.dumps,
                    data.dict() if hasattr(data, "dict") else data,
                    option=orjson.OPT_INDENT_2 | orjson.OPT_SERIALIZE_NUMPY,
                ),
            )

            async with self._file_operation(file_path, "wb") as file:
                await file.write(json_data)

            self._data_cache[file_name] = data
            logging.info(f"Successfully saved data to {file_name}")
        except Exception as e:
            logging.error(f"Error saving {file_name}: {e}")
            raise

    def __del__(self):
        self._executor.shutdown(wait=True)


# Controller


class Roles(interactions.Extension):
    def __init__(self, bot: interactions.Client):
        self.bot: Final[interactions.Client] = bot
        self.config: Final[Config] = Config()
        self.vetting_roles: Data = Data()
        self.custom_roles: Dict[str, Set[int]] = {}
        self.incarcerated_members: Dict[str, Dict[str, Any]] = {}
        self.stats: Final[Counter[str]] = Counter()
        self.processed_thread_ids: Set[int] = set()
        self.approval_counts: Dict[int, ApprovalInfo] = {}
        self.member_lock_map: Dict[int, Dict[str, Union[asyncio.Lock, datetime]]] = {}
        self._stats_lock: Final[asyncio.Lock] = asyncio.Lock()
        self._save_stats_task: asyncio.Task | None = None

        self.cache = TTLCache(maxsize=100, ttl=300)

        self.base_path: Final[Path] = Path(__file__).parent
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
                result = await asyncio.shield(func(self, ctx, *args, **kwargs))
                logger.info(f"`{func.__name__}` completed successfully: {result}")
                return result
            except asyncio.CancelledError:
                logger.warning(f"{func.__name__} was cancelled")
                raise
            except Exception as e:
                error_msg = f"Error in {func.__name__}: {e!r}\n{traceback.format_exc()}"
                logger.exception(error_msg)
                with suppress(Exception):
                    await asyncio.shield(
                        self.send_error(ctx, f"An error occurred: {e!s}")
                    )
                raise

        return wrapper

    # Validators

    @staticmethod
    def create_role_validator(
        role_ids_func: Callable[[Any], Set[int]]
    ) -> Callable[[Any, interactions.ContextType], bool]:
        return lambda self, ctx: bool(set(ctx.author.roles) & role_ids_func(self))

    def get_assignable_role_ids(self) -> frozenset[int]:
        return frozenset(
            role_id
            for roles in self.vetting_roles.assigned_roles.values()
            for role_id in roles.values()
            if any(
                name in assignable_roles
                for assignable_roles in self.vetting_roles.assignable_roles.values()
                for name in roles.keys()
            )
        )

    def validate_vetting_permissions(self, ctx: interactions.ContextType) -> bool:
        user_role_ids = set(role.id for role in ctx.author.roles)
        vetting_role_ids = set(self.config.VETTING_ROLE_IDS)
        common_roles = user_role_ids & vetting_role_ids

        result = bool(common_roles)

        return result

    def validate_vetting_permissions_with_roles(
        self, ctx: interactions.ContextType, role_ids_to_add: Iterable[int]
    ) -> bool:
        has_vetting_permission = self.validate_vetting_permissions(ctx)
        assignable_roles = self.get_assignable_role_ids()
        roles_are_assignable = frozenset(role_ids_to_add).issubset(assignable_roles)

        return has_vetting_permission and roles_are_assignable

    get_custom_role_ids: Callable[[], frozenset[int]] = lambda self: frozenset(
        self.config.AUTHORIZED_CUSTOM_ROLE_IDS
    )

    validate_custom_permissions: Callable[[Any, interactions.ContextType], bool] = (
        create_role_validator(get_custom_role_ids)
    )

    validate_penitentiary_permissions: Callable[
        [Any, interactions.ContextType], bool
    ] = create_role_validator(
        lambda self: frozenset(self.config.AUTHORIZED_PENITENTIARY_ROLE_IDS)
    )

    @lru_cache(maxsize=128)
    def _get_category_role_ids(self, category: str) -> FrozenSet[int]:
        cache_key = f"category_role_ids_{category}"
        if cache_key not in self.cache:
            self.cache[cache_key] = frozenset(
                self.vetting_roles.assigned_roles.get(category, {}).values()
            )
        return self.cache[cache_key]

    async def check_role_assignment_conflicts(
        self,
        ctx: interactions.SlashContext,
        member: interactions.Member,
        role_ids_to_add: Iterable[int],
    ) -> bool:
        member_current_roles: FrozenSet[int] = frozenset(
            role.id for role in member.roles
        )
        roles_to_add_set: FrozenSet[int] = frozenset(role_ids_to_add)

        for category in self.vetting_roles.assigned_roles:
            category_role_ids = self._get_category_role_ids(category)
            existing_category_roles = member_current_roles & category_role_ids
            new_category_roles = roles_to_add_set & category_role_ids

            if existing_category_roles and new_category_roles:
                total_category_roles = existing_category_roles | new_category_roles
                if len(total_category_roles) > 1:
                    await self.send_error(
                        ctx,
                        f"Conflicting roles detected in the {category} category. "
                        f"Member already has {len(existing_category_roles)} role(s) "
                        f"and is attempting to add {len(new_category_roles)} role(s).",
                    )
                    return True

        return False

    # View methods

    async def create_embed(
        self, title: str, description: str = "", color: EmbedColor = EmbedColor.INFO
    ) -> interactions.Embed:
        embed = interactions.Embed(
            title=title, description=description, color=color.value
        )
        guild: Optional[interactions.Guild] = await self.bot.fetch_guild(
            self.config.GUILD_ID
        )
        if guild and guild.icon:
            embed.set_footer(
                text=guild.name, icon_url=guild.icon.url if guild.icon else None
            )
        embed.timestamp = datetime.now(timezone.utc)
        embed.set_footer(text="鍵政大舞台")
        return embed

    async def notify_vetting_reviewers(
        self,
        reviewer_role_ids: List[int],
        thread: interactions.GuildPublicThread,
        timestamp: str,
    ) -> None:
        guild = await self.bot.fetch_guild(thread.guild.id)
        if guild is None:
            logger.error(f"Guild with ID {thread.guild.id} could not be fetched.")
            raise ValueError(f"Guild with ID {thread.guild.id} could not be fetched.")

        embed = await self.create_embed(
            title=f"Voter Identity Approval #{timestamp}",
            description=f"[Click to jump: {thread.name}](https://discord.com/channels/{thread.guild.id}/{thread.id})",
            color=EmbedColor.INFO,
        )

        async def process_role(role_id: int) -> None:
            try:
                role = await guild.fetch_role(role_id)
                if role is None:
                    logger.error(f"Reviewer role with ID {role_id} not found.")
                    return

                send_tasks = [
                    self.send_direct_message(member, embed) for member in role.members
                ]
                await asyncio.gather(*send_tasks)
                logger.info(
                    f"Notifications sent to role ID {role_id} in thread {thread.id}"
                )
            except Exception as e:
                logger.error(f"Error processing role {role_id}: {e}")

        await asyncio.gather(*(process_role(role_id) for role_id in reviewer_role_ids))
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
        embed: Final[interactions.Embed] = await self.create_embed(
            title, message, color
        )

        tasks: list[Coroutine] = []

        if ctx:
            tasks.append(ctx.send(embed=embed, ephemeral=True))

        if log_to_channel:
            LOG_CHANNEL_ID, LOG_POST_ID, LOG_FORUM_ID = self._get_log_channels()
            tasks.append(self.send_to_channel(LOG_CHANNEL_ID, embed))
            tasks.append(self.send_to_forum_post(LOG_FORUM_ID, LOG_POST_ID, embed))

        await asyncio.gather(*tasks, return_exceptions=True)

    async def send_to_channel(self, channel_id: int, embed: interactions.Embed) -> None:
        try:
            channel = await self.bot.fetch_channel(channel_id)
            if isinstance(channel, interactions.GuildText):
                await channel.send(embed=embed)
            else:
                logger.error(f"Channel ID {channel_id} is not a valid text channel.")
        except NotFound:
            logger.error(f"Channel with ID {channel_id} not found.")
        except Exception as e:
            logger.error(f"Error sending message to channel {channel_id}: {e}")

    async def send_to_forum_post(
        self, forum_id: int, post_id: int, embed: interactions.Embed
    ) -> None:
        try:
            forum = await self.bot.fetch_channel(forum_id)
            if isinstance(forum, interactions.GuildForum):
                thread = await forum.fetch_post(post_id)
                if isinstance(thread, interactions.GuildPublicThread):
                    await thread.send(embed=embed)
                else:
                    logger.error(f"Post with ID {post_id} is not a valid thread.")
            else:
                logger.error(f"Channel ID {forum_id} is not a valid forum channel.")
        except NotFound:
            logger.error(
                f"Forum or post not found. Forum ID: {forum_id}, Post ID: {post_id}"
            )
        except Exception as e:
            logger.error(
                f"Error sending message to forum post. Forum ID: {forum_id}, Post ID: {post_id}. Error: {e}"
            )

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
        self, thread: interactions.GuildPublicThread
    ) -> Tuple[interactions.Embed, List[interactions.Button]]:
        approval_info: ApprovalInfo = self.approval_counts.get(
            thread.id, ApprovalInfo()
        )
        approval_count: int = approval_info.approval_count
        required_count: int = (
            self.config.REQUIRED_APPROVALS
            if approval_count < self.config.REQUIRED_APPROVALS
            else self.config.REQUIRED_REJECTIONS
        )

        reviewers_text: str = (
            ", ".join(
                f"<@{reviewer_id}>" for reviewer_id in sorted(approval_info.reviewers)
            )
            or "No approvals yet."
        )

        embed: interactions.Embed = await self.create_embed(
            title="Voter Identity Approval",
            description=(
                f"**Approvals:** {approval_count}/{self.config.REQUIRED_APPROVALS}\n"
                f"**Reviewers:** {reviewers_text}"
            ),
            color=EmbedColor.INFO,
        )

        buttons: List[interactions.Button] = [
            interactions.Button(
                style=interactions.ButtonStyle.SUCCESS,
                label="Approve",
                custom_id=f"approve",
                disabled=False,
            ),
            interactions.Button(
                style=interactions.ButtonStyle.DANGER,
                label="Reject",
                custom_id=f"reject",
                disabled=False,
            ),
        ]

        return embed, buttons

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

    # Custom roles commands

    @module_group_custom.subcommand(
        "configure", sub_cmd_description="Configure custom roles"
    )
    @interactions.slash_option(
        name="roles",
        description="Roles",
        opt_type=interactions.OptionType.STRING,
        required=True,
    )
    @interactions.slash_option(
        name="action",
        description="Action",
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
        if self.validate_custom_permissions(ctx):
            return await self.send_error(
                ctx,
                "You don't have permission to use this command.",
                log_to_channel=False,
            )

        role_list = [role.strip() for role in roles.split(",")]
        updated_roles = set()

        for role in role_list:
            if action == "add":
                if role not in self.custom_roles:
                    self.custom_roles[role] = set()
                    updated_roles.add(role)
            else:
                if role in self.custom_roles:
                    del self.custom_roles[role]
                    updated_roles.add(role)

        if updated_roles:
            await self.save_custom_roles()
            action_past = "added" if action == "add" else "removed"
            await self.send_success(
                ctx,
                f"The following roles were {action_past}: {', '.join(updated_roles)}",
            )
        else:
            await self.send_error(
                ctx,
                "No changes made. The specified roles were already in the desired state.",
            )

    @interactions.user_context_menu(name="Custom Roles")
    @error_handler
    async def custom_roles_context_menu(
        self, ctx: interactions.ContextMenuContext
    ) -> None:
        try:
            logger.info(f"Context menu triggered for user: {ctx.target.id}")

            member: interactions.Member = ctx.target
            if self.validate_custom_permissions(ctx):
                logger.warning(
                    f"User {ctx.author.id} lacks permission for custom roles menu"
                )
                return await self.send_error(
                    ctx,
                    "You don't have permission to use this command.",
                    log_to_channel=False,
                )

            options = [
                interactions.StringSelectOption(label="Add", value="add"),
                interactions.StringSelectOption(label="Remove", value="remove"),
            ]

            components = interactions.StringSelectMenu(
                custom_id=f"manage_roles_menu_{member.id}",
                placeholder="Select action",
                *options,
            )

            await ctx.send(
                f"Choose custom roles for {member.mention}:",
                components=components,
                ephemeral=True,
            )
            logger.info(f"Context menu response sent for user: {ctx.target.id}")
        except Exception as e:
            logger.error(f"Error in custom_roles_context_menu: {str(e)}")
            logger.error(traceback.format_exc())
            await self.send_error(ctx, f"An error occurred: {str(e)}")

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
        if self.validate_custom_permissions(ctx):
            return await self.send_error(
                ctx,
                "You don't have permission to use this command.",
                log_to_channel=False,
            )

        role_list = [role.strip() for role in roles.split(",")]
        mentioned_users: Set[str] = set()
        found_roles: Set[str] = set()
        not_found_roles: Set[str] = set()

        for role in role_list:
            if role in self.custom_roles:
                mentioned_users.update(
                    f"<@{user_id}>" for user_id in self.custom_roles[role]
                )
                found_roles.add(role)
            else:
                not_found_roles.add(role)

        if mentioned_users:
            mention_message = f"Mentioning users with roles {', '.join(found_roles)}:\n{' '.join(mentioned_users)}"
            await ctx.send(mention_message)

        if not_found_roles:
            await self.send_error(
                ctx,
                f"The following roles were not found: {', '.join(not_found_roles)}",
            )

        if not mentioned_users and not not_found_roles:
            await self.send_error(
                ctx,
                f"No users found with the specified roles: {roles}",
            )

    @mention_custom_roles.autocomplete("roles")
    async def autocomplete_custom_roles(
        self, ctx: interactions.AutocompleteContext
    ) -> None:
        user_input = ctx.input_text.lower()
        choices = [
            interactions.SlashCommandChoice(name=role, value=role)
            for role in self.custom_roles
            if user_input in role.lower()
        ]
        await ctx.send(choices[:25])

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
        kwargs = {"ideology": ideology, "domicile": domicile, "status": status}
        role_ids = self.get_role_ids_to_assign(
            {k: v for k, v in kwargs.items() if v is not None}
        )

        if not self.validate_vetting_permissions_with_roles(ctx, role_ids):
            if not self.validate_vetting_permissions(ctx):
                await self.send_error(
                    ctx,
                    "You don't have permission to use this command.",
                    log_to_channel=False,
                )
            else:
                await self.send_error(
                    ctx, "Some of the roles you're trying to assign are not assignable."
                )
            return

        if not role_ids:
            await self.send_error(ctx, f"No roles specified for {action.value}.")
            return

        if action == Action.ADD and await self.check_role_assignment_conflicts(
            ctx, member, role_ids
        ):
            return

        manage_func = (
            self.assign_roles_to_member
            if action == Action.ADD
            else self.remove_roles_from_member
        )
        await manage_func(ctx, member, role_ids)

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
        if not self.vetting_roles or role_type not in self.vetting_roles.assigned_roles:
            await ctx.send([])
            return

        user_input = ctx.input_text.lower()
        choices = [
            interactions.SlashCommandChoice(name=name, value=name)
            for name in self.vetting_roles.assigned_roles[role_type].keys()
            if user_input in name.lower()
        ]
        await ctx.send(choices[:25])

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
        roles_to_add: List[interactions.Role] = list(
            filter(None, map(ctx.guild.get_role, role_ids_to_add))
        )
        if not roles_to_add:
            return await self.send_error(ctx, "No valid roles found to add.")

        try:
            await member.add_roles(roles_to_add)
            role_names = ", ".join(map(lambda r: r.name, roles_to_add))
            await self.send_success(
                ctx,
                f"Added the following roles to {member.user.mention}: {role_names}.",
            )
        except Exception as e:
            await self.send_error(ctx, f"Error adding roles: {str(e)}")

    async def remove_roles_from_member(
        self,
        ctx: interactions.SlashContext,
        member: interactions.Member,
        role_ids_to_remove: Set[int],
    ) -> None:
        roles_to_remove = {
            role for role in ctx.guild.roles if role.id in role_ids_to_remove
        }

        if not roles_to_remove:
            await self.send_error(ctx, "No valid roles found to remove.")
            return

        try:
            role_names = ", ".join(role.name for role in roles_to_remove)

            await asyncio.gather(
                member.remove_roles(list(roles_to_remove)),
                self.send_success(
                    ctx,
                    f"Removed the following roles from {member.mention}: {role_names}.",
                ),
                self.send_success(
                    ctx,
                    f"Moderator {ctx.author.mention} removed roles from {member.mention}:\n{role_names}",
                ),
            )
        except Exception as e:
            error_message = f"Error removing roles: {str(e)}"
            await self.send_error(ctx, error_message)
            logger.exception(
                f"Failed to remove roles from {member.id}: {error_message}"
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
    async def member_lock(self, member_id: int):
        lock_info = self.member_lock_map.setdefault(
            member_id, {"lock": asyncio.Lock(), "last_used": datetime.now(timezone.utc)}
        )
        async with lock_info["lock"]:
            lock_info["last_used"] = datetime.now(timezone.utc)
            yield

    async def process_approval_status_change(
        self, ctx: interactions.ComponentContext, status: Status
    ) -> Optional[str]:
        if not self.validate_vetting_permissions(ctx):
            await self.send_error(
                ctx,
                "You don't have permission to use this command.",
                log_to_channel=False,
            )
            return None

        thread = await self.bot.fetch_channel(ctx.channel_id)
        if not isinstance(thread, interactions.GuildPublicThread):
            raise ValueError("This callback should only be used in threads.")

        guild = await self.bot.fetch_guild(thread.guild.id)
        member = await guild.fetch_member(thread.owner_id)

        if not member:
            await self.send_error(ctx, "The member is no longer in the server.")
            return None

        async with self.member_lock(member.id):
            try:
                roles = await self.fetch_required_roles(guild)
                if not all(roles.values()):
                    await self.send_error(ctx, "Required roles not found.")
                    return None

                current_roles = {role.id for role in member.roles}
                thread_approvals = self.get_thread_approvals(thread.id)

                if ctx.author.id in thread_approvals.reviewers:
                    await self.send_error(
                        ctx,
                        "You have already voted on this thread.",
                        log_to_channel=False,
                    )
                    return None

                if status == Status.APPROVED:
                    response = await self.process_approval(
                        ctx, member, roles, current_roles, thread_approvals, thread
                    )
                elif status == Status.REJECTED:
                    response = await self.process_rejection(
                        ctx, member, roles, current_roles, thread_approvals, thread
                    )
                else:
                    await self.send_error(ctx, "Invalid status provided.")
                    return None

                return response

            except Exception as e:
                logger.exception(f"Error updating member status: {e}")
                await self.send_error(ctx, "An error occurred. Please try again later.")
                return None
            finally:
                await self.update_review_components(ctx, thread)

    async def update_review_components(
        self, ctx: interactions.ComponentContext, thread: interactions.GuildPublicThread
    ) -> None:
        try:
            embed, buttons = await self.create_review_components(thread)
            await ctx.message.edit(embed=embed, components=buttons)
        except Exception as e:
            logger.error(f"Failed to update message: {str(e)}")

    async def process_approval_status(
        self,
        ctx: interactions.ComponentContext,
        status: Status,
        member: interactions.Member,
        roles: Dict[str, interactions.Role],
        current_roles: Set[int],
        thread_approvals: ApprovalInfo,
        thread: interactions.GuildPublicThread,
    ) -> Optional[str]:
        if status == Status.APPROVED:
            return await self.process_approval(
                ctx, member, roles, current_roles, thread_approvals, thread
            )
        elif status == Status.REJECTED:
            return await self.process_rejection(
                ctx, member, roles, current_roles, thread_approvals, thread
            )
        return None

    async def process_approval(
        self,
        ctx: interactions.ComponentContext,
        member: interactions.Member,
        roles: Dict[str, interactions.Role],
        current_roles: Set[int],
        thread_approvals: ApprovalInfo,
        thread: interactions.GuildPublicThread,
    ) -> str:
        if roles["electoral"].id in current_roles:
            await self.send_error(
                ctx, "This member has already been approved.", log_to_channel=False
            )
            return "Approval aborted: Member already approved."

        thread_approvals.approval_count += 1
        thread_approvals.reviewers.add(ctx.author.id)
        self.approval_counts[thread.id] = thread_approvals

        if thread_approvals.approval_count >= self.config.REQUIRED_APPROVALS:
            await self.update_member_roles(
                member, roles["electoral"], roles["approved"], current_roles
            )
            thread_approvals.approval_count = self.config.REQUIRED_APPROVALS
            thread_approvals.last_approval_time = datetime.now(timezone.utc)
            await self.send_approval_notification(thread, member, thread_approvals)
            self.cleanup_approval_data(thread.id)
            return f"Approved {member.mention} and updated roles"
        else:
            await self.send_success(
                ctx,
                f"Approval registered. Current approvals: {thread_approvals.approval_count}/{self.config.REQUIRED_APPROVALS}",
                log_to_channel=False,
            )
            return "Approval registered successfully."

    async def process_rejection(
        self,
        ctx: interactions.ComponentContext,
        member: interactions.Member,
        roles: Dict[str, interactions.Role],
        current_roles: Set[int],
        thread_approvals: ApprovalInfo,
        thread: interactions.GuildPublicThread,
    ) -> str:
        if roles["electoral"].id not in current_roles:
            await self.send_error(
                ctx, "This member is not currently approved.", log_to_channel=False
            )
            return "Rejection aborted: Member not approved."

        if thread_approvals.last_approval_time and self.is_rejection_window_closed(
            thread_approvals
        ):
            await self.send_error(
                ctx,
                f"The {self.config.REJECTION_WINDOW_DAYS}-day window for rejection has passed.",
                log_to_channel=False,
            )
            return "Rejection aborted: Window closed."

        thread_approvals.rejection_count += 1
        thread_approvals.reviewers.add(ctx.author.id)
        self.approval_counts[thread.id] = thread_approvals

        if thread_approvals.rejection_count >= self.config.REQUIRED_REJECTIONS:
            await self.update_member_roles(
                member, roles["approved"], roles["electoral"], current_roles
            )
            thread_approvals.rejection_count = self.config.REQUIRED_REJECTIONS
            await self.send_rejection_notification(thread, member, thread_approvals)
            self.cleanup_approval_data(thread.id)
            return f"Rejected {member.mention} and updated roles"
        else:
            await self.send_success(
                ctx,
                f"Rejection registered. Current rejections: {thread_approvals.rejection_count}/{self.config.REQUIRED_REJECTIONS}",
                log_to_channel=False,
            )
            return "Rejection registered successfully."

    def is_rejection_window_closed(self, thread_approvals: ApprovalInfo) -> bool:
        return (
            datetime.now(timezone.utc) - thread_approvals.last_approval_time
        ) > timedelta(days=self.config.REJECTION_WINDOW_DAYS)

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

    def get_thread_approvals(self, thread_id: int) -> ApprovalInfo:
        return self.approval_counts.get(thread_id, ApprovalInfo())

    async def update_member_roles(
        self,
        member: interactions.Member,
        role_to_add: interactions.Role,
        role_to_remove: interactions.Role,
        current_roles: Set[int],
    ) -> bool:
        tasks = []
        if role_to_add.id not in current_roles:
            tasks.append(member.add_roles([role_to_add]))
        if role_to_remove.id in current_roles:
            tasks.append(member.remove_roles([role_to_remove]))

        if tasks:
            await asyncio.gather(*tasks)
            return True
        return False

    async def send_approval_notification(
        self,
        thread: interactions.GuildPublicThread,
        member: interactions.Member,
        thread_approvals: ApprovalInfo,
    ) -> None:
        reviewer_mentions = ", ".join(
            f"<@{reviewer_id}>" for reviewer_id in thread_approvals.reviewers
        )
        await self.send_success(
            thread,
            f"{member.mention} has been approved by {reviewer_mentions}.",
            EmbedColor.INFO,
        )

    async def send_rejection_notification(
        self,
        thread: interactions.GuildPublicThread,
        member: interactions.Member,
        thread_approvals: ApprovalInfo,
    ) -> None:
        reviewer_mentions = ", ".join(
            f"<@{reviewer_id}>" for reviewer_id in thread_approvals.reviewers
        )
        await self.send_success(
            thread,
            f"{member.mention} has been rejected by {reviewer_mentions}.",
            EmbedColor.ERROR,
        )

    # Servant commands

    @module_group_servant.subcommand("view", sub_cmd_description="Servant Directory")
    async def view_servant_roles(self, ctx: interactions.SlashContext) -> None:
        await ctx.defer()
        guild = ctx.guild
        filtered_roles = self.filter_roles(tuple(guild.roles))
        role_members_list = self.extract_role_members_list(filtered_roles)
        total_members = sum(
            role_member.member_count for role_member in role_members_list
        )

        embeds = []
        current_embed = await self.create_embed(
            title=f"Servant Directory ({total_members} members)",
            description="",
            color=EmbedColor.INFO,
        )
        field_count = 0

        for role_member in role_members_list:
            members_str = "\n".join(f"- {member}" for member in role_member.members)
            if members_str:
                if field_count >= 25:
                    embeds.append(current_embed)
                    current_embed = await self.create_embed(
                        title=f"Servant Directory ({total_members} members)",
                        description="",
                        color=EmbedColor.INFO,
                    )
                    field_count = 0

                current_embed.add_field(
                    name=f"{role_member.role_name} ({role_member.member_count} members)",
                    value=members_str,
                    inline=True,
                )
                field_count += 1

        if field_count > 0:
            embeds.append(current_embed)

        if not embeds:
            await self.send_error(ctx, "No matching roles found.")
            return

        paginator = Paginator.create_from_embeds(self.bot, *embeds, timeout=300)
        await paginator.send(ctx)

    @staticmethod
    @lru_cache(maxsize=128)
    def filter_roles(
        roles: Tuple[interactions.Role, ...]
    ) -> Tuple[interactions.Role, ...]:
        if not roles:
            return ()

        sorted_roles = sorted(roles, key=lambda role: role.position, reverse=True)
        bot_role_index = next(
            (
                i
                for i, role in enumerate(sorted_roles)
                if role.name == "═════･[Bot身份组]･═════"
            ),
            len(sorted_roles),
        )

        return tuple(
            role
            for role in sorted_roles[:bot_role_index]
            if not role.name.startswith(("——", "══")) and not role.bot_managed
        )

    @staticmethod
    @lru_cache(maxsize=128)
    def extract_role_members_list(
        roles: Tuple[interactions.Role, ...]
    ) -> List[Servant]:
        return [
            Servant(
                role_name=role.name,
                members=tuple(member.mention for member in role.members),
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
        if self.validate_penitentiary_permissions(ctx):
            return await self.send_error(
                ctx,
                "You don't have permission to use this command.",
                log_to_channel=False,
            )

        try:
            incarceration_duration = self.parse_duration(duration)
        except ValueError as e:
            return await self.send_error(ctx, str(e), log_to_channel=False)

        await self.manage_penitentiary_status(
            ctx, member, "incarcerate", duration=incarceration_duration
        )

    @staticmethod
    def parse_duration(duration: str) -> timedelta:
        duration_regex = re.compile(r"(\d+)([dhm])")
        matches = duration_regex.findall(duration.lower())
        if not matches:
            raise ValueError(
                "Invalid duration format. Use combinations of `d` (days), `h` (hours), and `m` (minutes)."
            )

        total_seconds = sum(
            int(value) * {"d": 86400, "h": 3600, "m": 60}[unit]
            for value, unit in matches
        )

        if total_seconds <= 0:
            raise ValueError("Incarceration time must be greater than zero.")

        return timedelta(seconds=total_seconds)

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
        if self.validate_penitentiary_permissions(ctx):
            return await self.send_error(
                ctx,
                "You don't have permission to use this command.",
                log_to_channel=False,
            )

        await self.manage_penitentiary_status(ctx, member, "release")

    @release_member.autocomplete("member")
    async def autocomplete_incarcerated_member(
        self, ctx: interactions.AutocompleteContext
    ) -> None:
        user_input = ctx.input_text.lower()
        choices = await asyncio.gather(
            *(
                self.fetch_member_choice(ctx.guild, member_id, data)
                for member_id, data in self.incarcerated_members.items()
            )
        )
        valid_choices = [choice for choice in choices if choice is not None]
        await ctx.send(valid_choices[:25])

    async def fetch_member_choice(
        self, guild: interactions.Guild, member_id: str, data: Dict[str, Any]
    ) -> Optional[interactions.SlashCommandChoice]:
        try:
            member = await guild.fetch_member(int(member_id))
            if member and self.ctx.input_text.lower() in member.user.username.lower():
                return interactions.SlashCommandChoice(
                    name=member.user.username, value=member_id
                )
        except NotFound:
            logger.warning(f"Member {member_id} not found in guild.")
        except Exception as e:
            logger.error(f"Failed to fetch member {member_id}: {e}")
        return None

    async def manage_penitentiary_status(
        self,
        ctx: Optional[interactions.SlashContext],
        member: interactions.Member,
        action: Action,
        **kwargs: Any,
    ) -> None:
        guild = ctx.guild if ctx else await self.bot.fetch_guild(self.config.GUILD_ID)
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

        action_map: Final[Dict[Literal["incarcerate", "release"], Callable]] = {
            "incarcerate": partial(self.perform_member_incarceration, **kwargs),
            "release": self.perform_member_release,
        }

        try:
            await action_map[action](member, roles, ctx)
            if ctx:
                action_text = "incarcerated" if action == "incarcerate" else "released"
                await self.send_success(
                    ctx,
                    f"{member.mention} has been {action_text} successfully.",
                )
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
            roles_to_remove: List[interactions.Role] = [
                role
                for role_key in ("electoral", "approved", "temporary")
                if (role := roles.get(role_key)) and role in member.roles
            ]

            original_roles = [
                role.id
                for role in member.roles
                if role.id
                in (
                    self.config.ELECTORAL_ROLE_ID,
                    self.config.APPROVED_ROLE_ID,
                    self.config.TEMPORARY_ROLE_ID,
                )
            ]

            await asyncio.gather(
                (
                    member.remove_roles(roles_to_remove)
                    if roles_to_remove
                    else asyncio.sleep(0)
                ),
                (
                    member.add_roles([incarcerated_role])
                    if incarcerated_role
                    else asyncio.sleep(0)
                ),
            )

            logger.info(
                f"Roles updated for {member}: removed {[r.id for r in roles_to_remove]}, added {incarcerated_role.id if incarcerated_role else 'None'}"
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

        guild = await self.bot.fetch_guild(self.config.GUILD_ID)
        executor = ctx.author if ctx else None
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
        incarcerated_role = roles.get("incarcerated")
        member_data = self.incarcerated_members.get(str(member.id), {})
        original_roles = member_data.get("original_roles", [])

        roles_to_add: List[interactions.Role] = [
            role
            for role in member.guild.roles
            if role.id in original_roles and role not in member.roles
        ]

        await asyncio.gather(
            (
                member.remove_roles([incarcerated_role])
                if incarcerated_role and incarcerated_role in member.roles
                else asyncio.sleep(0)
            ),
            member.add_roles(roles_to_add) if roles_to_add else asyncio.sleep(0),
        )

        logger.info(
            f"Released {member}: removed incarcerated role, restored {[r.id for r in roles_to_add]}"
        )

        self.incarcerated_members.pop(str(member.id), None)
        await self.save_incarcerated_members()

        guild = await self.bot.fetch_guild(self.config.GUILD_ID)
        executor = ctx.author if ctx else None
        log_message = (
            f"{member.mention} has been released and their original roles have been restored "
            f"by {executor.mention if executor else 'the system'}."
        )
        await self.send_success(ctx, log_message)

    async def schedule_release(
        self, member_id: str, data: Dict[str, Any], delay: float
    ) -> None:
        await asyncio.sleep(delay)
        await self.release_prisoner(member_id, data)

    async def release_prisoner(self, member_id: str, data: Dict[str, Any]) -> None:
        guild = await self.bot.fetch_guild(self.config.GUILD_ID)
        try:
            member = await guild.fetch_member(int(member_id))
            if member:
                await self.manage_penitentiary_status(None, member, "release")
                release_time = int(float(data["release_time"]))
                log_message = f"{member.mention} has been released at <t:{int(time.time())}:F>. Scheduled release time was <t:{release_time}:F>."
                await self.send_success(None, log_message)
            else:
                release_time = int(float(data["release_time"]))
                log_message = f"Member with ID {member_id} not found in guild during release. Scheduled release time was <t:{release_time}:F>."
                await self.send_error(None, log_message)
        except Exception as e:
            release_time = int(float(data["release_time"]))
            error_msg = f"Error releasing member {member_id}: {str(e)}. Scheduled release time was <t:{release_time}:F>."
            logger.error(error_msg)
            await self.send_error(None, error_msg)
        finally:
            self.incarcerated_members.pop(member_id, None)
            await self.save_incarcerated_members()

    async def fetch_penitentiary_roles(
        self, guild: interactions.Guild
    ) -> Dict[str, Optional[interactions.Role]]:
        role_ids = self._get_role_ids()

        async def fetch_role(
            key: str, role_id: int
        ) -> Tuple[str, Optional[interactions.Role]]:
            try:
                role = await guild.fetch_role(role_id)
                logger.info(f"Successfully fetched {key} role: {role.id}")
                return key, role
            except Exception as e:
                logger.error(f"Failed to fetch {key} role (ID: {role_id}): {e}")
                return key, None

        results = await asyncio.gather(*(fetch_role(k, v) for k, v in role_ids.items()))
        return {k: v for k, v in results if v is not None}

    # Events

    # @interactions.listen(MessageCreate)
    # async def on_message_create(self, event: MessageCreate) -> None:
    #     if not event.message.author.bot:
    #         author_id: Final[str] = str(event.message.author.id)
    #      async with self._stats_lock:
    #             self.stats[author_id] += 1
    #         await self._debounce_save_stats()

    async def _debounce_save_stats(self) -> None:
        if self._save_stats_task is not None:
            self._save_stats_task.cancel()
        self._save_stats_task = asyncio.create_task(self._delayed_save_stats())

    async def _delayed_save_stats(self) -> None:
        try:
            await asyncio.sleep(5)
            async with self._stats_lock:
                await self.save_stats_roles()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in _delayed_save_stats: {e}", exc_info=True)
        finally:
            self._save_stats_task = None

    @interactions.listen(ExtensionLoad)
    async def on_extension_load(self, event: ExtensionLoad) -> None:
        # self.update_roles_based_on_activity.start()
        self.cleanup_old_locks.start()
        self.check_incarcerated_members.start()

    @interactions.listen(ExtensionUnload)
    async def on_extension_unload(self, event: ExtensionUnload) -> None:
        tasks_to_stop: Final[tuple] = (
            # self.update_roles_based_on_activity,
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
        if not isinstance(event.thread, interactions.GuildPublicThread):
            return

        if not all(
            (
                event.thread.parent_id == self.config.VETTING_FORUM_ID,
                event.thread.id not in self.processed_thread_ids,
                event.thread.owner_id is not None,
            )
        ):
            return

        await self.handle_new_thread(event.thread)
        self.processed_thread_ids.add(event.thread.id)

    # Tasks

    @interactions.Task.create(interactions.IntervalTrigger(days=7))
    async def cleanup_old_locks(self) -> None:
        current_time: datetime = datetime.now(timezone.utc)
        threshold: timedelta = timedelta(days=7)
        keys_to_remove: List[int] = [
            key
            for key, val in self.member_lock_map.items()
            if current_time - val["last_used"] > threshold and not val["lock"].locked()
        ]
        for key in keys_to_remove:
            del self.member_lock_map[key]
        logger.info(f"Cleaned up {len(keys_to_remove)} old locks.")

    @interactions.Task.create(interactions.IntervalTrigger(seconds=30))
    async def check_incarcerated_members(self) -> None:
        now: float = time.time()
        releasable_prisoners: List[Tuple[str, Dict[str, Any]]] = []

        async def process_member(member_id: str, data: Dict[str, Any]) -> None:
            try:
                release_time: float = float(data["release_time"])
                if now >= release_time:
                    releasable_prisoners.append((member_id, data))
                elif (release_time - now) <= 60:
                    delay: float = max(0, release_time - now)
                    asyncio.create_task(self.schedule_release(member_id, data, delay))
            except Exception as e:
                logger.error(f"Error processing member {member_id}: {e}")

        await asyncio.gather(
            *(
                process_member(member_id, data)
                for member_id, data in self.incarcerated_members.items()
            )
        )

        release_tasks: List[Coroutine] = [
            self.release_prisoner(member_id, data)
            for member_id, data in releasable_prisoners
        ]
        release_results: List[Any] = await asyncio.gather(
            *release_tasks, return_exceptions=True
        )

        for result in release_results:
            if isinstance(result, Exception):
                logger.error(f"Error releasing prisoner: {result}")

        if releasable_prisoners:
            logger.info(f"Released {len(releasable_prisoners)} prisoners")
        else:
            logger.info("No prisoners to release at this time")

    @interactions.Task.create(interactions.IntervalTrigger(hours=1))
    async def update_roles_based_on_activity(self) -> None:
        guild: interactions.Guild = await self.bot.fetch_guild(self.config.GUILD_ID)
        roles: Dict[str, interactions.Role] = {
            "approved": await guild.fetch_role(self.config.APPROVED_ROLE_ID),
            "temporary": await guild.fetch_role(self.config.TEMPORARY_ROLE_ID),
        }

        async def process_member(
            member_id: str, message_count: int
        ) -> Tuple[int, str, List[interactions.Role], List[interactions.Role]]:
            if message_count < 50:
                return int(member_id), "", [], []

            try:
                member: interactions.Member = await guild.fetch_member(int(member_id))
                member_role_ids: Set[int] = {role.id for role in member.roles}

                if (
                    roles["temporary"].id in member_role_ids
                    and roles["approved"].id not in member_role_ids
                ):
                    reason: str = (
                        f"Sent {message_count} messages, upgraded from {self.config.TEMPORARY_ROLE_ID} to {self.config.APPROVED_ROLE_ID}."
                    )
                    return (
                        int(member_id),
                        reason,
                        [roles["temporary"]],
                        [roles["approved"]],
                    )
            except Exception as e:
                logger.error(
                    f"Error processing member {member_id}: {str(e)}\n{traceback.format_exc()}"
                )

            return int(member_id), "", [], []

        results: List[
            Tuple[int, str, List[interactions.Role], List[interactions.Role]]
        ] = await asyncio.gather(
            *(
                process_member(member_id, count)
                for member_id, count in self.stats.items()
            )
        )

        role_updates: Dict[str, Dict[int, List[interactions.Role]]] = defaultdict(
            lambda: defaultdict(list)
        )
        members_to_update: Set[int] = set()
        log_messages: List[str] = []

        for member_id, reason, roles_to_remove, roles_to_add in results:
            if reason:
                role_updates["remove"][member_id].extend(roles_to_remove)
                role_updates["add"][member_id].extend(roles_to_add)
                members_to_update.add(member_id)
                log_messages.append(f"Updated roles for {member_id}: {reason}")

        if members_to_update:

            async def update_member_roles(member_id: int) -> None:
                member = await guild.fetch_member(member_id)
                await asyncio.gather(
                    member.remove_roles(role_updates["remove"][member_id]),
                    member.add_roles(role_updates["add"][member_id]),
                )

            await asyncio.gather(
                *(update_member_roles(member_id) for member_id in members_to_update)
            )

            log_message = "\n".join(log_messages)
            await self.send_success(None, log_message)

        self.stats = {
            k: v for k, v in self.stats.items() if int(k) not in members_to_update
        }
        await self.save_stats_roles()

    # Serve

    async def handle_new_thread(self, thread: interactions.GuildPublicThread) -> None:
        try:
            timestamp = datetime.now(timezone.utc).strftime("%y%m%d%H%M")
            new_title = f"[{timestamp}] {thread.name}"

            await asyncio.gather(
                thread.edit(name=new_title),
                self._send_review_components(thread),
                self.notify_vetting_reviewers(
                    self.config.VETTING_ROLE_IDS, thread, timestamp
                ),
            )
        except Exception as e:
            logger.exception(f"Error processing new post: {str(e)}")

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
        if not (match := self.custom_roles_menu_pattern.match(ctx.custom_id)):
            await self.send_error(ctx, "Invalid custom ID format.")
            return

        member_id: int = int(match.group(1))

        if not ctx.values:
            await self.send_error(ctx, "No action selected.", log_to_channel=False)
            return

        action: Literal["add", "remove"] = ctx.values[0]

        try:
            member: interactions.Member = await ctx.guild.fetch_member(member_id)
        except NotFound:
            await self.send_error(ctx, f"Member with ID {member_id} not found.")
            return

        options = [
            interactions.StringSelectOption(label=role, value=role)
            for role in self.custom_roles.keys()
        ]

        if not options:
            await self.send_error(
                ctx, "No custom roles available.", log_to_channel=False
            )
            return

        if len(options) > 25:
            await self.send_error(
                ctx,
                "Too many custom roles. Pagination not implemented yet.",
                log_to_channel=False,
            )
            return

        components = [
            interactions.StringSelectMenu(
                *options,
                custom_id=f"{action}_roles_menu_{member.id}",
                placeholder="Select role",
                max_values=1,
            )
        ]

        await ctx.send(
            f"Select role to {action} for {member.mention}:",
            components=components,
            ephemeral=True,
        )

    role_menu_regex_pattern = re.compile(r"(add|remove)_roles_menu_(\d+)")

    @interactions.component_callback(role_menu_regex_pattern)
    async def on_role_menu_select(self, ctx: interactions.ComponentContext) -> None:
        try:
            logger.info(
                f"on_role_menu_select triggered with custom_id: {ctx.custom_id}"
            )

            match = self.role_menu_regex_pattern.match(ctx.custom_id)
            if not match:
                logger.error(f"Invalid custom ID format: {ctx.custom_id}")
                return await self.send_error(ctx, "Invalid custom ID format.")

            action, member_id = match.groups()
            member_id = int(member_id)
            logger.info(f"Parsed action: {action}, member_id: {member_id}")

            if action not in [Action.ADD.value, Action.REMOVE.value]:
                logger.error(f"Invalid action: {action}")
                return await self.send_error(ctx, f"Invalid action: {action}")

            try:
                member = await ctx.guild.fetch_member(member_id)
            except NotFound:
                logger.error(f"Member with ID {member_id} not found.")
                return await self.send_error(
                    ctx, f"Member with ID {member_id} not found."
                )

            if not ctx.values:
                logger.warning("No role selected.")
                return await self.send_error(
                    ctx, "No role selected.", log_to_channel=False
                )

            selected_role = ctx.values[0]
            logger.info(f"Selected role: {selected_role}")

            updated_roles = await self.update_custom_roles(
                member_id, {selected_role}, Action(action)
            )

            if updated_roles:
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
        updated_roles = set()
        for role in roles:
            if role not in self.custom_roles:
                self.custom_roles[role] = set()

            role_members = self.custom_roles[role]
            if action == Action.ADD:
                if user_id not in role_members:
                    role_members.add(user_id)
                    updated_roles.add(role)
            elif action == Action.REMOVE:
                if user_id in role_members:
                    role_members.remove(user_id)
                    updated_roles.add(role)
                    if not role_members:
                        del self.custom_roles[role]

        if updated_roles:
            await self.save_custom_roles()
        return updated_roles

    async def save_custom_roles(self) -> None:
        try:
            serializable_custom_roles = {
                role: list(members) for role, members in self.custom_roles.items()
            }
            await self.model.save_data("custom.json", serializable_custom_roles)
            logger.info("Custom roles saved successfully")
        except Exception as e:
            logger.error(f"Failed to save custom roles: {e}")
            raise

    async def save_stats_roles(self) -> None:
        try:
            await self.model.save_data("stats.json", dict(self.stats))
            logger.info(f"Stats saved successfully: {self.stats}")
        except Exception as e:
            logger.error(f"Failed to save stats roles: {e}", exc_info=True)
            raise

    async def save_incarcerated_members(self) -> None:
        try:
            await self.model.save_data(
                "incarcerated_members.json", self.incarcerated_members
            )
        except Exception as e:
            logger.error(f"Failed to save incarcerated members: {e}")
            raise
