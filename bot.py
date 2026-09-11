import asyncio
import copy
import json
import logging
import os
import re
import time
import uuid
import xml.etree.ElementTree as ET
from urllib.parse import parse_qsl, urlsplit

import aiohttp
import discord
from aiohttp import web
from discord import app_commands
from discord.ext import commands

TOKEN = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
if TOKEN:
    TOKEN = TOKEN.strip().strip("'").strip('"')

BOOKSHELF_URL = (os.getenv("BOOKSHELF_URL") or "http://bookshelf:8787").strip().strip("'").strip('"').rstrip("/")
API_KEY = os.getenv("BOOKSHELF_API_KEY")
if API_KEY:
    API_KEY = API_KEY.strip().strip("'").strip('"')

JACKETT_URL = (os.getenv("JACKETT_URL") or "").strip().strip("'").strip('"').rstrip("/")
JACKETT_API_KEY = (os.getenv("JACKETT_API_KEY") or "").strip().strip("'").strip('"')
ABS_REPORT_CHANNEL_ID_VALUE = (os.getenv("ABS_REPORT_CHANNEL_ID") or "").strip()

try:
    HEALTH_PORT = int(os.getenv("PORT", "8080"))
    DOWNLOAD_POLL_SECONDS = max(10, int(os.getenv("DOWNLOAD_POLL_SECONDS", "30")))
    DOWNLOAD_WATCH_SECONDS = max(300, int(os.getenv("DOWNLOAD_WATCH_SECONDS", "86400")))
    ABS_REPORT_CHANNEL_ID = int(ABS_REPORT_CHANNEL_ID_VALUE) if ABS_REPORT_CHANNEL_ID_VALUE else None
except ValueError as exc:
    raise RuntimeError(
        "PORT, DOWNLOAD_POLL_SECONDS, DOWNLOAD_WATCH_SECONDS, and ABS_REPORT_CHANNEL_ID "
        "must be valid integers"
    ) from exc

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger("booky")

AUDIO_KEYWORDS = (
    "audio", "audible", "spoken", "cd", "cassette", "mp3", "unabridged", "abridged",
    "tantor", "blackstone", "brilliance", "recorded books", "listening library",
    "podium", "sound library", "chivers", "harperaudio", "random house audio",
    "simon & schuster audio",
)
AUDIO_PATTERN = re.compile(
    r"\b(?:" + "|".join(re.escape(keyword) for keyword in AUDIO_KEYWORDS) + r")\b",
    re.IGNORECASE,
)
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


class BookshelfError(Exception):
    pass


class BookshelfClient:
    """Reusable, non-blocking Bookshelf API client with pooling, retries, and timeouts."""

    def __init__(self):
        self.session = None

    async def start(self):
        if self.session and not self.session.closed:
            return
        self.session = aiohttp.ClientSession(
            headers={"X-Api-Key": API_KEY or ""},
            timeout=aiohttp.ClientTimeout(total=25, connect=5, sock_read=20),
            connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300),
        )

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def request(self, method, path, *, params=None, payload=None, attempts=3):
        await self.start()
        url = f"{BOOKSHELF_URL}{path}"

        for attempt in range(attempts):
            started = time.monotonic()
            try:
                async with self.session.request(method, url, params=params, json=payload) as response:
                    text = await response.text()
                    try:
                        data = json.loads(text) if text else None
                    except json.JSONDecodeError:
                        data = None

                    logger.debug(
                        "%s %s completed in %.2fs with status %s",
                        method,
                        path,
                        time.monotonic() - started,
                        response.status,
                    )
                    if response.status in RETRYABLE_STATUSES and attempt + 1 < attempts:
                        retry_after = response.headers.get("Retry-After", "")
                        delay = float(retry_after) if retry_after.isdigit() else 2**attempt
                        logger.warning(
                            "%s %s returned %s; retrying in %.1fs",
                            method,
                            path,
                            response.status,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    return response.status, data, text
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt + 1 == attempts:
                    raise BookshelfError(f"Bookshelf request failed: {exc}") from exc
                delay = 2**attempt
                logger.warning("%s %s failed (%s); retrying in %ss", method, path, exc, delay)
                await asyncio.sleep(delay)

        raise BookshelfError("Bookshelf request failed after all retries")

    async def get(self, path, *, params=None):
        return await self.request("GET", path, params=params)

    async def post(self, path, *, payload=None):
        return await self.request("POST", path, payload=payload)

    async def put(self, path, *, payload=None):
        return await self.request("PUT", path, payload=payload)


class JackettClient:
    """Optional Torznab client used only when Bookshelf/Prowlarr finds no release."""

    def __init__(self):
        self.session = None

    @property
    def configured(self):
        return bool(JACKETT_URL and JACKETT_API_KEY)

    async def start(self):
        if not self.configured or (self.session and not self.session.closed):
            return
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=45, connect=5, sock_read=40),
            connector=aiohttp.TCPConnector(limit=5, ttl_dns_cache=300),
        )

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def search(self, query, limit=5):
        if not self.configured:
            logger.warning("Jackett fallback is not configured")
            return []

        await self.start()
        endpoint = f"{JACKETT_URL}/api/v2.0/indexers/all/results/torznab/api"
        try:
            async with self.session.get(
                endpoint,
                params={"apikey": JACKETT_API_KEY, "t": "search", "q": query},
            ) as response:
                body = await response.text()
                if response.status != 200:
                    logger.error("Jackett search returned HTTP %s: %s", response.status, body[:300])
                    return []
        except (aiohttp.ClientError, asyncio.TimeoutError):
            logger.exception("Jackett fallback search failed")
            return []

        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            logger.exception("Jackett returned invalid Torznab XML")
            return []

        results = []
        for item in (node for node in root.iter() if node.tag.rsplit("}", 1)[-1] == "item"):
            title = self._child_text(item, "title")
            attributes = {
                node.get("name", "").casefold(): node.get("value", "")
                for node in item.iter()
                if node.tag.rsplit("}", 1)[-1] == "attr"
            }
            link = self._safe_link(
                attributes.get("magneturl"),
                self._child_text(item, "comments"),
                self._child_text(item, "guid"),
                self._child_text(item, "link"),
            )
            if title and link:
                results.append(
                    {
                        "title": title,
                        "link": link,
                        "seeders": attributes.get("seeders"),
                        "indexer": attributes.get("indexer"),
                    }
                )
            if len(results) >= limit:
                break
        return results

    @staticmethod
    def _child_text(item, name):
        for child in item:
            if child.tag.rsplit("}", 1)[-1] == name and child.text:
                return child.text.strip()
        return ""

    @staticmethod
    def _safe_link(*candidates):
        sensitive_terms = ("apikey", "api_key", "passkey", "authkey", "token=")
        for candidate in candidates:
            link = str(candidate or "").strip()
            if not link or JACKETT_API_KEY in link or any(char in link for char in "<>\r\n"):
                continue
            lowered = link.casefold()
            if any(term in lowered for term in sensitive_terms):
                continue
            parsed = urlsplit(link)
            if parsed.scheme == "magnet":
                return link
            if parsed.scheme in ("http", "https") and parsed.netloc:
                query_keys = {key.casefold() for key, _ in parse_qsl(parsed.query)}
                if not query_keys.intersection({"apikey", "api_key", "passkey", "authkey", "token"}):
                    return link
        return ""


api = BookshelfClient()
jackett = JackettClient()
_profile_cache = {"value": None, "expires": 0.0}
_library_cache = {"value": None, "expires": 0.0}
_profile_lock = asyncio.Lock()
_library_lock = asyncio.Lock()
_download_watch_tasks = set()


def normalize(value):
    return " ".join(re.sub(r"[^\w]+", " ", str(value or "").casefold()).split())


def no_results_message(title):
    safe_title = discord.utils.escape_markdown(
        discord.utils.escape_mentions(str(title or "Unknown Title")[:200])
    )
    return f"No Results Found for **{safe_title}**."


def get_collection_name(book):
    series_title = str(book.get("seriesTitle") or "").split(";", 1)[0].strip()
    if series_title:
        return re.sub(r"\s+#\s*\d+(?:\.\d+)?\s*$", "", series_title).strip()

    for series in book.get("series") or []:
        name = str(series.get("title") or series.get("name") or "").strip()
        if name:
            return name
    for link in book.get("seriesLinks") or []:
        series = link.get("series") or {}
        name = str(series.get("title") or series.get("name") or "").strip()
        if name:
            return name
    return ""


def collection_position(book):
    series_title = str(book.get("seriesTitle") or "")
    match = re.search(r"#\s*(\d+(?:\.\d+)?)", series_title)
    return float(match.group(1)) if match else float("inf")


def sanitize(obj):
    """Replace null collection fields with empty lists for Readarr schema validation."""
    if not isinstance(obj, dict):
        return
    for key in ("images", "genres", "links", "tags", "editions"):
        if obj.get(key) is None:
            obj[key] = []
    if obj.get("ratings") is None:
        obj["ratings"] = {"votes": 0, "value": 0}


def get_author_name(book):
    author = book.get("author") or {}
    return book.get("authorTitle") or author.get("authorName") or author.get("name") or "Unknown Author"


def get_poster_url(book):
    """Return the best public cover URL supplied by the metadata lookup."""
    images = list(book.get("images") or [])
    for edition in book.get("editions") or []:
        images.extend(edition.get("images") or [])

    images.sort(
        key=lambda image: str(image.get("coverType") or "").casefold() not in ("cover", "poster")
    )
    for image in images:
        for key in ("remoteUrl", "url"):
            url = str(image.get(key) or "").strip()
            if url.startswith(("https://", "http://")):
                return url
    return None


def build_selection_embed(book):
    title = book.get("title", "Unknown Book")
    author_name = get_author_name(book)
    year = str(book.get("publishDate") or "")[:4]
    embed = discord.Embed(
        title=title,
        description=f"by **{author_name}**",
        color=discord.Color.from_rgb(99, 102, 241),
    )
    if year:
        embed.add_field(name="Published", value=year, inline=True)
    embed.add_field(
        name="Format",
        value="🎧 Audiobook" if is_audio_edition(book) else "📖 Book",
        inline=True,
    )
    poster_url = get_poster_url(book)
    if poster_url:
        embed.set_image(url=poster_url)
    else:
        embed.add_field(name="Cover", value="No cover image was provided.", inline=False)
    embed.set_footer(text="Confirm below to add this title and search Prowlarr.")
    return embed


def audio_edition_score(book):
    """Prefer structured audio metadata over incidental description matches."""
    best_score = 0
    for edition in book.get("editions") or []:
        structured = " ".join(str(edition.get(field) or "") for field in ("format", "binding"))
        identity = " ".join(str(edition.get(field) or "") for field in ("title", "publisher"))
        description = str(edition.get("overview") or edition.get("description") or "")
        score = 0
        if AUDIO_PATTERN.search(structured):
            score += 8
        if AUDIO_PATTERN.search(identity):
            score += 4
        if AUDIO_PATTERN.search(description):
            score += 1
        best_score = max(best_score, score)
    return best_score


def is_audio_edition(book):
    return audio_edition_score(book) > 0


def rank_and_limit_results(results, title_query="", author_query="", limit=25):
    """Deduplicate results and rank title and author relevance before Discord's limit."""
    normalized_title_query = normalize(title_query)
    normalized_author_query = normalize(author_query)
    ranked = []
    seen = set()

    for position, book in enumerate(results):
        title = book.get("title")
        if not title:
            continue

        author_name = get_author_name(book)
        foreign_edition_id = str(book.get("foreignEditionId") or "")
        foreign_book_id = str(book.get("foreignBookId") or "")
        key = (foreign_edition_id, foreign_book_id)
        if not foreign_edition_id and not foreign_book_id:
            key = (normalize(title), normalize(author_name), str(book.get("publishDate") or "")[:4])
        if key in seen:
            continue
        seen.add(key)

        normalized_title = normalize(title)
        normalized_author = normalize(author_name)
        audio_score = audio_edition_score(book)
        score = audio_score * 10

        title_matches = False
        if normalized_title_query:
            if normalized_title == normalized_title_query:
                score += 100
                title_matches = True
            elif normalized_title.startswith(normalized_title_query):
                score += 60
                title_matches = True
            elif normalized_title_query in normalized_title:
                score += 35
                title_matches = True

        author_matches = False
        if normalized_author_query:
            if normalized_author == normalized_author_query:
                score += 80
                author_matches = True
            elif normalized_author.startswith(normalized_author_query):
                score += 45
                author_matches = True
            elif normalized_author_query in normalized_author:
                score += 25
                author_matches = True

        if normalized_title_query and normalized_author_query and title_matches and author_matches:
            score += 50
        ranked.append((score, -position, audio_score, book))

    audio_results = [item for item in ranked if item[2] > 0]
    candidates = audio_results or ranked
    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [item[3] for item in candidates[:limit]]


async def require_list(path):
    status, data, text = await api.get(path)
    if status != 200 or not isinstance(data, list):
        raise BookshelfError(f"Bookshelf returned {status} for {path}: {text[:160]}")
    return data


async def get_profile_settings():
    """Cache rarely changing root/profile settings for ten minutes."""
    now = time.monotonic()
    if _profile_cache["value"] and _profile_cache["expires"] > now:
        return _profile_cache["value"]

    async with _profile_lock:
        now = time.monotonic()
        if _profile_cache["value"] and _profile_cache["expires"] > now:
            return _profile_cache["value"]

        root_folders, quality_profiles, metadata_profiles = await asyncio.gather(
            require_list("/api/v1/rootfolder"),
            require_list("/api/v1/qualityprofile"),
            require_list("/api/v1/metadataprofile"),
        )
        if not root_folders or not quality_profiles or not metadata_profiles:
            raise BookshelfError("Bookshelf profile configuration is incomplete")

        root_folder = next(
            (
                item["path"]
                for item in root_folders
                if "audio" in item.get("path", "").casefold()
                or "spoken" in item.get("path", "").casefold()
            ),
            root_folders[0]["path"],
        )
        quality_profile = next(
            (
                item["id"]
                for item in quality_profiles
                if "audio" in item.get("name", "").casefold()
                or "spoken" in item.get("name", "").casefold()
            ),
            quality_profiles[0]["id"],
        )
        value = (root_folder, quality_profile, metadata_profiles[0]["id"])
        _profile_cache.update(value=value, expires=now + 600)
        return value


def build_library_indexes(books, authors):
    indexes = {
        "book_by_foreign_book": {},
        "book_by_foreign_edition": {},
        "book_by_title_author": {},
        "author_by_foreign_id": {},
        "author_by_name": {},
    }
    for book in books:
        foreign_book_id = str(book.get("foreignBookId") or "")
        foreign_edition_id = str(book.get("foreignEditionId") or "")
        if foreign_book_id:
            indexes["book_by_foreign_book"][foreign_book_id] = book
        if foreign_edition_id:
            indexes["book_by_foreign_edition"][foreign_edition_id] = book
        indexes["book_by_title_author"][(normalize(book.get("title")), normalize(get_author_name(book)))] = book

    for author in authors:
        foreign_author_id = str(author.get("foreignAuthorId") or "")
        if foreign_author_id:
            indexes["author_by_foreign_id"][foreign_author_id] = author
        indexes["author_by_name"][normalize(author.get("authorName"))] = author
    return indexes


async def get_library_indexes():
    """Cache indexed library data to avoid full scans on every selection."""
    now = time.monotonic()
    if _library_cache["value"] and _library_cache["expires"] > now:
        return _library_cache["value"]

    async with _library_lock:
        now = time.monotonic()
        if _library_cache["value"] and _library_cache["expires"] > now:
            return _library_cache["value"]

        books, authors = await asyncio.gather(
            require_list("/api/v1/book"),
            require_list("/api/v1/author"),
        )
        value = build_library_indexes(books, authors)
        _library_cache.update(value=value, expires=now + 30)
        return value


def invalidate_library_cache():
    _library_cache.update(value=None, expires=0.0)


def response_records(data):
    if isinstance(data, dict):
        return data.get("records") or []
    return data if isinstance(data, list) else []


async def get_recent_grab_ids(book_id):
    status, data, _ = await api.get(
        "/api/v1/history",
        params={
            "bookId": book_id,
            "pageSize": 25,
            "sortKey": "date",
            "sortDirection": "descending",
        },
    )
    if status != 200:
        return None
    return {
        str(record.get("id"))
        for record in response_records(data)
        if record.get("id") is not None
        and record.get("eventType") == "grabbed"
        and str(record.get("bookId")) == str(book_id)
    }


def format_jackett_results(results):
    content = "🔎 Prowlarr found no releases. Jackett found these possible matches:\n"
    added = 0
    for result in results:
        title = discord.utils.escape_markdown(result["title"][:120])
        details = []
        if result.get("indexer"):
            details.append(discord.utils.escape_markdown(str(result["indexer"])[:40]))
        if result.get("seeders"):
            details.append(f"{result['seeders']} seeders")
        suffix = f" — {' · '.join(details)}" if details else ""
        entry = f"\n• **{title}**{suffix}\n  <{result['link']}>"
        if len(content) + len(entry) > 1900:
            break
        content += entry
        added += 1
    return content if added else None


def book_has_file(book):
    statistics = book.get("statistics") or {}
    return (
        book.get("hasFile") is True
        or bool(book.get("bookFileId"))
        or statistics.get("bookFileCount", 0) > 0
        or statistics.get("sizeOnDisk", 0) > 0
    )


async def monitor_download(book_id, channel_id, requester_id, title, author_name):
    deadline = time.monotonic() + DOWNLOAD_WATCH_SECONDS
    while time.monotonic() < deadline:
        try:
            status, book, _ = await api.get(f"/api/v1/book/{book_id}")
            if status == 200 and isinstance(book, dict) and book_has_file(book):
                channel = bot.get_channel(channel_id)
                if channel is None:
                    channel = await bot.fetch_channel(channel_id)

                safe_title = discord.utils.escape_markdown(
                    discord.utils.escape_mentions(str(title))
                )
                safe_author = discord.utils.escape_markdown(
                    discord.utils.escape_mentions(str(author_name))
                )
                await channel.send(
                    f"✅ <@{requester_id}> **{safe_title}** by *{safe_author}* "
                    "has finished downloading and is now available for playback.",
                    allowed_mentions=discord.AllowedMentions(
                        users=[discord.Object(id=requester_id)],
                        roles=False,
                        everyone=False,
                        replied_user=False,
                    ),
                )
                logger.info("Download completed for book %s; Discord user %s notified", book_id, requester_id)
                return
            if status == 404:
                logger.warning("Stopped monitoring removed Bookshelf book %s", book_id)
                return
        except BookshelfError:
            logger.exception("Bookshelf check failed while monitoring book %s", book_id)
        except discord.DiscordException:
            logger.exception("Could not notify Discord channel %s for book %s", channel_id, book_id)
            return

        await asyncio.sleep(DOWNLOAD_POLL_SECONDS)

    logger.warning("Download monitoring timed out for book %s", book_id)


def schedule_download_monitor(interaction, book_id, title, author_name):
    if not interaction.channel_id or not book_id:
        logger.warning("Cannot monitor book %s because the Discord channel is unavailable", book_id)
        return

    task = asyncio.create_task(
        monitor_download(
            book_id,
            interaction.channel_id,
            interaction.user.id,
            title,
            author_name,
        )
    )
    _download_watch_tasks.add(task)
    task.add_done_callback(_download_watch_tasks.discard)


async def track_search_and_notify(interaction, book_id, title, author_name):
    """Trigger BookSearch and use targeted, bounded polling to detect a new grab."""
    if getattr(interaction, "collection_mode", False):
        status, _, response_text = await api.post(
            "/api/v1/command",
            payload={"name": "BookSearch", "bookIds": [book_id]},
        )
        if status in (200, 201):
            schedule_download_monitor(interaction, book_id, title, author_name)
            await interaction.edit_original_response(
                content=f"🔎 Started an individual Prowlarr search for **{title}** by *{author_name}*."
            )
        else:
            await interaction.edit_original_response(
                content=f"⚠️ Could not start the search for **{title}**: `{response_text[:160]}`"
            )
        return

    previous_grab_ids = await get_recent_grab_ids(book_id)
    status, command, _ = await api.post(
        "/api/v1/command",
        payload={"name": "BookSearch", "bookIds": [book_id]},
    )

    grabbed = False
    if status in (200, 201) and isinstance(command, dict) and command.get("id") is not None:
        command_id = command["id"]
        for delay in (1, 2, 3, 5, 8, 11):
            await asyncio.sleep(delay)
            command_status, command_data, _ = await api.get(f"/api/v1/command/{command_id}")
            if command_status == 200 and isinstance(command_data, dict):
                if command_data.get("status") in ("completed", "failed"):
                    break

        queue_status, queue_data, _ = await api.get(
            "/api/v1/queue",
            params={"bookId": book_id, "pageSize": 50},
        )
        if queue_status == 200:
            grabbed = any(
                str(item.get("bookId")) == str(book_id)
                for item in response_records(queue_data)
            )

        if not grabbed and previous_grab_ids is not None:
            current_grab_ids = await get_recent_grab_ids(book_id)
            if current_grab_ids is not None:
                grabbed = bool(current_grab_ids - previous_grab_ids)

    if grabbed:
        content = (
            f"✅ Added **{title}** by *{author_name}* to Bookshelf!\n"
            "🟢 **Download Found:** Prowlarr matched a release and sent it to your download client.\n"
            "🔔 I’ll notify you here when it is available for playback."
        )
        schedule_download_monitor(interaction, book_id, title, author_name)
    else:
        jackett_results = await jackett.search(f"{title} {author_name} audiobook")
        content = format_jackett_results(jackett_results) or no_results_message(title)
    await interaction.edit_original_response(content=content)


class BookSelect(discord.ui.Select):
    def __init__(self, results, collection_requested=False):
        self.results = results[:25]
        self.collection_requested = collection_requested
        options = []
        for index, book in enumerate(self.results):
            title = book.get("title", "Unknown Title")[:80]
            author_name = get_author_name(book)
            year = str(book.get("publishDate") or "")[:4]
            tag = "📚 [Collection]" if collection_requested else (
                "🎧 [Audio]" if is_audio_edition(book) else "📖 [Book]"
            )
            description = f"{tag} By {author_name} ({year})" if year else f"{tag} By {author_name}"
            options.append(
                discord.SelectOption(
                    label=title,
                    description=description[:100],
                    value=str(index),
                )
            )

        super().__init__(
            placeholder=(
                "Choose a book from the collection..."
                if collection_requested
                else "Choose the exact audiobook edition..."
            ),
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        selected_index = int(self.values[0])
        book = self.results[selected_index]
        if self.collection_requested:
            await interaction.response.defer()
            await start_collection_request(interaction, book)
            return

        await interaction.response.edit_message(
            content="Please review your selection before starting the search:",
            embed=build_selection_embed(book),
            view=ConfirmSelectionView(self, selected_index, interaction.user.id),
        )

    async def handle_selection(self, interaction, selected_index):
        book = copy.deepcopy(self.results[selected_index])
        title = book.get("title", "Unknown Book")
        author_obj = book.get("author") or {}
        author_name = get_author_name(book)
        foreign_book_id = str(book.get("foreignBookId") or "")
        foreign_edition_id = str(book.get("foreignEditionId") or "")

        indexes = await get_library_indexes()
        existing_book = None
        if foreign_book_id:
            existing_book = indexes["book_by_foreign_book"].get(foreign_book_id)
        if not existing_book and foreign_edition_id:
            existing_book = indexes["book_by_foreign_edition"].get(foreign_edition_id)
        if not existing_book:
            existing_book = indexes["book_by_title_author"].get(
                (normalize(title), normalize(author_name))
            )

        if existing_book:
            book_id = existing_book.get("id")
            status, fresh_book, _ = await api.get(f"/api/v1/book/{book_id}")
            if status == 200 and isinstance(fresh_book, dict):
                existing_book = fresh_book

            stats = existing_book.get("statistics") or {}
            has_file = (
                existing_book.get("hasFile") is True
                or bool(existing_book.get("bookFileId"))
                or stats.get("bookFileCount", 0) > 0
                or stats.get("sizeOnDisk", 0) > 0
            )
            if has_file:
                await interaction.edit_original_response(
                    content=f"📁 **{title}** by *{author_name}* is already downloaded and present in your Bookshelf library! No action taken.",
                    view=None,
                )
                return

            existing_book["monitored"] = True
            update_status, _, update_text = await api.put(
                f"/api/v1/book/{book_id}",
                payload=existing_book,
            )
            if update_status not in (200, 202):
                raise BookshelfError(f"Unable to monitor the existing book: {update_text[:160]}")

            await interaction.edit_original_response(
                content=f"🔎 **{title}** by *{author_name}* exists in Bookshelf but has **no file downloaded**. Searching Prowlarr/indexers...",
                view=None,
            )
            await track_search_and_notify(interaction, book_id, title, author_name)
            return

        target_foreign_author_id = str(author_obj.get("foreignAuthorId") or "")
        existing_author = None
        if target_foreign_author_id:
            existing_author = indexes["author_by_foreign_id"].get(target_foreign_author_id)
        if not existing_author:
            existing_author = indexes["author_by_name"].get(normalize(author_name))

        root_folder, quality_profile, metadata_profile = await get_profile_settings()
        author_id = None
        if existing_author:
            author_payload = copy.deepcopy(existing_author)
            author_id = existing_author.get("id")
        else:
            author_payload = copy.deepcopy(author_obj)
            if not author_payload.get("foreignAuthorId"):
                author_payload["foreignAuthorId"] = f"auth-{uuid.uuid4().hex[:8]}"
            author_payload.update(
                {
                    "authorName": author_name,
                    "qualityProfileId": quality_profile,
                    "metadataProfileId": metadata_profile,
                    "rootFolderPath": root_folder,
                    "monitored": True,
                    "addOptions": {"monitor": "none", "searchForMissingBooks": False},
                }
            )
            author_payload.pop("id", None)

        sanitize(book)
        sanitize(author_payload)
        foreign_book_id = str(book.get("foreignBookId") or "").strip()
        if not foreign_book_id:
            foreign_book_id = f"bk-{uuid.uuid4().hex[:8]}"
            book["foreignBookId"] = foreign_book_id

        target_edition_id = str(book.get("foreignEditionId") or "").strip()
        editions = book.get("editions") or []
        if editions:
            match_index = next(
                (
                    index
                    for index, edition in enumerate(editions)
                    if target_edition_id
                    and str(edition.get("foreignEditionId") or "").strip() == target_edition_id
                ),
                0,
            )
            if not target_edition_id:
                target_edition_id = str(
                    editions[match_index].get("foreignEditionId") or f"{foreign_book_id}-ed-0"
                )
            for index, edition in enumerate(editions):
                sanitize(edition)
                edition["isDefault"] = index == match_index
                edition["foreignBookId"] = foreign_book_id
                edition["monitored"] = True
            editions[match_index]["foreignEditionId"] = target_edition_id
        else:
            target_edition_id = target_edition_id or f"{foreign_book_id}-ed-0"
            editions = [
                {
                    "title": title,
                    "foreignEditionId": target_edition_id,
                    "foreignBookId": foreign_book_id,
                    "isDefault": True,
                    "monitored": True,
                    "images": [],
                    "links": [],
                    "genres": [],
                }
            ]

        book.update(
            {
                "foreignEditionId": target_edition_id,
                "editions": editions,
                "author": author_payload,
                "qualityProfileId": quality_profile,
                "monitored": True,
                "addOptions": {"searchForNewBook": False},
            }
        )
        if author_id:
            book["authorId"] = author_id
        book.pop("id", None)

        add_status, created_book, add_text = await api.post("/api/v1/book", payload=book)
        if add_status in (200, 201) and isinstance(created_book, dict):
            invalidate_library_cache()
            await interaction.edit_original_response(
                content=f"⏳ Added **{title}** by *{author_name}*! Searching indexers via Prowlarr...",
                view=None,
            )
            await track_search_and_notify(
                interaction,
                created_book.get("id"),
                title,
                author_name,
            )
        elif "already" in add_text.casefold() or "exists" in add_text.casefold():
            invalidate_library_cache()
            await interaction.edit_original_response(
                content=f"ℹ️ **{title}** by *{author_name}* is already present in Bookshelf.",
                view=None,
            )
        else:
            await interaction.edit_original_response(
                content=f"⚠️ Error: `{add_text[:250]}`",
                view=None,
            )


class CollectionInteractionProxy:
    collection_mode = True

    def __init__(self, channel, user):
        self.channel = channel
        self.channel_id = channel.id
        self.user = user

    async def edit_original_response(self, *, content=None, **_kwargs):
        if content:
            await self.channel.send(
                content,
                allowed_mentions=discord.AllowedMentions.none(),
            )


async def find_collection_books(selected_book, collection_name):
    status, results, response_text = await api.get(
        "/api/v1/book/lookup",
        params={"term": collection_name},
    )
    if status != 200 or not isinstance(results, list):
        raise BookshelfError(
            f"Collection lookup failed with HTTP {status}: {response_text[:160]}"
        )

    normalized_collection = normalize(collection_name)
    matching_books = [
        book
        for book in [selected_book, *results]
        if normalize(get_collection_name(book)) == normalized_collection
    ]
    unique_books = []
    seen = set()
    for book in matching_books:
        key = str(book.get("foreignBookId") or "") or (
            normalize(book.get("title")),
            normalize(get_author_name(book)),
        )
        if key in seen:
            continue
        seen.add(key)
        unique_books.append(book)

    unique_books.sort(
        key=lambda book: (
            collection_position(book),
            str(book.get("publishDate") or ""),
            normalize(book.get("title")),
        )
    )
    return unique_books


async def process_collection(channel, user, collection_name, books):
    proxy = CollectionInteractionProxy(channel, user)
    processed = 0
    for book in books:
        try:
            await BookSelect([book]).handle_selection(proxy, 0)
            processed += 1
        except BookshelfError:
            logger.exception("Bookshelf operation failed for collection book %r", book.get("title"))
            await proxy.edit_original_response(
                content=f"❌ Could not process **{book.get('title', 'Unknown Book')}**. Check the bot logs."
            )
        except (KeyError, TypeError, ValueError):
            logger.exception("Unexpected response while processing collection book %r", book.get("title"))
            await proxy.edit_original_response(
                content=f"❌ Bookshelf returned an unexpected response for **{book.get('title', 'Unknown Book')}**."
            )

    safe_collection = discord.utils.escape_markdown(
        discord.utils.escape_mentions(collection_name[:200])
    )
    await channel.send(
        f"✅ Collection **{safe_collection}** processed: {processed} individual book request(s) submitted.",
        allowed_mentions=discord.AllowedMentions.none(),
    )


async def start_collection_request(interaction, selected_book):
    collection_name = get_collection_name(selected_book)
    if not collection_name:
        await interaction.edit_original_response(
            content="❌ The selected book does not include collection or series metadata.",
            embed=None,
            view=None,
        )
        return

    try:
        books = await find_collection_books(selected_book, collection_name)
    except BookshelfError as exc:
        logger.exception("Collection lookup failed for %r", collection_name)
        await interaction.edit_original_response(content=f"❌ {exc}", embed=None, view=None)
        return

    if not books:
        await interaction.edit_original_response(
            content=no_results_message(collection_name),
            embed=None,
            view=None,
        )
        return

    safe_collection = discord.utils.escape_markdown(
        discord.utils.escape_mentions(collection_name[:200])
    )
    await interaction.edit_original_response(
        content=(
            f"📚 Starting **{len(books)}** individual book request(s) from "
            f"**{safe_collection}**. Progress will be posted in this channel."
        ),
        embed=None,
        view=None,
    )
    task = asyncio.create_task(
        process_collection(interaction.channel, interaction.user, collection_name, books)
    )
    _download_watch_tasks.add(task)
    task.add_done_callback(_download_watch_tasks.discard)


class ConfirmSelectionView(discord.ui.View):
    def __init__(self, book_select, selected_index, user_id):
        super().__init__(timeout=120)
        self.book_select = book_select
        self.selected_index = selected_index
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id == self.user_id:
            return True
        await interaction.response.send_message(
            "Only the person who selected this audiobook can use these controls.",
            ephemeral=True,
        )
        return False

    @discord.ui.button(
        label="Confirm selection",
        style=discord.ButtonStyle.success,
        emoji="✅",
    )
    async def confirm(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await interaction.response.defer()
        self.stop()
        await interaction.edit_original_response(
            content="⏳ Selection confirmed. Checking Bookshelf...",
            embed=None,
            view=None,
        )
        try:
            await self.book_select.handle_selection(interaction, self.selected_index)
        except BookshelfError as exc:
            logger.exception("Bookshelf operation failed")
            await interaction.edit_original_response(content=f"❌ {exc}", embed=None, view=None)
        except (KeyError, TypeError, ValueError):
            logger.exception("Unexpected Bookshelf response while adding a book")
            await interaction.edit_original_response(
                content="❌ Bookshelf returned an unexpected response. Check the bot logs for details.",
                embed=None,
                view=None,
            )

    @discord.ui.button(
        label="Other options",
        style=discord.ButtonStyle.secondary,
        emoji="↩️",
    )
    async def other_options(self, interaction: discord.Interaction, _button: discord.ui.Button):
        self.stop()
        await interaction.response.edit_message(
            content="Choose another audiobook from the original search results:",
            embed=None,
            view=BookSelectView(self.book_select.results),
        )


class BookSelectView(discord.ui.View):
    def __init__(self, results, collection_requested=False):
        super().__init__(timeout=120)
        self.add_item(BookSelect(results, collection_requested=collection_requested))


class ABSReportModal(discord.ui.Modal, title="Audiobook Issue Report"):
    audiobook_title = discord.ui.TextInput(
        label="Audiobook Title",
        placeholder="Enter the audiobook title",
        max_length=200,
    )
    audiobook_author = discord.ui.TextInput(
        label="Audiobook Author",
        placeholder="Enter the author's name",
        max_length=200,
    )
    issue = discord.ui.TextInput(
        label="Issue",
        placeholder="Describe the playback, metadata, or file issue",
        style=discord.TextStyle.paragraph,
        max_length=1000,
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        audiobook_title = str(self.audiobook_title).strip()
        audiobook_author = str(self.audiobook_author).strip()
        issue = str(self.issue).strip()
        if not audiobook_title or not audiobook_author or not issue:
            await interaction.followup.send(
                "❌ Audiobook Title, Audiobook Author, and Issue are required.",
                ephemeral=True,
            )
            return

        try:
            channel = bot.get_channel(ABS_REPORT_CHANNEL_ID)
            if channel is None:
                channel = await bot.fetch_channel(ABS_REPORT_CHANNEL_ID)
            if not hasattr(channel, "send"):
                await interaction.followup.send(
                    "❌ The configured report destination is not a message channel.",
                    ephemeral=True,
                )
                return

            report = discord.Embed(
                title="🎧 Audiobook Issue Report",
                color=discord.Color.from_rgb(99, 102, 241),
                timestamp=discord.utils.utcnow(),
            )
            report.add_field(name="Audiobook Title", value=audiobook_title, inline=False)
            report.add_field(name="Audiobook Author", value=audiobook_author, inline=False)
            report.add_field(name="Issue", value=issue, inline=False)
            report.set_footer(
                text=f"Submitted by {interaction.user} • User ID {interaction.user.id}",
                icon_url=interaction.user.display_avatar.url,
            )
            await channel.send(
                embed=report,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            await interaction.followup.send(
                "✅ Your audiobook issue report was submitted.",
                ephemeral=True,
            )
        except discord.DiscordException:
            logger.exception("Failed to deliver audiobook issue report to channel %s", ABS_REPORT_CHANNEL_ID)
            await interaction.followup.send(
                "❌ I could not deliver the report. Check the report channel and bot permissions.",
                ephemeral=True,
            )


async def health_check(_request):
    return web.json_response({"status": "ok"})


async def readiness_check(request):
    active_bot = request.app["bot"]
    if active_bot.is_ready():
        return web.json_response({"status": "ready"})
    return web.json_response({"status": "starting"}, status=503)


async def start_health_server(active_bot):
    app = web.Application()
    app["bot"] = active_bot
    app.add_routes(
        [
            web.get("/healthz", health_check),
            web.get("/readyz", readiness_check),
        ]
    )
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", HEALTH_PORT).start()
    logger.info("Health server listening on port %s", HEALTH_PORT)
    return runner


class BookshelfBot(commands.Bot):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.health_runner = None

    async def setup_hook(self):
        self.health_runner = await start_health_server(self)
        await api.start()
        await self.tree.sync()

    async def close(self):
        for task in tuple(_download_watch_tasks):
            task.cancel()
        if _download_watch_tasks:
            await asyncio.gather(*_download_watch_tasks, return_exceptions=True)
        await api.close()
        await jackett.close()
        if self.health_runner:
            await self.health_runner.cleanup()
        await super().close()


intents = discord.Intents.default()
bot = BookshelfBot(
    command_prefix="!",
    intents=intents,
    status=discord.Status.online,
    activity=discord.Activity(
        type=discord.ActivityType.listening,
        name="/request audiobook searches",
    ),
)


@bot.event
async def on_ready():
    await bot.change_presence(
        status=discord.Status.online,
        activity=discord.Activity(
            type=discord.ActivityType.listening,
            name="/request audiobook searches",
        ),
    )
    logger.info("Logged in as %s and presence set to online", bot.user)


@bot.event
async def on_disconnect():
    logger.warning("Disconnected from the Discord gateway")


@bot.event
async def on_resumed():
    logger.info("Discord gateway session resumed")


@bot.tree.command(name="absreport", description="Report an issue with an audiobook")
async def slash_absreport(interaction: discord.Interaction):
    if ABS_REPORT_CHANNEL_ID is None:
        await interaction.response.send_message(
            "❌ Audiobook reporting is not configured. Set `ABS_REPORT_CHANNEL_ID`.",
            ephemeral=True,
        )
        return
    await interaction.response.send_modal(ABSReportModal())


@bot.tree.command(name="request", description="Search for an audiobook by title, author, or both")
@app_commands.describe(
    title="Audiobook title (optional when author is provided)",
    author="Author name (optional when title is provided)",
    collection="Yes: request every book in the selected series",
)
async def slash_request(
    interaction: discord.Interaction,
    title: str | None = None,
    author: str | None = None,
    collection: bool = False,
):
    title = (title or "").strip()
    author = (author or "").strip()
    if not title and not author:
        await interaction.response.send_message(
            "Please enter a title, an author, or both.",
            ephemeral=True,
        )
        return

    await interaction.response.defer()
    search_label = f"{title} by {author}" if title and author else title or f"Author: {author}"
    search_terms = []
    if title and author:
        search_terms.append(f"{title} {author}")
    if title:
        search_terms.append(title)
    if author:
        search_terms.append(author)
    search_terms = list(dict.fromkeys(search_terms))

    try:
        started = time.monotonic()
        responses = await asyncio.gather(
            *(
                api.get("/api/v1/book/lookup", params={"term": term})
                for term in search_terms
            )
        )
        elapsed = time.monotonic() - started
        raw_results = []
        valid_response_seen = False
        unexpected_response = False
        first_http_error = None

        for term, (status, results, response_text) in zip(search_terms, responses):
            if status in (401, 403):
                logger.error("Book lookup authorization failed with status %s in %.2fs", status, elapsed)
                await interaction.followup.send(
                    "❌ Bookshelf rejected the API key. Check `BOOKSHELF_API_KEY` in the container settings."
                )
                return
            if status == 503:
                logger.warning(
                    "Book lookup returned 503 for %r in %.2fs: %s",
                    term,
                    elapsed,
                    response_text[:300],
                )
                continue
            if status != 200:
                logger.error(
                    "Book lookup for %r failed with status %s in %.2fs: %s",
                    term,
                    status,
                    elapsed,
                    response_text[:300],
                )
                first_http_error = first_http_error or status
                continue
            if not isinstance(results, list):
                logger.error(
                    "Book lookup for %r returned %s instead of a list: %s",
                    term,
                    type(results).__name__,
                    response_text[:300],
                )
                unexpected_response = True
                continue

            valid_response_seen = True
            raw_results.extend(results)

        if not raw_results:
            if valid_response_seen or (not first_http_error and not unexpected_response):
                logger.info("Lookup for %r returned no metadata matches in %.2fs", search_label, elapsed)
                await interaction.followup.send(no_results_message(search_label))
            elif unexpected_response:
                await interaction.followup.send(
                    "❌ Bookshelf returned an unexpected lookup response. Check the container logs."
                )
            else:
                await interaction.followup.send(
                    f"❌ Bookshelf lookup failed with HTTP {first_http_error}. "
                    "Check the container logs and `BOOKSHELF_URL`."
                )
            return

        final_results = rank_and_limit_results(raw_results, title, author)
        logger.info(
            "Lookup for %r returned %d raw and %d displayed results in %.2fs",
            search_label,
            len(raw_results),
            len(final_results),
            elapsed,
        )
        if not final_results:
            await interaction.followup.send(no_results_message(search_label))
            return

        safe_search_label = discord.utils.escape_markdown(
            discord.utils.escape_mentions(search_label[:200])
        )
        selection_prompt = (
            "Select any book from the collection to request the full series:"
            if collection
            else "Select below:"
        )
        await interaction.followup.send(
            f"🎧 Found {len(final_results)} match(es) for **{safe_search_label}**. {selection_prompt}",
            view=BookSelectView(final_results, collection_requested=collection),
        )
    except BookshelfError:
        logger.exception("Book lookup failed")
        await interaction.followup.send(
            "❌ Bookshelf could not complete the search. Please try again shortly."
        )


if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN or DISCORD_BOT_TOKEN must be configured")
if not API_KEY:
    raise RuntimeError("BOOKSHELF_API_KEY must be configured")

bot.run(TOKEN)
