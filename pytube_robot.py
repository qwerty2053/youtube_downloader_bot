try:
    from aiogram import Bot, Dispatcher, executor, types
    from aiogram.bot.api import TelegramAPIServer
except ImportError:
    print("aiogram module is not installed.")
    exit(1)

try:
    import pytube
except ImportError:
    print("pytube module is not installed.")
    exit(1)

from datetime import datetime
from io import BytesIO
import sqlite3 as sql
import subprocess
import logging
import urllib
import os
import re


if subprocess.getstatusoutput("ffmpeg")[0] != 1:
    print("ffmpeg is not installed")
    exit(1)

TOKEN = "6108391846:AAGB8WgwZRnZMS4FsO3mHpUY-hEiP3fsajQ"
ESCAPE_CHARS_REGEX = re.compile(r"[_*\\~`>#+\-=|{}.!()\[\]]")
VIDEO_URL_REGEX = re.compile(r"^((?:https?:)?\/\/)?((?:www|m)\.)?((?:youtube(-nocookie)?\.com|youtu.be))(\/(?:["
                             r"\w\-]+\?v=|embed\/|live\/|v\/)?)([\w\-]+)(\S+)?$")
PRINT_LOGS = True
DB_FILENAME = "youtube_bot_database.db"
UPLOAD_FILE_SIZE_LIMIT_MB = 2000

logging.basicConfig(level=logging.INFO)

# STORE_LOGS_IN_FILES = True
# if STORE_LOGS_IN_FILES:
#     LOGS_DIR = "yt_bot_logs"
#     if os.path.exists(LOGS_DIR) and not os.path.isdir(LOGS_DIR):
#         os.remove(LOGS_DIR)
#     os.makedirs(LOGS_DIR, exist_ok=True)


def printl(*args, **kwargs):
    """Just adds timestamp before printing for the logging purposes."""
    print(datetime.now(), *args, **kwargs)


class Database:
    """Custom ORM for the bot."""

    def __init__(self):
        self.conn = sql.connect(DB_FILENAME)
        self.cur = self.conn.cursor()
        if PRINT_LOGS:
            printl("Connected to the database.")

    def create_tables(self) -> None:
        self.cur.execute(
            """CREATE TABLE IF NOT EXISTS user (tg_id INTEGER PRIMARY KEY,
             used_times_number INTEGER DEFAULT 0)""")
        self.conn.commit()

    def _user_exists(self, tg_id) -> bool:
        res = self.cur.execute(
            "SELECT tg_id FROM user WHERE tg_id = ?", (tg_id,))
        return bool(res.fetchone())

    def add_user(self, tg_id) -> None:
        if not self._user_exists(tg_id):
            self.cur.execute("INSERT INTO user (tg_id) VALUES (?)", (tg_id,))
            self.conn.commit()

    def used(self, tg_id) -> None:
        """Increments used_times_number of the user"""
        if not self._user_exists(tg_id):
            self.add_user(tg_id)
        self.cur.execute("""UPDATE user SET used_times_number =
            used_times_number + 1 WHERE tg_id = ?""", (tg_id,))

    def get_used_times_count(self, tg_id) -> int:
        if not self._user_exists(tg_id):
            self.add_user(tg_id)
        res = self.cur.execute(
            """SELECT used_times_number FROM user WHERE tg_id = ?""",
            (tg_id,))
        return res.fetchone()

    def __del__(self):
        self.conn.close()


def get_video_info(url: str, logs=False) -> dict:
    if PRINT_LOGS and logs:
        printl(f"Fetching {url}")
    yt = pytube.YouTube(url)
    yt.bypass_age_gate()

    # Collects width and height of videos
    streams_data = yt.streaming_data
    resolutions = {}
    for i in streams_data["formats"]:
        if "height" in i:
            resolutions[i["itag"]] = {"width": i["width"], "height": i["height"]}
    for i in streams_data["adaptiveFormats"]:
        if "height" in i:
            resolutions[i["itag"]] = {"width": i["width"], "height": i["height"]}

    video_info = {"audio": [],
                  "video": [],
                  "info": {"title": yt.title,
                           "watch_url": yt.watch_url,
                           "author": yt.author,
                           "channel_url": yt.channel_url,
                           "thumbnail_url": yt.thumbnail_url,
                           "length": yt.length}
                 }

    for stream in yt.streams:
        stream_info = {"stream": stream,
                       "filesize_mb": round(stream.filesize_mb, 1),
                       "is_progressive": stream.is_progressive,
                       "default_filename": stream.default_filename}

        if stream.type == "audio":
            stream_info["bitrate"] = stream.abr
            stream_info["lang"] = [[i[-2:] for i in stream.url.split("&") if i.startswith("xtags")] or [None]][0][0]
            video_info["audio"].append(stream_info)
        elif stream.type == "video":
            stream_info["resolution"] = stream.resolution
            stream_info["fps"] = stream.fps
            stream_info["width"] = resolutions.get(stream.itag, {"width": None})["width"],
            stream_info["height"] = resolutions.get(stream.itag, {"height": None})["height"],
            video_info["video"].append(stream_info)

    if PRINT_LOGS and logs:
        printl(f"Parsed info from {url}.")

    return video_info


def video_url_match(url: str) -> bool:
    return bool(re.match(VIDEO_URL_REGEX, url))


def video_size_with_sound_normalizer(video_info: dict) -> dict:
    audio_size = int(max(video_info["audio"],
                     key=lambda x: int(x["bitrate"][:-4]))["filesize_mb"])
    for video in video_info["video"]:
        if not video["is_progressive"]:
            video["filesize_mb"] = round(video["filesize_mb"] + audio_size, 1)
    return video_info


def generate_download_options(video_info: dict) -> list[dict]:
    msg = []
    videos = sorted(video_info["video"], key=lambda x: int(x["resolution"][:-1]))
    audios = sorted(video_info["audio"], key=lambda x: int(x["bitrate"][:-4]))
    for vid in videos:
        msg.append({"msg": f"{vid['resolution']}/{vid['fps']}fps:  {vid['filesize_mb']}mb" + ("🚀" if vid["is_progressive"] else ""),
                    "itag": vid["stream"].itag, "type": "video"})
    for aud in audios:
        msg.append({"msg": f"🔊 {aud['bitrate']:>8}  {aud['filesize_mb']}mb{'  ' + aud['lang'] if aud['lang'] else ''}",
                    "itag": aud["stream"].itag, "type": "audio"})

    return msg


def download_option_to_button(item):
    return types.InlineKeyboardButton(text=item["msg"], callback_data=f"{item['itag']} {item['type']}")


def download_options_to_inline_markup(download_options: list[dict]) -> types.InlineKeyboardMarkup:
    markup_inline = types.InlineKeyboardMarkup()
    download_options = map(download_option_to_button, download_options)
    two_els = []
    for index, el in enumerate(download_options, 1):
        two_els.append(el)
        if index % 2 == 0:
            markup_inline.row(*two_els)
            two_els = []
    if two_els:
        markup_inline.row(*two_els)

    return markup_inline


def markdown_prepare(s: str) -> str:
    """Adds backslashes before MarkdownV2 special characters"""
    formatted = []
    for ch in s:
        if re.match(ESCAPE_CHARS_REGEX, ch):
            formatted.append("\\" + ch)
        else:
            formatted.append(ch)
    return "".join(formatted)


def generate_video_title_and_author_message(video_info: dict) -> str:
    video_title = markdown_prepare(video_info["info"]["title"])
    video_url = markdown_prepare(video_info["info"]["watch_url"])
    video_link_msg = f"📹 [{video_title}]({video_url})"

    author_name = markdown_prepare(video_info["info"]["author"])
    channel_url = markdown_prepare(video_info["info"]["channel_url"])
    author_link_msg = f"👤 [{author_name}]({channel_url})"

    return "\n\n".join([video_link_msg, author_link_msg]) + "\n"


def generate_link_reply_message(video_info: dict) -> str:
    msg = [generate_video_title_and_author_message(video_info), "Select a format for downloading ↓"]
    return "\n".join(msg)


def download_from_youtube(stream: pytube.Stream, filename) -> str:
    if PRINT_LOGS:
        printl(f"Downloading {filename}")
    try:
        stream.download(filename=filename)
    except Exception:
        printl(f"Download error {filename}")
    else:
        if PRINT_LOGS:
            printl(f"Downloaded {filename}.")
    return filename


def merge_audio_and_video(audio_path: str, video_path: str, output_file: str) -> None:
    # ffmpeg -i video.mp4 -i audio.wav -c:v copy -c:a aac output.mp4
    # ffmpeg -i video.mp4 -i audio.wav -c:v copy output.mp4
    # ffmpeg -i video.mp4 -i audio.wav -c copy output.mkv
    if PRINT_LOGS:
        printl(f"Merging {output_file}")
    cmd = f"ffmpeg -loglevel quiet -i \"{video_path}\" -i \"{audio_path}\" -c:v copy "\
          f"\"{output_file}\" && rm \"{video_path}\" && rm \"{audio_path}\""
    if os.system(cmd) != 0:
        printl("Merging error")
    elif PRINT_LOGS:
        printl(f"Merged {output_file}.")
    return output_file


def convert2mp3(filename: str) -> str:
    fname_mp3 = filename[:filename.rfind(".")] + ".mp3"
    cmd = f"ffmpeg -loglevel quiet -i \"{filename}\" -vn \"{fname_mp3}\" && rm \"{filename}\""
    if PRINT_LOGS:
        printl(f"Converting to mp3 {filename}")
    if os.system(cmd) != 0:
        printl(f"Converting error {filename}")
    elif PRINT_LOGS:
        printl(f"Converted to {fname_mp3}.")
    return fname_mp3


def convert2mp4(filename: str) -> str:
    fname_mp4 = filename[:filename.rfind(".")] + ".mp4"
    cmd = f"ffmpeg -loglevel quiet -i \"{filename}\" -c copy \"{fname_mp4}\" && rm \"{filename}\""
    if PRINT_LOGS:
        printl(f"Converting to mp4 {filename}")
    if os.system(cmd) != 0:
        printl(f"Converting error {filename}")
    elif PRINT_LOGS:
        printl(f"Converted to {fname_mp4}.")
    return fname_mp4


def generate_success_message(video_info, type_: str, res=None, fps=None, bitrate=None) -> str:
    link = generate_video_title_and_author_message(video_info)
    if type_ == "video":
        link += f"\n📹 {res}/{fps}fps"
    elif type_ == "audio":
        link += f"\n🔊 {bitrate}"

    return link


def make_unique_filename(filename: str) -> str:
    listdir = os.listdir()
    if filename not in listdir:
        return filename
    title, _, extension = filename.rpartition(".")
    new_filename = f"{title}_2.{extension}"
    if new_filename not in listdir:
        return new_filename
    listdir = [i for i in listdir if i.startswith(title) and i.endswith(extension)]
    i = 3
    while new_filename in listdir:
        i += 1
        new_filename = f"{title}_{i}.{extension}"
    return new_filename


local_server = TelegramAPIServer.from_base("http://localhost:8081")

bot = Bot(token=TOKEN, server=local_server)
dp = Dispatcher(bot)

db = Database()
db.create_tables()


@dp.callback_query_handler()
async def report(call):
    itag, type_ = call.data.split()
    video_url = call.message.caption_entities[0].url
    video_info = video_size_with_sound_normalizer(get_video_info(video_url))

    await bot.edit_message_caption(chat_id=call.from_user.id,
                                   caption=generate_video_title_and_author_message(
                                       video_info) + "\n📥 Downloading\\.\\.\\.",
                                   message_id=call.message.message_id,
                                   parse_mode="MarkdownV2")

    stream = [i for i in video_info[type_] if str(i["stream"].itag) == itag][0]
    if stream["filesize_mb"] >= UPLOAD_FILE_SIZE_LIMIT_MB:
        await bot.send_message(call.from_user.id,
                               generate_video_title_and_author_message(video_info) + f"\n🛑 Cannot send the file \\({UPLOAD_FILE_SIZE_LIMIT_MB} Mb limit\\)",
                               parse_mode="MarkdownV2")
        await bot.delete_message(call.from_user.id, call.message.message_id)
        if PRINT_LOGS:
            printl(f"{UPLOAD_FILE_SIZE_LIMIT_MB} Mb limit error for {stream['default_filename']}.")
        return
    unique_filename = make_unique_filename(f"{video_url[-11:]}.{stream['default_filename'].split('.')[-1]}")
    filename = download_from_youtube(stream["stream"], unique_filename)

    if type_ == "video" and not stream["is_progressive"]:
        # Somehow ask the user to choose the language!!!!!!!!!!!!!!!!!!!!!!!!!!!
        audio_stream = max(video_info["audio"], key=lambda x: int(x["bitrate"][:-4]))["stream"]
        download_audio_filename = make_unique_filename(f"{video_url[-11:]}.{audio_stream['default_filename'].split('.')[-1]}")
        audio_filename = download_from_youtube(audio_stream, download_audio_filename)
        await bot.edit_message_caption(chat_id=call.from_user.id,
                                       caption=generate_video_title_and_author_message(
                                           video_info) + "\n📦 Merging\\.\\.\\.",
                                       message_id=call.message.message_id,
                                       parse_mode="MarkdownV2")
        filename = merge_audio_and_video(audio_filename, filename, make_unique_filename(filename))

    if type_ == "audio" and not filename.endswith("mp3"):
        filename = convert2mp3(filename)

    if type_ == "video" and not filename.endswith("mp4"):
        filename = convert2mp4(filename)

    preview_url = video_info["info"]["thumbnail_url"]

    if type_ == "video":
        try:
            await bot.edit_message_caption(chat_id=call.from_user.id,
                                           caption=generate_video_title_and_author_message(
                                               video_info) + "\n📤 Uploading\\.\\.\\.",
                                           message_id=call.message.message_id,
                                           parse_mode="MarkdownV2")
            await bot.send_chat_action(call.from_user.id, "upload_video")
            if PRINT_LOGS:
                printl(f"Sending {filename}")
            await bot.send_video(chat_id=call.from_user.id, video=types.InputFile(filename),
                                 supports_streaming=True,
                                 caption=generate_success_message(video_info, type_="video",
                                                                  res=stream["resolution"], fps=stream["fps"]),
                                 thumb=BytesIO(urllib.request.urlopen(preview_url).read()),
                                 duration=video_info["info"]["length"],
                                 parse_mode="MarkdownV2", width=stream["width"], height=stream["height"])
            if PRINT_LOGS:
                printl(f"Sent {filename}.")
            db.used(call.from_user.id)
        except sql.ProgrammingError as exc:
            if PRINT_LOGS:
                printl(f"{exc} {filename}.")
        except Exception as exc:
            await bot.send_message(call.from_user.id,
                                   generate_video_title_and_author_message(
                                       video_info) + "\n🛑 Could not send the video file",
                                   parse_mode="MarkdownV2")
            if PRINT_LOGS:
                printl(f"{exc} {filename}.")

    elif type_ == "audio":
        try:
            await bot.send_chat_action(call.from_user.id, "upload_audio")
            if PRINT_LOGS:
                printl(f"Sending {filename}")
            await bot.send_audio(call.from_user.id, types.InputFile(filename),
                                 caption=generate_success_message(video_info, type_="audio",
                                                                  bitrate=stream["bitrate"]),
                                 parse_mode="MarkdownV2",
                                 duration=video_info["info"]["length"],
                                 performer=video_info["info"]["author"],
                                 thumb=BytesIO(urllib.request.urlopen(preview_url).read()))
            if PRINT_LOGS:
                printl(f"Sent {filename}.")
            db.used(call.from_user.id)
        except sql.ProgrammingError as exc:
            if PRINT_LOGS:
                printl(f"{exc} {filename}.")
        except Exception as exc:
            if PRINT_LOGS:
                printl(f"{exc} {filename}.")
            await bot.send_message(call.from_user.id,
                                   generate_video_title_and_author_message(
                                       video_info) + "\n🛑 Could not send the audio file",
                                   parse_mode="MarkdownV2")
    await bot.delete_message(call.from_user.id, call.message.message_id)
    os.remove(filename)


@dp.message_handler(content_types=["text"])
async def get_text(message):
    if message.text == "/start":
        user_name = message.from_user.first_name
        await bot.send_message(message.chat.id,
                               f"""👋 Hi {user_name}. Send me a YouTube video link"""
                               """ and I"ll download that video or audio.""")

    elif video_url_match(message.text):
        try:
            info = get_video_info(message.text, logs=True)
            info = video_size_with_sound_normalizer(info)
        except pytube.exceptions.AgeRestrictedError as exc:
            printl(exc)
            await bot.send_message(message.chat.id, f"🚫 [This video]({markdown_prepare(message.text)}) is age restricted\\.",
                                   parse_mode="MarkdownV2")
            await bot.delete_message(message.chat.id, message.message_id)
        except Exception as exc:
            printl(exc)
            await bot.send_message(message.chat.id, f"🚫 An error occured while fetching [this video]({markdown_prepare(message.text)})\\.",
                                   parse_mode="MarkdownV2")
            await bot.delete_message(message.chat.id, message.message_id)
        else:
            download_options = generate_download_options(info)
            reply = generate_link_reply_message(info)
            markup_inline = download_options_to_inline_markup(download_options)
            preview_url = info["info"]["thumbnail_url"]
            preview = BytesIO(urllib.request.urlopen(preview_url).read())
            await bot.send_chat_action(message.chat.id, "upload_photo")
            await bot.send_photo(message.chat.id, preview, caption=reply,
                                 reply_markup=markup_inline,
                                 parse_mode="MarkdownV2")
            await bot.delete_message(message.chat.id, message.message_id)
    else:
        await bot.send_message(message.chat.id,
                               f"🔗 Send me a YouTube video link")
        await bot.delete_message(message.chat.id, message.message_id)


executor.start_polling(dp, skip_updates=True)

# TODO
# Mark progressive videos
# Logging to file
# Video and audio language choice
# Thumbnail ratio
# Speed up merging audio and video
# Bypass age limit w/ logging in to a google account
# Use database
# Handle possible errors
