try:
    from aiogram import Bot, Dispatcher, executor, types
    from aiogram.bot.api import TelegramAPIServer
    from aiogram.types import ContentType
except ImportError:
    print("aiogram module is not installed.")
    exit(1)

try:
    import pytube
except ImportError:
    print("pytube module is not installed.")
    exit(1)

from datetime import datetime
from pprint import pprint
from io import BytesIO
import sqlite3 as sql
import subprocess
import requests
import logging
import urllib
import os
import re


if subprocess.getstatusoutput('ffmpeg')[0] != 1:
    print("ffmpeg is not installed")
    exit(1)

TOKEN = ""
ESCAPE_CHARS_REGEX = re.compile(r"[_*\\~`>#+\-=|{}.!()\[\]]")
VIDEO_URL_REGEX = re.compile(r"^((?:https?:)?\/\/)?((?:www|m)\.)?((?:youtube(-nocookie)?\.com|youtu.be))(\/(?:["
                             r"\w\-]+\?v=|embed\/|live\/|v\/)?)([\w\-]+)(\S+)?$")
PRINT_LOGS = True
DB_FILENAME = "youtube_bot_database.db"
UPLOAD_FILE_SIZE_LIMIT_MB = 2000

logging.basicConfig(level=logging.INFO)


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


def get_video_info(url: str) -> dict:
    if PRINT_LOGS:
        printl(f"Fetching {url}")
    yt = pytube.YouTube(url)
    yt.bypass_age_gate()

    # Collects width and height of videos
    streams_data = yt.streaming_data
    resolutions = {}
    for i in streams_data["formats"]:
        if 'height' in i:
            resolutions[i['itag']] = {"width": i['width'], "height": i['height']}
    for i in streams_data["adaptiveFormats"]:
        if 'height' in i:
            resolutions[i['itag']] = {"width": i['width'], "height": i['height']}

    video_info = {'audio': [],
                  'video': [],
                  'info': {'title': yt.title,
                           'watch_url': yt.watch_url,
                           'author': yt.author,
                           'channel_url': yt.channel_url,
                           'thumbnail_url': yt.thumbnail_url,
                           'length': yt.length}
                 }

    itags = set()
    for stream in yt.streams:
        if stream.itag in itags:
            continue
        itags.add(stream.itag)
        stream_info = {'stream': stream,
                       'filesize_mb': round(stream.filesize_mb, 1),
                       'is_progressive': stream.is_progressive,
                       'default_filename': stream.default_filename}

        if stream.type == 'audio':
            stream_info['bitrate'] = stream.abr
            video_info['audio'].append(stream_info)
        elif stream.type == 'video':
            stream_info['resolution'] = stream.resolution
            stream_info['fps'] = stream.fps
            stream_info["width"] = resolutions.get(stream.itag, {"width": None})["width"],
            stream_info["height"] = resolutions.get(stream.itag, {"height": None})["height"],
            video_info['video'].append(stream_info)

    if PRINT_LOGS:
        printl(f"Parsed info from {url}.")

    return video_info


def video_url_match(url: str) -> bool:
    return bool(re.match(VIDEO_URL_REGEX, url))


def video_size_with_sound_normalizer(video_info: dict) -> dict:
    audio_size = int(max(video_info['audio'],
                     key=lambda x: int(x['bitrate'][:-4]))['filesize_mb'])
    for video in video_info["video"]:
        video["filesize_mb"] = round(video["filesize_mb"] + audio_size, 1)
    return video_info


def generate_download_options(video_info: dict) -> list[dict]:
    msg = []
    videos = sorted(video_info['video'], key=lambda x: int(x['resolution'][:-1]))
    audios = sorted(video_info['audio'], key=lambda x: int(x['bitrate'][:-4]))
    for vid in videos:
        msg.append({'msg': f"{vid['resolution']}/{vid['fps']}fps:  {vid['filesize_mb']}mb",
                    'itag': vid['stream'].itag, 'type': 'video'})
    for aud in audios:
        msg.append({'msg': f"🔊 {aud['bitrate']:>8}  {aud['filesize_mb']}mb",
                    'itag': aud['stream'].itag, 'type': 'audio'})

    return msg


def download_options_to_inline_markup(download_options: list[dict]) -> types.InlineKeyboardMarkup:
    markup_inline = types.InlineKeyboardMarkup()
    items = []
    for i, item in enumerate(download_options, 1):
        button = types.InlineKeyboardButton(text=item['msg'],
                                            callback_data=f"{item['itag']} {item['type']}")
        items.append(button)
        if i % 2 == 0:
            markup_inline.row(*items)
            items = []
    if items:
        markup_inline.row(*items)

    return markup_inline


def markdown_prepare(s: str) -> str:
    """Adds backslashes before MarkdownV2 special characters"""
    formatted = []
    for ch in s:
        if re.match(ESCAPE_CHARS_REGEX, ch):
            formatted.append("\\" + ch)
        else:
            formatted.append(ch)
    return ''.join(formatted)


def generate_video_title_and_author_message(video_info: dict) -> str:
    video_title = markdown_prepare(video_info['info']['title'])
    video_url = markdown_prepare(video_info['info']['watch_url'])
    video_link_msg = f"📹 [{video_title}]({video_url})"

    author_name = markdown_prepare(video_info['info']['author'])
    channel_url = markdown_prepare(video_info['info']['channel_url'])
    author_link_msg = f"👤 [{author_name}]({channel_url})"

    return '\n\n'.join([video_link_msg, author_link_msg]) + "\n"


def generate_link_reply_message(video_info: dict) -> str:
    msg = [generate_video_title_and_author_message(video_info), 'Select a format for downloading ↓']
    return '\n'.join(msg)


def download_from_youtube(stream) -> str:
    filename = str(stream.default_filename)
    if PRINT_LOGS:
        printl(f"Downloading {filename}")
    try:
        stream.download()
    except Exception:
        printl(f"Download error {filename}")
    else:
        if PRINT_LOGS:
            printl(f"Downloaded {filename}.")
    return filename


def merge_audio_and_video(audio_path: str, video_path: str, title: str) -> None:
    # ffmpeg -i video.mp4 -i audio.wav -c:v copy -c:a aac output.mp4
    # ffmpeg -i video.mp4 -i audio.wav -c:v copy output.mp4
    # ffmpeg -i video.mp4 -i audio.wav -c copy output.mkv
    if PRINT_LOGS:
        printl(f"Merging {title}")
    cmd = f"ffmpeg -loglevel quiet -i \"{video_path}\" -i \"{audio_path}\" -c:v copy "\
          f"\"{title}\" && rm \"{video_path}\" && rm \"{audio_path}\""
    if os.system(cmd) != 0:
        printl("Merging error")
    elif PRINT_LOGS:
        printl(f"Merged {title}.")


def convert2mp3(filename: str) -> str:
    fname_mp3 = filename[:filename.rfind('.')] + ".mp3"
    cmd = f"ffmpeg -loglevel quiet -i \"{filename}\" -vn \"{fname_mp3}\" && rm \"{filename}\""
    if PRINT_LOGS:
        printl(f"Converting to mp3 {filename}")
    if os.system(cmd) != 0:
        printl(f"Converting error {filename}")
    elif PRINT_LOGS:
        printl(f"Converted to {fname_mp3}.")
    return fname_mp3


def convert2mp4(filename: str) -> str:
    fname_mp4 = filename[:filename.rfind('.')] + ".mp4"
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
    if type_ == 'video':
        link += f"\n📹 {res}/{fps}fps"
    elif type_ == 'audio':
        link += f"\n🔊 {bitrate}"

    return link


local_server = TelegramAPIServer.from_base('http://localhost:8081')

bot = Bot(token=TOKEN, server=local_server)
dp = Dispatcher(bot)

db = Database()
db.create_tables()
streams = {}


@dp.callback_query_handler()
async def report(call):
    video_url = call.message.caption_entities[0].url

    await bot.edit_message_caption(chat_id=call.from_user.id,
                                   caption=generate_video_title_and_author_message(
                                       streams[call.from_user.id][video_url]) + "\n📥 Downloading\\.\\.\\.",
                                   message_id=call.message.message_id,
                                   parse_mode="MarkdownV2")

    itag, type_ = call.data.split()
    stream = [i for i in streams[call.from_user.id][video_url][type_] if str(i['stream'].itag) == itag][0]
    if stream['filesize_mb'] >= UPLOAD_FILE_SIZE_LIMIT_MB:
        await bot.send_message(call.from_user.id,
                               generate_video_title_and_author_message(streams[call.from_user.id][video_url]) + f'\n🛑 Cannot send the file \\({UPLOAD_FILE_SIZE_LIMIT_MB} Mb limit\\)',
                               parse_mode="MarkdownV2")
        await bot.delete_message(call.from_user.id, call.message.message_id)
        del streams[call.from_user.id][video_url]
        if PRINT_LOGS:
            printl(f"{UPLOAD_FILE_SIZE_LIMIT_MB} Mb limit error for {stream['default_filename']}.")
        return
    filename = download_from_youtube(stream['stream'])
    title = filename[:]

    if type_ == 'video' and not stream['is_progressive']:
        title = str(stream['default_filename'])
        os.rename(filename, filename + str(call.from_user.id) + "video")
        filename += str(call.from_user.id) + "video"

        audio_stream = max(streams[call.from_user.id][video_url]['audio'],
                           key=lambda x: int(x['bitrate'][:-4]))['stream']
        audio_filename = download_from_youtube(audio_stream)
        os.rename(audio_filename, audio_filename + str(call.from_user.id) + 'audio')
        audio_filename += str(call.from_user.id) + "audio"
        await bot.edit_message_caption(chat_id=call.from_user.id,
                                       caption=generate_video_title_and_author_message(
                                           streams[call.from_user.id][video_url]) + "\n📦 Merging\\.\\.\\.",
                                       message_id=call.message.message_id,
                                       parse_mode="MarkdownV2")
        merge_audio_and_video(audio_filename, filename, title)

    preview_url = streams[call.from_user.id][video_url]['info']['thumbnail_url']

    if type_ == 'audio' and not title.endswith("mp3"):
        title = convert2mp3(title)

    if type_ == 'video' and not title.endswith("mp4"):
        title = convert2mp4(title)

    if type_ == 'video':
        try:
            await bot.edit_message_caption(chat_id=call.from_user.id,
                                           caption=generate_video_title_and_author_message(
                                               streams[call.from_user.id][video_url]) + "\n📤 Uploading\\.\\.\\.",
                                           message_id=call.message.message_id,
                                           parse_mode="MarkdownV2")
            await bot.send_chat_action(call.from_user.id, 'upload_video')
            if PRINT_LOGS:
                printl(f"Sending {title}")
            await bot.send_video(chat_id=call.from_user.id, video=types.InputFile(title),
                                 supports_streaming=True,
                                 caption=generate_success_message(streams[call.from_user.id][video_url], type_="video",
                                                                  res=stream['resolution'], fps=stream['fps']),
                                 thumb=BytesIO(urllib.request.urlopen(preview_url).read()),
                                 duration=streams[call.from_user.id][video_url]['info']['length'],
                                 parse_mode="MarkdownV2", width=stream["width"], height=stream["height"])
            if PRINT_LOGS:
                printl(f"Sent {title}.")
            db.used(call.from_user.id)
        except sql.ProgrammingError as exc:
            if PRINT_LOGS:
                printl(f"{exc} {title}.")
        except Exception as exc:
            await bot.send_message(call.from_user.id,
                                   generate_video_title_and_author_message(
                                       streams[call.from_user.id][video_url]) + '\n🛑 Could not send the video file',
                                   parse_mode="MarkdownV2")
            if PRINT_LOGS:
                printl(f"{exc} {title}.")

    elif type_ == 'audio':
        try:
            await bot.send_chat_action(call.from_user.id, 'upload_audio')
            if PRINT_LOGS:
                printl(f"Sending {title}")
            await bot.send_audio(call.from_user.id, types.InputFile(title),
                                 caption=generate_success_message(streams[call.from_user.id][video_url], type_="audio",
                                                                  bitrate=stream['bitrate']),
                                 parse_mode="MarkdownV2",
                                 duration=streams[call.from_user.id][video_url]['info']['length'],
                                 performer=streams[call.from_user.id][video_url]['info']['author'],
                                 thumb=BytesIO(urllib.request.urlopen(preview_url).read()))
            if PRINT_LOGS:
                printl(f"Sent {title}.")
            db.used(call.from_user.id)
        except sql.ProgrammingError as exc:
            if PRINT_LOGS:
                printl(f"{exc} {title}.")
        except Exception as exc:
            if PRINT_LOGS:
                printl(f"{exc} {title}.")
            await bot.send_message(call.from_user.id,
                                   generate_video_title_and_author_message(
                                       streams[call.from_user.id][video_url]) + '\n🛑 Could not send the audio file',
                                   parse_mode="MarkdownV2")

    await bot.delete_message(call.from_user.id, call.message.message_id)

    os.remove(title)
    del streams[call.from_user.id][video_url]


@dp.message_handler(content_types=["text"])
async def get_text(message):
    if message.text == "/start":
        user_name = message.from_user.first_name
        await bot.send_message(message.chat.id,
                               f"""👋 Hi {user_name}. Send me a YouTube video link"""
                               """ and I'll download that video or audio.""")

    elif video_url_match(message.text):
        try:
            info = get_video_info(message.text)
            info = video_size_with_sound_normalizer(info)
            streams.setdefault(message.chat.id, {})[info['info']['watch_url']] = info
        except pytube.exceptions.AgeRestrictedError as exc:
            printl(exc)
            await bot.delete_message(message.chat.id, message.message_id)
            await bot.send_message(message.chat.id, f'🚫 [This video]({markdown_prepare(message.text)}) is age restricted\\.',
                                   parse_mode="MarkdownV2")
        except Exception as exc:
            printl(exc)
            await bot.delete_message(message.chat.id, message.message_id)
            await bot.send_message(message.chat.id, f'🚫 An error occured while fetching [this video]({markdown_prepare(message.text)})\\.',
                                   parse_mode="MarkdownV2")
        else:
            download_options = generate_download_options(info)
            reply = generate_link_reply_message(info)
            markup_inline = download_options_to_inline_markup(download_options)
            preview_url = info['info']['thumbnail_url']
            preview = BytesIO(urllib.request.urlopen(preview_url).read())
            await bot.send_chat_action(message.chat.id, 'upload_photo')
            await bot.delete_message(message.chat.id, message.message_id)
            await bot.send_photo(message.chat.id, preview, caption=reply,
                                 reply_markup=markup_inline,
                                 parse_mode="MarkdownV2")

    else:
        await bot.delete_message(message.chat.id, message.message_id)
        await bot.send_message(message.chat.id,
                               f"🔗 Send me a YouTube video link")


executor.start_polling(dp, skip_updates=True)
