import os
import re
import time
import shutil
import asyncio
from datetime import datetime
from PIL import Image
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import InputMediaDocument, Message
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser
from plugins.antinsfw import check_anti_nsfw
from helper.utils import progress_for_pyrogram, humanbytes, convert
from helper.database import codeflixbots
from config import Config
import aiofiles

# ------------------ Performance Optimizations ------------------ #
NUM_WORKERS = 3  # Adjust based on server capacity
renaming_operations = {}
rename_queue = asyncio.Queue()
USER_CACHE = {}  # Cache for user preferences
CACHE_TTL = 5  # Seconds to cache user settings

# ------------------ Patterns (Preserved Original) ------------------ #
pattern1 = re.compile(r'S(\d+)(?:E|EP)(\d+)')
pattern2 = re.compile(r'S(\d+)\s*(?:E|EP|-\s*EP)(\d+)')
pattern3 = re.compile(r'(?:[([<{]?\s*(?:E|EP)\s*(\d+)\s*[)\]>}]?)')
pattern3_2 = re.compile(r'(?:\s*-\s*(\d+)\s*)')
pattern4 = re.compile(r'S(\d+)[^\d]*(\d+)', re.IGNORECASE)
patternX = re.compile(r'(\d+)')
pattern5 = re.compile(r'\b(?:.*?(\d{3,4}[^\dp]*p).*?|.*?(\d{3,4}p))\b', re.IGNORECASE)
pattern6 = re.compile(r'[([<{]?\s*4k\s*[)\]>}]?', re.IGNORECASE)
pattern7 = re.compile(r'[([<{]?\s*2k\s*[)\]>}]?', re.IGNORECASE)
pattern8 = re.compile(r'[([<{]?\s*HdRip\s*[)\]>}]?|\bHdRip\b', re.IGNORECASE)
pattern9 = re.compile(r'[([<{]?\s*4kX264\s*[)\]>}]?', re.IGNORECASE)
pattern10 = re.compile(r'[([<{]?\s*4kx265\s*[)\]>}]?', re.IGNORECASE)
pattern11 = re.compile(r'Vol(\d+)\s*-\s*Ch(\d+)', re.IGNORECASE)

# ------------------ Optimized Helper Functions ------------------ #
async def get_cached_user_data(user_id):
    """Cache user settings to reduce database calls"""
    now = time.time()
    if user_id in USER_CACHE:
        data, timestamp = USER_CACHE[user_id]
        if now - timestamp < CACHE_TTL:
            return data
    
    data = {
        'format_template': await codeflixbots.get_format_template(user_id),
        'media_preference': await codeflixbots.get_media_preference(user_id),
        'title': await codeflixbots.get_title(user_id),
        'artist': await codeflixbots.get_artist(user_id),
        'author': await codeflixbots.get_author(user_id),
        'video': await codeflixbots.get_video(user_id),
        'audio': await codeflixbots.get_audio(user_id),
        'subtitle': await codeflixbots.get_subtitle(user_id)
    }
    USER_CACHE[user_id] = (data, now)
    return data

# ------------------ Optimized File Handling ------------------ #
async def async_download_file(client, message, renamed_file_path):
    """Asynchronous file download using stream_media"""
    file_id = message.document.file_id if message.document else message.video.file_id
    async with aiofiles.open(renamed_file_path, 'wb') as out_file:
        async for chunk in client.stream_media(file_id):
            await out_file.write(chunk)

# ------------------ Modified Process Function ------------------ #
async def process_rename(client: Client, message: Message):
    ph_path = None
    user_id = message.from_user.id
    user_data = await get_cached_user_data(user_id)
    
    if not user_data['format_template']:
        return await message.reply_text("Please Set An Auto Rename Format First Using /autorename")

    # File type detection (original logic preserved)
    if message.document:
        file_id = message.document.file_id
        file_name = message.document.file_name
        media_type = user_data['media_preference'] or "document"
        is_pdf = message.document.mime_type == "application/pdf"
    elif message.video:
        file_id = message.video.file_id
        file_name = f"{message.video.file_name}.mp4"
        media_type = user_data['media_preference'] or "video"
        is_pdf = False
    elif message.audio:
        file_id = message.audio.file_id
        file_name = f"{message.audio.file_name}.mp3"
        media_type = user_data['media_preference'] or "audio"
        is_pdf = False
    else:
        return await message.reply_text("Unsupported File Type")

    # Anti-NSFW check (original logic preserved)
    if await check_anti_nsfw(file_name, message):
        return await message.reply_text("NSFW content detected. File upload rejected.")

    # Rate limiting (original logic preserved)
    if file_id in renaming_operations:
        elapsed_time = (datetime.now() - renaming_operations[file_id]).seconds
        if elapsed_time < 10:
            return

    renaming_operations[file_id] = datetime.now()

    # ------------------ Optimized Renaming Logic ------------------ #
    format_template = user_data['format_template']
    
    # Episode/Season extraction (original logic preserved)
    episode_number = extract_episode_number(file_name)
    if episode_number:
        format_template = format_template.replace("[EP.NUM]", str(episode_number)).replace("{episode}", str(episode_number))

    season_number = extract_season_number(file_name)
    if season_number:
        format_template = format_template.replace("[SE.NUM]", str(season_number)).replace("{season}", str(season_number))

    # Quality handling (original logic preserved with early exit)
    if not is_pdf:
        extracted_quality = extract_quality(file_name)
        if extracted_quality == "Unknown":
            await message.reply_text("**__Quality Extraction Failed - Using 'Unknown'...__**")
            del renaming_operations[file_id]
            return
        format_template = format_template.replace("[QUALITY]", extracted_quality).replace("{quality}", extracted_quality)

    # File paths (original logic preserved)
    _, file_extension = os.path.splitext(file_name)
    renamed_file_name = f"{format_template}{file_extension}"
    renamed_file_path = f"downloads/{renamed_file_name}"
    metadata_file_path = f"Metadata/{renamed_file_name}"
    os.makedirs(os.path.dirname(renamed_file_path), exist_ok=True)
    os.makedirs(os.path.dirname(metadata_file_path), exist_ok=True)

    # ------------------ Optimized Download ------------------ #
    download_msg = await message.reply_text("**__Downloading...__**")
    try:
        await async_download_file(client, message, renamed_file_path)
    except Exception as e:
        del renaming_operations[file_id]
        return await download_msg.edit(f"**Download Error:** {e}")

    # ------------------ Optimized Metadata Handling ------------------ #
    await download_msg.edit("**__Processing Metadata...__**")
    try:
        ffmpeg_cmd = shutil.which('ffmpeg')
        if not ffmpeg_cmd:
            await download_msg.edit("**Error:** `ffmpeg` not found.")
            return

        # Simplified FFmpeg command
        metadata_command = [
            ffmpeg_cmd,
            '-i', renamed_file_path,
            '-map_metadata', '0',
            '-metadata', f'title={user_data["title"]}',
            '-c', 'copy',
            '-loglevel', 'error',
            metadata_file_path
        ]

        process = await asyncio.create_subprocess_exec(
            *metadata_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await process.communicate()

        if process.returncode != 0:
            await download_msg.edit("**Metadata Error**")
            return

        # ------------------ Optimized Upload ------------------ #
        upload_msg = await download_msg.edit("**__Uploading...__**")
        c_caption = await codeflixbots.get_caption(message.chat.id)
        caption = c_caption.format(
            filename=renamed_file_name,
            filesize=humanbytes(message.document.file_size),
            duration=convert(0),
        ) if c_caption else f"**{renamed_file_name}**"

        # Async thumbnail processing
        c_thumb = await codeflixbots.get_thumbnail(message.chat.id)
        if c_thumb:
            ph_path = await client.download_media(c_thumb)
        elif media_type == "video" and message.video.thumbs:
            ph_path = await client.download_media(message.video.thumbs[0].file_id)

        if ph_path and os.path.exists(ph_path):
            try:
                img = Image.open(ph_path).convert("RGB")
                img = await asyncio.to_thread(img.resize, (320, 320))
                await asyncio.to_thread(img.save, ph_path, "JPEG")
            except Exception as e:
                ph_path = None

        # Upload logic (preserved original)
        try:
            upload_method = {
                "document": client.send_document,
                "video": client.send_video,
                "audio": client.send_audio
            }[media_type]

            await upload_method(
                message.chat.id,
                metadata_file_path,
                thumb=ph_path,
                caption=caption,
                progress=progress_for_pyrogram,
                progress_args=("Upload Started...", upload_msg, time.time())
            )
        finally:
            # ------------------ Optimized Cleanup ------------------ #
            for path in [renamed_file_path, metadata_file_path, ph_path]:
                if path and os.path.exists(path):
                    os.remove(path)
            del renaming_operations[file_id]
            await download_msg.delete()

    except Exception as e:
        await upload_msg.edit(f"Error: {e}")
        for path in [renamed_file_path, metadata_file_path, ph_path]:
            if path and os.path.exists(path):
                os.remove(path)

# ------------------ Worker Initialization ------------------ #
async def rename_worker():
    while True:
        client, message = await rename_queue.get()
        try:
            await process_rename(client, message)
        except Exception as e:
            print(f"Worker Error: {e}")
        finally:
            rename_queue.task_done()

# Start multiple workers
for _ in range(NUM_WORKERS):
    asyncio.create_task(rename_worker())

# ------------------ Preserved Original Handler ------------------ #
@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_rename_files(client, message):
    await rename_queue.put((client, message))
