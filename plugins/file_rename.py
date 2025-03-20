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

# Global dictionary to prevent duplicate renaming within a short time
renaming_operations = {}

# Asyncio Queue to manage file renaming tasks
rename_queue = asyncio.Queue()

# ---------------- Patterns and utility functions ---------------- #

# Pattern 1: S01E02 or S01EP02
pattern1 = re.compile(r'S(\d+)(?:E|EP)(\d+)')
# Pattern 2: S01 E02 or S01 EP02 or S01 - E01 or S01 - EP02
pattern2 = re.compile(r'S(\d+)\s*(?:E|EP|-\s*EP)(\d+)')
# Pattern 3: Episode Number After "E" or "EP"
pattern3 = re.compile(r'(?:[([<{]?\s*(?:E|EP)\s*(\d+)\s*[)\]>}]?)')
# Pattern 3_2: episode number after - [hyphen]
pattern3_2 = re.compile(r'(?:\s*-\s*(\d+)\s*)')
# Pattern 4: S2 09 ex.
pattern4 = re.compile(r'S(\d+)[^\d]*(\d+)', re.IGNORECASE)
# Pattern X: Standalone Episode Number
patternX = re.compile(r'(\d+)')

# QUALITY PATTERNS 
# Pattern 5: 3-4 digits before 'p' as quality
pattern5 = re.compile(r'\b(?:.*?(\d{3,4}[^\dp]*p).*?|.*?(\d{3,4}p))\b', re.IGNORECASE)
# Pattern 6: Find 4k in brackets or parentheses
pattern6 = re.compile(r'[([<{]?\s*4k\s*[)\]>}]?', re.IGNORECASE)
# Pattern 7: Find 2k in brackets or parentheses
pattern7 = re.compile(r'[([<{]?\s*2k\s*[)\]>}]?', re.IGNORECASE)
# Pattern 8: Find HdRip without spaces
pattern8 = re.compile(r'[([<{]?\s*HdRip\s*[)\]>}]?|\bHdRip\b', re.IGNORECASE)
# Pattern 9: Find 4kX264 in brackets or parentheses
pattern9 = re.compile(r'[([<{]?\s*4kX264\s*[)\]>}]?', re.IGNORECASE)
# Pattern 10: Find 4kx265 in brackets or parentheses
pattern10 = re.compile(r'[([<{]?\s*4kx265\s*[)\]>}]?', re.IGNORECASE)

# Volume and Chapter Patterns
# Pattern 11: Volume and Chapter (e.g., Vol1 - Ch01)
pattern11 = re.compile(r'Vol(\d+)\s*-\s*Ch(\d+)', re.IGNORECASE)

def extract_quality(filename):
    # Try Quality Patterns
    match5 = re.search(pattern5, filename)
    if match5:
        print("Matched Pattern 5")
        quality5 = match5.group(1) or match5.group(2)  # Extracted quality from both patterns
        print(f"Quality: {quality5}")
        return quality5

    match6 = re.search(pattern6, filename)
    if match6:
        print("Matched Pattern 6")
        quality6 = "4k"
        print(f"Quality: {quality6}")
        return quality6

    match7 = re.search(pattern7, filename)
    if match7:
        print("Matched Pattern 7")
        quality7 = "2k"
        print(f"Quality: {quality7}")
        return quality7

    match8 = re.search(pattern8, filename)
    if match8:
        print("Matched Pattern 8")
        quality8 = "HdRip"
        print(f"Quality: {quality8}")
        return quality8

    match9 = re.search(pattern9, filename)
    if match9:
        print("Matched Pattern 9")
        quality9 = "4kX264"
        print(f"Quality: {quality9}")
        return quality9

    match10 = re.search(pattern10, filename)
    if match10:
        print("Matched Pattern 10")
        quality10 = "4kx265"
        print(f"Quality: {quality10}")
        return quality10    

    # Return "Unknown" if no pattern matches
    unknown_quality = "Unknown"
    print(f"Quality: {unknown_quality}")
    return unknown_quality

def extract_episode_number(filename):    
    # Try Pattern 1
    match = re.search(pattern1, filename)
    if match:
        print("Matched Pattern 1")
        return match.group(2)  # Extracted episode number
    
    # Try Pattern 2
    match = re.search(pattern2, filename)
    if match:
        print("Matched Pattern 2")
        return match.group(2)  # Extracted episode number

    # Try Pattern 3
    match = re.search(pattern3, filename)
    if match:
        print("Matched Pattern 3")
        return match.group(1)  # Extracted episode number

    # Try Pattern 3_2
    match = re.search(pattern3_2, filename)
    if match:
        print("Matched Pattern 3_2")
        return match.group(1)  # Extracted episode number
        
    # Try Pattern 4
    match = re.search(pattern4, filename)
    if match:
        print("Matched Pattern 4")
        return match.group(2)  # Extracted episode number

    # Try Pattern X
    match = re.search(patternX, filename)
    if match:
        print("Matched Pattern X")
        return match.group(1)  # Extracted episode number
        
    # Return None if no pattern matches
    return None

def extract_season_number(filename):
    """
    Season number extract karne ke liye function.
    Pehle pattern1 se season number extract karta hai (group 1), 
    agar match na ho to pattern4 se try karta hai.
    """
    # Try Pattern 1: S01E02 or S01EP02
    match = re.search(pattern1, filename)
    if match:
        print("Matched Pattern 1 for Season")
        return match.group(1)  # Extracted season number

    # Try Pattern 4: S2 09 ex.
    match = re.search(pattern4, filename)
    if match:
        print("Matched Pattern 4 for Season")
        return match.group(1)  # Extracted season number

    # Return None if no pattern matches
    return None

def extract_volume_chapter(filename):
    """
    Extract volume and chapter numbers from filename.
    """
    match = re.search(pattern11, filename)
    if match:
        print("Matched Pattern 11 for Volume and Chapter")
        return match.group(1), match.group(2)  # Extracted volume and chapter numbers
    return None, None

# ---------------- Queue Worker and Process Function ---------------- #

async def process_rename(client: Client, message: Message):
    """
    Process a single renaming request.
    This function contains the original renaming, metadata addition, and upload logic.
    """
    # Initialize ph_path to None
    ph_path = None
    
    user_id = message.from_user.id
    format_template = await codeflixbots.get_format_template(user_id)
    media_preference = await codeflixbots.get_media_preference(user_id)

    if not format_template:
        return await message.reply_text(
            "Please Set An Auto Rename Format First Using /autorename"
        )

    if message.document:
        file_id = message.document.file_id
        file_name = message.document.file_name
        media_type = media_preference or "document"
        is_pdf = message.document.mime_type == "application/pdf"
    elif message.video:
        file_id = message.video.file_id
        file_name = f"{message.video.file_name}.mp4"
        media_type = media_preference or "video"
        is_pdf = False
    elif message.audio:
        file_id = message.audio.file_id
        file_name = f"{message.audio.file_name}.mp3"
        media_type = media_preference or "audio"
        is_pdf = False
    else:
        return await message.reply_text("Unsupported File Type")

    # Anti-NSFW check
    if await check_anti_nsfw(file_name, message):
        return await message.reply_text("NSFW content detected. File upload rejected.")

    if file_id in renaming_operations:
        elapsed_time = (datetime.now() - renaming_operations[file_id]).seconds
        if elapsed_time < 10:
            return

    renaming_operations[file_id] = datetime.now()

    # Extract episode number
    episode_number = extract_episode_number(file_name)
    if episode_number:
        # Replace [EP.NUM] or {episode} with the extracted episode number
        format_template = format_template.replace("[EP.NUM]", str(episode_number))
        format_template = format_template.replace("{episode}", str(episode_number))
    
    # Extract season number
    season_number = extract_season_number(file_name)
    if season_number:
        # Replace [SE.NUM] or {season} with the extracted season number
        format_template = format_template.replace("[SE.NUM]", str(season_number))
        format_template = format_template.replace("{season}", str(season_number))

    # Extract volume and chapter
    volume_number, chapter_number = extract_volume_chapter(file_name)
    if volume_number and chapter_number:
        # Replace [Vol{volume}] and [Ch{chapter}] with extracted values
        format_template = format_template.replace("[Vol{volume}]", f"Vol{volume_number}")
        format_template = format_template.replace("[Ch{chapter}]", f"Ch{chapter_number}")

    # Skip quality extraction for PDF files
    if not is_pdf:
        # Extract quality
        extracted_quality = extract_quality(file_name)
        if extracted_quality != "Unknown":
            # Replace [QUALITY] or {quality} with the extracted quality
            format_template = format_template.replace("[QUALITY]", extracted_quality)
            format_template = format_template.replace("{quality}", extracted_quality)
        else:
            await message.reply_text("**__I Was Not Able To Extract The Quality Properly. Renaming As 'Unknown'...__**")
            # Mark the file as ignored
            del renaming_operations[file_id]
            return  # Exit if quality extraction fails

    # Add file extension
    _, file_extension = os.path.splitext(file_name)
    renamed_file_name = f"{format_template}{file_extension}"
    renamed_file_path = f"downloads/{renamed_file_name}"
    metadata_file_path = f"Metadata/{renamed_file_name}"
    os.makedirs(os.path.dirname(renamed_file_path), exist_ok=True)
    os.makedirs(os.path.dirname(metadata_file_path), exist_ok=True)

    download_msg = await message.reply_text("**__Downloading...__**")

    try:
        path = await client.download_media(
            message,
            file_name=renamed_file_path,
            progress=progress_for_pyrogram,
            progress_args=("Download Started...", download_msg, time.time()),
        )
    except Exception as e:
        del renaming_operations[file_id]
        return await download_msg.edit(f"**Download Error:** {e}")

    await download_msg.edit("**__Renaming and Adding Metadata...__**")

    try:
        # Rename the file
        os.rename(path, renamed_file_path)
        path = renamed_file_path

        # Prepare metadata command
        ffmpeg_cmd = shutil.which('ffmpeg')
        if ffmpeg_cmd is None:
            await download_msg.edit("**Error:** `ffmpeg` not found. Please install `ffmpeg` to use this feature.")
            return

        metadata_command = [
            ffmpeg_cmd,
            '-i', path,
            '-metadata', f'title={await codeflixbots.get_title(user_id)}',
            '-metadata', f'artist={await codeflixbots.get_artist(user_id)}',
            '-metadata', f'author={await codeflixbots.get_author(user_id)}',
            '-metadata:s:v', f'title={await codeflixbots.get_video(user_id)}',
            '-metadata:s:a', f'title={await codeflixbots.get_audio(user_id)}',
            '-metadata:s:s', f'title={await codeflixbots.get_subtitle(user_id)}',
            '-map', '0',
            '-c', 'copy',
            '-loglevel', 'error',
            metadata_file_path
        ]

        # Execute the metadata command
        process = await asyncio.create_subprocess_exec(
            *metadata_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            error_message = stderr.decode()
            await download_msg.edit(f"**Metadata Error:**\n{error_message}")
            return

        # Use the new metadata file path for the upload
        path = metadata_file_path

        # Upload the file
        upload_msg = await download_msg.edit("**__Uploading...__**")

        c_caption = await codeflixbots.get_caption(message.chat.id)
        c_thumb = await codeflixbots.get_thumbnail(message.chat.id)

        caption = (
            c_caption.format(
                filename=renamed_file_name,
                filesize=humanbytes(message.document.file_size),
                duration=convert(0),
            )
            if c_caption
            else f"**{renamed_file_name}**"
        )

        # Check for thumbnail
        if c_thumb:
            ph_path = await client.download_media(c_thumb)
        elif media_type == "video" and message.video.thumbs:
            ph_path = await client.download_media(message.video.thumbs[0].file_id)

        # Process thumbnail if it exists
        if ph_path and os.path.exists(ph_path):
            try:
                from PIL import Image
                img = Image.open(ph_path).convert("RGB")
                img = img.resize((320, 320))
                img.save(ph_path, "JPEG")
            except Exception as e:
                await upload_msg.edit(f"**Thumbnail Processing Error:** {e}")
                ph_path = None  # Reset ph_path if thumbnail processing fails

        try:
            if media_type == "document":
                await client.send_document(
                    message.chat.id,
                    document=path,
                    thumb=ph_path,
                    caption=caption,
                    progress=progress_for_pyrogram,
                    progress_args=("Upload Started...", upload_msg, time.time()),
                )
            elif media_type == "video":
                await client.send_video(
                    message.chat.id,
                    video=path,
                    caption=caption,
                    thumb=ph_path,
                    duration=0,
                    progress=progress_for_pyrogram,
                    progress_args=("Upload Started...", upload_msg, time.time()),
                )
            elif media_type == "audio":
                await client.send_audio(
                    message.chat.id,
                    audio=path,
                    caption=caption,
                    thumb=ph_path,
                    duration=0,
                    progress=progress_for_pyrogram,
                    progress_args=("Upload Started...", upload_msg, time.time()),
                )
            elif is_pdf:
                await client.send_document(
                    message.chat.id,
                    document=path,
                    thumb=ph_path,
                    caption=caption,
                    progress=progress_for_pyrogram,
                    progress_args=("Upload Started...", upload_msg, time.time()),
                )
        except Exception as e:
            os.remove(renamed_file_path)
            if ph_path:
                os.remove(ph_path)
            return await upload_msg.edit(f"Error: {e}")

        await download_msg.delete() 
        os.remove(path)  # Remove the original downloaded or metadata file
        if ph_path:
            os.remove(ph_path)

    finally:
        # Clean up temporary files
        if os.path.exists(renamed_file_path):
            os.remove(renamed_file_path)
        if os.path.exists(metadata_file_path):
            os.remove(metadata_file_path)
        if ph_path and os.path.exists(ph_path):
            os.remove(ph_path)
        del renaming_operations[file_id]

async def rename_worker():
    """
    Continuously process renaming tasks from the queue sequentially.
    """
    while True:
        client, message = await rename_queue.get()
        try:
            await process_rename(client, message)
        except Exception as e:
            print(f"Error processing rename task: {e}")
        finally:
            rename_queue.task_done()

# ---------------- Telegram Message Handler ---------------- #

@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_rename_files(client, message):
    """
    Instead of processing immediately, add the task to the rename queue.
    """
    await rename_queue.put((client, message))

# Start the rename worker in the background when this module is imported
asyncio.create_task(rename_worker())
