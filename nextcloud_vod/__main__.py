# -*- coding=utf-8 -*-
import asyncio
from datetime import timedelta
import enum
import hashlib
import json
import logging
import os
import subprocess
import textwrap
import time
import urllib.parse

import aiohttp
from aiohttp import web

logger = logging.getLogger(__name__)

listen = dict(host="127.0.0.1", port=52014)
upstream = "http://127.0.0.1:52013"
upstream_root = "/themylogin/files/Storage"
root = "/media/storage"
pool = "/media/storage/Temp/VOD"

files = {}
processes = {}
segments_count = {}
access = {}


class Action(enum.Enum):
    SLEEP_IF = 1
    SLEEP = 2
    KILL = 3
    RAISE = 4


async def cleanup_processes():
    while True:
        await asyncio.sleep(10)
        for key, last_access in list(access.items()):
            if last_access < time.monotonic() - 60:
                try:
                    processes.pop(key).kill()
                except ProcessLookupError:
                    pass
                access.pop(key)


async def ffmpeg(path, pool_path, segment_start_number=0):
    segment_time = 6

    try:
        os.unlink(os.path.join(pool_path, "complete"))
    except IOError:
        pass

    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-ss", str(timedelta(seconds=segment_start_number * segment_time)),
        "-i", path,
        "-v", "quiet",
        "-threads", "0",

        "-codec:v", "libx264",
        "-vf", "scale=trunc(min(max(iw\,ih*dar)\,1280)/2)*2:trunc(ow/dar/2)*2",
        "-pix_fmt", "yuv420p", "-preset", "veryfast", "-crf", "23", "-maxrate", "8545816", "-bufsize", "17091632",
        "-profile:v", "high", "-level", "4.1",
        "-x264opts", "subme=0:me_range=4:rc_lookahead=10:me=dia:no_chroma_me:8x8dct=0:partitions=none",
        "-force_key_frames", "expr:if(isnan(prev_forced_t),eq(t,t),gte(t,prev_forced_t+3))", "-copyts",
        "-vsync", "-1",

        "-codec:a", "libmp3lame", "-ac", "1", "-ab", "64000",

        "-f", "segment", "-max_delay", "5000000",
        "-avoid_negative_ts", "disabled", "-map_metadata", "-1", "-map_chapters", "-1", "-start_at_zero",
        "-segment_time", str(segment_time), "-segment_time_delta", str(segment_start_number * segment_time),
        "-individual_header_trailer", "0", "-segment_format", "mpegts",
        "-segment_list_type", "m3u8", "-segment_start_number", str(segment_start_number),
        "-segment_list", os.path.join(pool_path, "playlist.m3u8"),
        "-y", os.path.join(pool_path, "%d.ts")
    )

    asyncio.ensure_future(after_ffmpeg(process, pool_path))

    return process


async def after_ffmpeg(process, pool_path):
    await process.communicate()
    if process.returncode == 0:
        with open(os.path.join(pool_path, "complete"), "w") as f:
            pass


def ready_to_serve(key, pool_path, index):
    if index == segments_count[key] - 1:
        return os.path.exists(os.path.join(pool_path, "complete"))

    return (
        os.path.exists(os.path.join(pool_path, "%d.ts" % index)) and
        os.path.exists(os.path.join(pool_path, "%d.ts" % (index + 1)))
    )


async def handler(request):
    key = hashlib.sha256(json.dumps([request.match_info["id"], request.GET["requesttoken"]]).encode("utf-8")).hexdigest()
    path = files.get(key)
    if path is None:
        async with aiohttp.ClientSession(cookies=request.cookies) as session:
            async with session.request(
                    method="get",
                    url="%s/apps/gallery/%s/vod/%s" % (upstream, request.match_info["path"], request.match_info["id"]),
                    params=request.GET,
                    headers=request.headers,
            ) as resp:
                path = os.path.join(root, os.path.relpath((await resp.json())["path"], upstream_root))
                files[key] = path

    pool_path = os.path.join(pool, key)
    os.makedirs(pool_path, exist_ok=True)

    ext = request.match_info["ext"]

    if ext == "m3u8":
        qs = urllib.parse.urlencode(request.GET)

        ffprobe = await asyncio.create_subprocess_exec("ffprobe", "-v", "0", "-select_streams", "v", "-of", "json",
                                                       "-show_entries", "format=duration", path,
                                                       stdout=subprocess.PIPE)
        stdout, stderr = await ffprobe.communicate()
        duration = json.loads(stdout.decode("utf-8"))["format"]["duration"]

        playlist = textwrap.dedent("""\
            #EXTM3U
            #EXT-X-PLAYLIST-TYPE:VOD
            #EXT-X-VERSION:3
            #EXT-X-TARGETDURATION:6
            #EXT-X-MEDIA-SEQUENCE:0
        """)
        count, remain = divmod(float(duration), 6)
        count = int(count)
        for i in range(count):
            playlist += textwrap.dedent("""\
                #EXTINF:6.0000, nodesc
                %d.ts?%s
            """ % (i, qs))
        if remain > 0:
            playlist += textwrap.dedent("""\
                #EXTINF:%.4f, nodesc
                %d.ts?%s
            """ % (remain, count, qs))
            count += 1
        playlist += "#EXT-X-ENDLIST\n"

        processes[key] = await ffmpeg(path, pool_path)
        segments_count[key] = count
        access[key] = time.monotonic()

        return web.Response(headers={"Content-type": "application/x-mpegURL",
                                     "Access-Control-Allow-Origin": "*"},
                            text=playlist)

    if ext == "ts":
        index = int(request.match_info["file"])

        access[key] = time.monotonic()

        serve_path = os.path.join(pool_path, "%d.ts" % index)
        for action in [Action.SLEEP if index == 0 else Action.SLEEP_IF, Action.KILL, Action.SLEEP, Action.RAISE]:
            if not ready_to_serve(key, pool_path, index):
                if action == Action.SLEEP_IF:
                    if os.path.exists(os.path.join(pool_path, "%d.ts" % (index - 3))):
                        action = Action.SLEEP
                if action == Action.SLEEP:
                    for i in range(150):
                        if ready_to_serve(key, pool_path, index):
                            break
                        await asyncio.sleep(0.1)
                if action == Action.KILL:
                    if key in processes:
                        try:
                            processes.pop(key).kill()
                        except ProcessLookupError:
                            pass
                        access.pop(key)
                    processes[key] = await ffmpeg(path, pool_path, max(index - 2, 0))
                if action == Action.RAISE:
                    raise web.HTTPServiceUnavailable()

        return web.FileResponse(serve_path, headers={"Content-type": "video/mp2t",
                                                     "Access-Control-Allow-Origin": "*"})


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    app = web.Application()
    app.router.add_get("/apps/gallery/{path}/vod/{id:\d+}/{file}.{ext}", handler)
    asyncio.ensure_future(cleanup_processes())
    web.run_app(app, **listen)
