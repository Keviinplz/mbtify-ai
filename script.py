import os
import json
import asyncio

import aiohttp
from pydantic import BaseModel

token = "BQAcf5CFJeTttNdUi579K7Sz7NFVDG50FLEwDHGAEZbTRjatALOCFDBmBqfxuFn8T2EJTYhBaUuD4VV5BJmhnN60UpW7-_3QSoKWpNX56DT7QPt4x80VAHO4zaaON5moggnwWVK7dBETmuD_Vt98i2APIuGq1paiwgxMsMv1hQXyBTmnjzvqiItJ"
headers = {"Authorization": f"Bearer {token}"}


class APIError(Exception):
    ...


class MBTIClass(BaseModel):
    playlists_ids: list[str]

class Valence(BaseModel):
    id: str
    danceability: float
    energy: float
    key: int
    loudness: float
    mode: int
    speechiness: float
    acousticness: float
    instrumentalness: float
    liveness: float
    valence: float
    tempo: float
    duration_ms: float
    time_signature: int

def load_dataset(path: str) -> dict[str, MBTIClass]:
    with open(path, "rb") as f:
        data = json.load(f)

    return {mbti: MBTIClass(**data[mbti]) for mbti in data}


def flatten(l: list) -> list:
    return [item for sublist in l for item in sublist]


async def get_tracks_from_multiple_playlist(
    session: aiohttp.ClientSession, playlists: list[str]
) -> list[str]:
    return flatten(
        await asyncio.gather(
            *[get_tracks_from_playlist(session, playlist) for playlist in playlists]
        )
    )


async def get_valence_from_multiple_tracks(
    session: aiohttp.ClientSession, tracks: list[str]
) -> list[dict]:
    track = ",".join(tracks)
    return await get_valence_from_track(session, track)


async def get_tracks_from_playlist(
    session: aiohttp.ClientSession, playlist: str
) -> list[str]:
    async with session.get(f"/v1/playlists/{playlist}", headers=headers) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise APIError(f"Unable to get tracks from playlist {playlist}: {text}")

        response = await resp.json()
        tracks = response["tracks"]

    return [item["track"]["id"] for item in tracks["items"]]


async def get_valence_from_track(session: aiohttp.ClientSession, track: str) -> list[dict]:
    async with session.get(f"/v1/audio-features?ids={track}", headers=headers) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise APIError(f"Unable to get valence from track {track}: {text}")
        response = await resp.json()

    return [Valence(**valence).dict() for valence in response["audio_features"]]


async def main():
    dataset = os.path.join(os.getcwd(), "data", "base.json")
    results = os.path.join(os.getcwd(), "data", "results.json")
    classes = load_dataset(dataset)

    async with aiohttp.ClientSession("https://api.spotify.com") as session:
        tracks = await asyncio.gather(
            *[
                get_tracks_from_multiple_playlist(session, mbti.playlists_ids)
                for mbti in classes.values()
            ]
        )
        valences = await asyncio.gather(
            *[
                get_valence_from_multiple_tracks(session, tracks_ids)
                for tracks_ids in tracks
            ]
        )

    output = []
    for mbti, valence in zip(classes, valences):
        output.append([{**v, "class": mbti } for v in valence])

    with open(results, "w") as f:
        json.dump(flatten(output), f)


if __name__ == "__main__":
    asyncio.run(main())
