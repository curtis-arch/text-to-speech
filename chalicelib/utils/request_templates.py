from typing import Dict, List, Union


def cloud_convert_create_job(url: str) -> Dict[str, Dict[str, Dict[str, Union[str, List[str]]]]]:
    return {
        "tasks": {
            "import_source_from_url": {
                "operation": "import/url",
                "url": url
            },
            "split_audio": {
                "operation": "convert",
                "input": [
                    "import_source_from_url"
                ],
                "output_format": "mp3"
            },
            "split_video": {
                "operation": "convert",
                "input": [
                    "import_source_from_url"
                ],
                "output_format": "mp4",
                "engine": "ffmpeg",
                "audio_codec": "none"
            },
            "export_audio_artifact": {
                "operation": "export/url",
                "input": "split_audio"
            },
            "export_video_artifact": {
                "operation": "export/url",
                "input": "split_video"
            }
        }
    }


def cloud_convert_create_webhook(url: str) -> Dict[str, Union[str, List[str]]]:
    return {
        "url": url,
        "events": [
            "job.failed",
            "job.finished"
        ]
    }
