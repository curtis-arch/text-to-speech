from typing import Dict, List, Union, Any


def cloud_convert_create_job(url: str) -> Dict[str, Dict[str, Dict[str, Union[str, List[str]]]]]:
    return {
        "tasks": {
            "import_source_from_url": {
                "operation": "import/url",
                "url": url
            },
            "extract_audio": {
                "operation": "convert",
                "input": [
                    "import_source_from_url"
                ],
                "output_format": "mp3"
            },
            "export_artifacts": {
                "operation": "export/url",
                "input": "extract_audio"
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
