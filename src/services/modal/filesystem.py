def convert_bucket_url_to_pipeline_name(x: str) -> str:
    return x.replace(
        "gs://",
        "",
    ).replace(
        "-",
        "_",
    )
