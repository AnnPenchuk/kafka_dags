from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=("./dags/.env.dev"),
        env_file_encoding="utf-8",
    )

    mongo_url: str
    bucket_name: str
    schema_registry: str
    kafka: str
    group_id: str
    group_id2: str
    topic1: str
    topic2: str
    db: str


settings = Settings()
