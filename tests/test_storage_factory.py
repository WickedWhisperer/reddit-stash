from __future__ import annotations

import sys
import types

import pytest

from utils.storage.base import StorageProvider
from utils.storage import factory as storage_factory


def _make_settings(tmp_path, contents: str):
    path = tmp_path / "settings.ini"
    path.write_text(contents, encoding="utf-8")
    return path


def test_load_storage_config_prefers_storage_root(monkeypatch, tmp_path):
    settings = _make_settings(
        tmp_path,
        """
        [Storage]
        provider = mega
        storage_root = /reddit
        """,
    )
    monkeypatch.setenv("STORAGE_PROVIDER", "")
    monkeypatch.setenv("STORAGE_ROOT", "")
    monkeypatch.setattr(storage_factory, "get_settings_file_path", lambda: str(settings))

    cfg = storage_factory.load_storage_config()
    assert cfg.provider == StorageProvider.MEGA
    assert cfg.storage_root == "/reddit"
    assert cfg.dropbox_directory == "/reddit"


def test_load_storage_config_legacy_dropbox_directory_alias(monkeypatch, tmp_path):
    settings = _make_settings(
        tmp_path,
        """
        [Storage]
        provider = dropbox
        dropbox_directory = /legacy-root
        """,
    )
    monkeypatch.delenv("STORAGE_ROOT", raising=False)
    monkeypatch.setattr(storage_factory, "get_settings_file_path", lambda: str(settings))

    cfg = storage_factory.load_storage_config()
    assert cfg.provider == StorageProvider.DROPBOX
    assert cfg.storage_root == "/legacy-root"


def test_get_storage_provider_requires_s3_bucket(monkeypatch):
    cfg = storage_factory.StorageConfig(provider=StorageProvider.S3, storage_root="/reddit", s3_bucket=None)
    with pytest.raises(ValueError, match="S3 provider selected"):
        storage_factory.get_storage_provider(cfg)


def test_get_storage_provider_mega(monkeypatch):
    fake_module = types.SimpleNamespace()

    class FakeMega:
        def __init__(self):
            self.connected = False

        def connect(self):
            self.connected = True

        def get_provider_name(self):
            return "Mega"

        def download_directory(self, *args, **kwargs):
            return {"errors": []}

        def upload_directory(self, *args, **kwargs):
            return {"errors": []}

    fake_module.MegaStorageProvider = FakeMega
    monkeypatch.setitem(sys.modules, "utils.storage.mega_provider", fake_module)

    cfg = storage_factory.StorageConfig(provider=StorageProvider.MEGA, storage_root="/reddit")
    provider = storage_factory.get_storage_provider(cfg)
    assert provider.__class__.__name__ == "FakeMega"
