from __future__ import annotations

import argparse
import types

import pytest

import storage_utils


class DummyConfig:
    def __init__(self, provider_value="mega", storage_root="/reddit", s3_bucket="bucket"):
        self.provider = types.SimpleNamespace(value=provider_value)
        self.storage_root = storage_root
        self.s3_bucket = s3_bucket
        self.s3_region = "us-east-1"
        self.s3_storage_class = "STANDARD_IA"
        self.s3_endpoint_url = None


class DummyProvider:
    def __init__(self, name="Dummy"):
        self._name = name
        self.connected = False
        self.download_calls = []
        self.upload_calls = []

    def connect(self):
        self.connected = True

    def get_provider_name(self):
        return self._name

    def download_directory(self, remote_dir, local_dir, check_type="DIR"):
        self.download_calls.append((remote_dir, local_dir, check_type))
        return types.SimpleNamespace(errors=[], summary=lambda: "ok")

    def upload_directory(self, local_dir, remote_dir, check_type="DIR"):
        self.upload_calls.append((local_dir, remote_dir, check_type))
        return types.SimpleNamespace(errors=[], summary=lambda: "ok")


def test_remote_root_for_dropbox_and_mega(monkeypatch):
    monkeypatch.setattr(storage_utils, "load_storage_config", lambda: DummyConfig(storage_root="/reddit"))
    assert storage_utils._remote_root_for("mega") == "/reddit"
    assert storage_utils._remote_root_for("dropbox") == "/reddit"


def test_remote_root_for_s3(monkeypatch):
    monkeypatch.setattr(storage_utils, "load_storage_config", lambda: DummyConfig(storage_root="/reddit"))
    assert storage_utils._remote_root_for("s3") == "reddit"


def test_migrate_rejects_same_provider():
    ns = argparse.Namespace(source="mega", target="mega", execute=False)
    assert storage_utils.cmd_migrate(ns) == 1


def test_download_uses_provider(monkeypatch):
    dummy = DummyProvider(name="Mega")
    monkeypatch.setattr(storage_utils, "load_storage_config", lambda: DummyConfig(provider_value="mega"))
    monkeypatch.setattr(storage_utils, "get_storage_provider", lambda cfg: dummy)
    monkeypatch.setattr(storage_utils, "_load_local_dir", lambda: "reddit")
    monkeypatch.setattr(storage_utils, "_load_check_type", lambda: "DIR")
    monkeypatch.setattr(storage_utils, "_remote_root_for", lambda provider_name: "/reddit")

    rc = storage_utils.cmd_download(argparse.Namespace())
    assert rc == 0
    assert dummy.connected is True
    assert dummy.download_calls == [("/reddit", "reddit", "DIR")]


def test_upload_uses_provider(monkeypatch):
    dummy = DummyProvider(name="Mega")
    monkeypatch.setattr(storage_utils, "load_storage_config", lambda: DummyConfig(provider_value="mega"))
    monkeypatch.setattr(storage_utils, "get_storage_provider", lambda cfg: dummy)
    monkeypatch.setattr(storage_utils, "_load_local_dir", lambda: "reddit")
    monkeypatch.setattr(storage_utils, "_load_check_type", lambda: "DIR")
    monkeypatch.setattr(storage_utils, "_remote_root_for", lambda provider_name: "/reddit")

    rc = storage_utils.cmd_upload(argparse.Namespace())
    assert rc == 0
    assert dummy.connected is True
    assert dummy.upload_calls == [("reddit", "/reddit", "DIR")]
