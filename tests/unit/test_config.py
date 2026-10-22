import pytest

from sbit.config import Config, load_settings


def test_config_defaults():
    config = Config()
    assert config.catalogue_name == 'dev'
    assert config.layer == 'bronze'
    assert config.stream_flag is False


def test_config_validation_error():
    with pytest.raises(ValueError):
        Config(layer='invalid_layer')


def test_load_settings_from_cli():
    config = load_settings(['--catalogue', 'prod', '--layer', 'silver', '--stream'])
    assert config.catalogue_name == 'prod'
    assert config.layer == 'silver'
    assert config.stream_flag is True


def test_load_settings_from_env(monkeypatch):
    monkeypatch.setenv('WEARPIPE_CATALOGUE', 'staging')
    monkeypatch.setenv('WEARPIPE_LAYER', 'gold')
    config = load_settings([])
    assert config.catalogue_name == 'staging'
    assert config.layer == 'gold'
