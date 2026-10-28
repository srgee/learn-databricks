import pytest

from wearpipe.config import Config, load_settings

VALID_LAYERS = ('bronze', 'silver', 'gold', 'all')
INVALID_LAYER = 'invalid_layer'


def test_config_without_catalogue_name_raises_type_error() -> None:
    with pytest.raises(TypeError):
        Config()


def test_config_with_catalogue_name_succeeds():
    config = Config(catalogue_name='dev')
    assert config.catalogue_name == 'dev'


@pytest.mark.parametrize('layer', VALID_LAYERS)
def test_load_settings_accepts_valid_layer(layer):
    """Each documented layer must be accepted without error."""
    config = load_settings(['--layer', layer])
    assert config.layer == layer


def test_load_settiings_rejects_layer_not_in_allowed_set():
    with pytest.raises(ValueError, match='layer'):
        load_settings(['--layer', INVALID_LAYER])


def test_load_settings_defaults():
    config = load_settings([])
    assert config.layer == 'bronze'
    assert config.stream_flag is False


def test_load_settings_from_cli():
    config = load_settings(['--catalogue', 'prod', '--layer', 'silver', '--stream'])
    assert config.catalogue_name == 'prod'
    assert config.layer == 'silver'
    assert config.stream_flag is True


def test_load_settings_from_env(monkeypatch):
    monkeypatch.setenv('WEARPIPE_CATALOGUE', 'staging')
    monkeypatch.setenv('WEARPIPE_LAYER', 'gold')
    config = load_settings([], env_file=None)
    assert config.catalogue_name == 'staging'
    assert config.layer == 'gold'
