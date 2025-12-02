# 让外部可直接 from configs import ConfigParser, AppConfig
from configs.parser import ConfigParser
from configs.config_models import (
    AppConfig, StatModeConfig, TimeConfig,
    PetPhraseConfig, FilterConfig, OutputConfig
)

__all__ = [
    "ConfigParser", "AppConfig", "StatModeConfig",
    "TimeConfig", "PetPhraseConfig", "FilterConfig", "OutputConfig"
]