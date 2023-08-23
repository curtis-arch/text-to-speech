import os
from enum import IntFlag


class FeatureToggles(IntFlag):
    TEST_CONTEXT = 1

    @staticmethod
    def on(feature: IntFlag) -> bool:
        current_toggles = os.environ.get("FEATURE_TOGGLES", "0")
        feature_toggles = FeatureToggles(int(current_toggles))
        return feature in feature_toggles
