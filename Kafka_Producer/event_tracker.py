from typing import List, Dict, Any, Optional
import random
from collections import deque

class EventTracker:
    # Maintains a rolling in-memory cache of recent events
    # so downstream events (clicks, conversions) can be linked to upstream events(impressions, clicks)
    # this enables realistic stream-stream joins downstream
    def __init__(
            self,
            max_impressions: int = 200_000,
            max_clicks: int = 100_000

    ):
        self.impressions = deque(maxlen=max_impressions)
        self.clicks = deque(maxlen=max_clicks)
        self.max_impressions = max_impressions
        self.max_clicks = max_clicks

    #------------------------------
    # Add impression event
    #------------------------------
    def add_impression(self, impr_event: Dict[str, Any]) :
        # stores new impression event and removes oldest if the limit exceeded
        self.impressions.append(impr_event)

    # ------------------------------
    # Add click event
    # ------------------------------
    def add_click(self, click_event: Dict[str, Any]) :
        # stores new clicks event and removes oldest if the limit exceeds
        self.clicks.append(click_event)


    # ------------------------------
    # create a sample impression event
    # ------------------------------
    def sample_impression(self):
        if not self.impressions:
            return None
        return random.choice(self.impressions)

    # ------------------------------
    # create a sample impression event
    # ------------------------------
    def sample_click(self) :
        if not self.clicks:
            return None
        return random.choice(self.clicks)