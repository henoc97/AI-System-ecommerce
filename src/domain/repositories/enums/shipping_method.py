from enum import Enum

class ShippingMethod(Enum):
    STANDARD = "STANDARD"
    EXPRESS = "EXPRESS"
    OVERNIGHT = "OVERNIGHT"
    SAME_DAY = "SAME_DAY"
    ECONOMY = "ECONOMY"
    TWO_DAY = "TWO_DAY"
