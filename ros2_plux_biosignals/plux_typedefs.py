"""Typedefs from C++ API, primarily used for readability"""
import enum

class PluxSensor(enum.IntEnum):
    """
    https://www.downloads.plux.info/apis/cpp-api-docs/struct_plux_1_1_sensor.html#aacf53364bbfca49ec520393a153bbdd0
    """
    UNKNOWN_CLASS = 0
    EMG = 1
    ECG = 2
    LIGHT = 3
    EDA = 4
    BVP = 5
    RESP = 6
    XYZ = 7
    SYNC = 8
    EEG = 9
    SYNC_ADAP = 10
    SYNC_LED = 11
    SYNC_SW = 12
    USB = 13
    FORCE = 14
    TEMP = 15
    VPROBE = 16
    BREAKOUT = 17
    OXIMETER = 18
    GONI = 19
    ACT = 20
    EOG = 21
    EGG = 22
    ANSA = 23
    OSL = 26