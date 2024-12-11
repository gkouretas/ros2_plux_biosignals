"""Functions for processing raw plux signals to discernable data"""
from plux_configs import PLUX_RESOLUTION_BITS

def emg_in_mV(emg: int) -> float:
    """
    Math: EMG_{mV} = \frac{(\frac{ADC}{2^n} - \frac{1}{2}).VCC}{G_{EMG}}
    Math: EMG_{mV} - \textit{EMG value in mV}
    Math: n - \textit{Number of bits of the channel}
    
    Reference: http://notebooks.pluxbiosignals.com/notebooks/Categories/Pre-Process/unit_conversion_emg_rev.html
    """
    VCC = 3000  # mV
    G_EMG = 1000  # mV/V
    emg_mV = (((emg / 2**PLUX_RESOLUTION_BITS) - 0.5) * VCC) / G_EMG
    return emg_mV

def eda_in_μsiemens(eda: int) -> float:
    """
    Math: EDA(\mu S) = \frac{(\frac{ADC}{2^n} .VCC)}{0.12}
    Math: n - \textit{Number of bits of the channel}
    """
    VCC = 3
    eda_μS = ((eda / 2**PLUX_RESOLUTION_BITS) * VCC) / 0.12
    return eda_μS

def ecg_in_mV(ecg: int) -> float:
    """
    Math: ECG_{mV} = \frac{(\frac{ADC}{2^n} - \frac{1}{2}).VCC}{G_{ECG}}
    Math: n - \textit{Number of bits of the channel}
    """
    VCC = 3000
    GAIN = 1000
    ecg_mV = (((ecg / 2**PLUX_RESOLUTION_BITS) - 0.5) * VCC) / GAIN
    return ecg_mV