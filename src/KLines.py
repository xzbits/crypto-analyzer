import numpy as np
import pandas as pd


class KLines:
    def __init__(self, kline_data):
        """
        Structure of one elements of K Lines data
        [
          [
            0   - 1499040000000,      // Open time
            1   - "0.01634790",       // Open
            2   - "0.80000000",       // High
            3   - "0.01575800",       // Low
            4   - "0.01577100",       // Close
            5   - "148976.11427815",  // Volume
            6   - 1499644799999,      // Close time
            7   - "2434.19055334",    // Quote asset volume
            8   - 308,                // Number of trades
            9   - "1756.87402397",    // Taker buy base asset volume
            10  - "28.46694368",      // Taker buy quote asset volume
            11  - "17928899.62484339" // Ignore.
          ]
        ]
        """
        self.kline_data = np.array(kline_data).astype(float)
        self.kline_length = self.kline_data.shape[0]

    def get_price_array(self):
        """
        Get the array price in order of column 0 - Open, 1 - Close, 2 - High, 3 - Low
        :return: return the price (no_klines, 4) array
        """
        return np.c_[self.get_open_price(), self.get_close_price(), self.get_high_price(), self.get_low_price()]

    def get_open_time_timestamps(self, idx=None):
        if idx is None:
            return self.kline_data[:, 0]
        else:
            return self.kline_data[idx, 0]

    def get_open_time_readable(self, idx=None):
        if idx is None:
            return pd.to_datetime(self.kline_data[:, 0], unit="ms")
        else:
            return pd.to_datetime(self.kline_data[idx, 0], unit="ms")

    def get_open_price(self, idx=None):
        if idx is None:
            return self.kline_data[:, 1]
        else:
            return self.kline_data[idx, 1]

    def get_high_price(self, idx=None):
        if idx is None:
            return self.kline_data[:, 2]
        else:
            return self.kline_data[idx, 2]

    def get_low_price(self, idx=None):
        if idx is None:
            return self.kline_data[:, 3]
        else:
            return self.kline_data[idx, 3]

    def get_close_price(self, idx=None):
        if idx is None:
            return self.kline_data[:, 4]
        else:
            return self.kline_data[idx, 4]

    def get_volume(self, idx=None):
        if idx is None:
            return self.kline_data[:, 5]
        else:
            return self.kline_data[idx, 5]

    def get_close_time_timestamps(self, idx=None):
        if idx is None:
            return self.kline_data[:, 6]
        else:
            return self.kline_data[idx, 6]

    def get_close_time_readable(self, idx=None):
        if idx is None:
            return pd.to_datetime(self.kline_data[:, 6], unit="ms")
        else:
            return pd.to_datetime(self.kline_data[idx, 6], unit="ms")

    def get_number_trades(self, idx=None):
        if idx is None:
            return self.kline_data[:, 8]
        else:
            return self.kline_data[idx, 8]

    def get_buy_base_asset(self, idx=None):
        if idx is None:
            return self.kline_data[:, 9]
        else:
            return self.kline_data[idx, 9]

    def get_sell_base_asset(self, idx=None):
        if idx is None:
            return np.subtract(self.get_volume(), self.get_buy_base_asset())
        else:
            return np.subtract(self.get_volume(), self.get_buy_base_asset())[idx]
